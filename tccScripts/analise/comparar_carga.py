"""
Compara, nível a nível (1..N clientes), o teste de carga P2P (bancada.teste_carga) com o do
servidor centralizado (bancada.carga_central), lidos de dados/carga.db.

    python analise/comparar_carga.py                         # últimas execuções de cada cenário
    python analise/comparar_carga.py --api http://127.0.0.1:5000   # completa energia ausente via telemetria
    python analise/comparar_carga.py --p2p <id> --central <id> --csv dados/csv

Níveis sem energia (ex.: execução interrompida antes do cálculo) são recalculados pela API de
telemetria a partir do consulta_id, com os parâmetros atuais de config_tcc.json.
"""

import argparse
import sqlite3
from pathlib import Path

import pandas as pd
import requests

RAIZ = Path(__file__).resolve().parent.parent
BANCO = RAIZ / "dados" / "carga.db"
CAMPOS_ENERGIA = ("ti_bruta_wh", "ti_marginal_wh", "rede_wh", "total_wh", "carbono_g", "agua_ml",
                  "wh_por_token", "mg_co2_por_token", "maquinas", "valido", "problemas")


def cenario_da_execucao(row):
    c = row.get("cenario")
    return "p2p" if c is None or pd.isna(c) or not c else c


def preencher_energia(con, execucao_id, api):
    """Recalcula pela API os níveis sem total_wh."""
    faltando = con.execute("SELECT n_clientes, consulta_id FROM niveis WHERE execucao_id=? AND total_wh IS NULL "
                           "AND consulta_id IS NOT NULL", (execucao_id,)).fetchall()
    for n, cid in faltando:
        try:
            rel = requests.get(f"{api}/consultas/{cid}/relatorio", timeout=60).json()
            e, pt = rel["energia"], rel.get("por_token") or {}
            valores = (e["ti_bruta_wh"], e["ti_marginal_wh"], e["rede_wh"], e["total_wh"], rel["carbono_g"],
                       rel["agua_total_ml"], pt.get("wh"), pt.get("mg_co2"), len(rel["maquinas"]),
                       int(rel["valido"]), " | ".join(rel["problemas"]))
            con.execute(f"UPDATE niveis SET {', '.join(c + '=?' for c in CAMPOS_ENERGIA)} "
                        "WHERE execucao_id=? AND n_clientes=?", (*valores, execucao_id, n))
        except Exception as ex:
            print(f"⚠️  energia do nível {n} ({execucao_id}) indisponível: {ex}")
    con.commit()
    return len(faltando)


def escolher(execucoes, cenario, explicito, banco):
    if explicito:
        return explicito
    cand = execucoes[execucoes["cenario_efetivo"] == cenario]
    if cand.empty:
        raise SystemExit(f"Nenhuma execução do cenário '{cenario}' em {banco}.")
    return cand.iloc[-1]["id"]


def carregar(banco, p2p=None, central=None, api=None):
    con = sqlite3.connect(banco)
    try:
        execucoes = pd.read_sql("SELECT * FROM execucoes ORDER BY inicio", con)
        if "cenario" not in execucoes:
            execucoes["cenario"] = None
        execucoes["cenario_efetivo"] = [cenario_da_execucao(r) for r in execucoes.to_dict("records")]
        ids = {"p2p": escolher(execucoes, "p2p", p2p, banco),
               "centralizado": escolher(execucoes, "centralizado", central, banco)}
        if api:
            for i in ids.values():
                n = preencher_energia(con, i, api.rstrip("/"))
                if n:
                    print(f"Energia recalculada pela API para {n} nível(is) da execução {i}.")
        partes = []
        for cen, i in ids.items():
            df = pd.read_sql("SELECT * FROM niveis WHERE execucao_id=? ORDER BY n_clientes", con, params=(i,))
            df["cenario"] = cen
            partes.append(df)
        niveis = pd.concat(partes, ignore_index=True)
        req = pd.read_sql(f"SELECT * FROM requisicoes WHERE execucao_id IN ({','.join('?' * len(ids))})", con,
                          params=tuple(ids.values()))
    finally:
        con.close()
    meta = execucoes.set_index("id").loc[list(ids.values())]
    return ids, meta, niveis, req


def comparar(niveis):
    cols = ["tokens_por_s", "latencia_media_s", "latencia_p95_s", "duracao_s", "total_wh", "wh_por_token",
            "carbono_g", "agua_ml", "valido"]
    largo = niveis.pivot(index="n_clientes", columns="cenario", values=cols)
    largo.columns = [f"{m}_{'p2p' if c == 'p2p' else 'cent'}" for m, c in largo.columns]
    comuns = largo.dropna(subset=["tokens_por_s_p2p", "tokens_por_s_cent"])
    razao = pd.DataFrame(index=comuns.index)
    razao["vazao_cent/p2p"] = comuns["tokens_por_s_cent"] / comuns["tokens_por_s_p2p"]
    razao["latencia_cent/p2p"] = comuns["latencia_media_s_cent"] / comuns["latencia_media_s_p2p"]
    if "wh_por_token_cent" in comuns and comuns["wh_por_token_p2p"].notna().any():
        razao["wh_token_cent/p2p"] = comuns["wh_por_token_cent"] / comuns["wh_por_token_p2p"]
    return largo, razao


def main():
    p = argparse.ArgumentParser(description="Compara os testes de carga P2P × centralizado")
    p.add_argument("--banco", default=str(BANCO))
    p.add_argument("--p2p", help="id da execução P2P (padrão: a mais recente)")
    p.add_argument("--central", help="id da execução centralizada (padrão: a mais recente)")
    p.add_argument("--api", help="API de telemetria para completar energia ausente")
    p.add_argument("--csv", help="Pasta para exportar a comparação")
    a = p.parse_args()

    ids, meta, niveis, req = carregar(a.banco, a.p2p, a.central, a.api)
    for cen, i in ids.items():
        m = meta.loc[i]
        print(f"{cen:<12} {i} · {m['experimento']} · {m['modelo']} ({m['torch_dtype']}) · {m['cpu_modelo']} · "
              f"{m['status']}\n{'':<13}{m['modo_clientes']}")
    if meta["modelo"].nunique() > 1 or meta["torch_dtype"].nunique() > 1:
        print("⚠️  Modelos ou precisões diferentes entre as execuções: a comparação não é válida.")

    largo, razao = comparar(niveis)
    with pd.option_context("display.width", 220, "display.max_columns", 40, "display.float_format", "{:.4g}".format):
        print("\nPor nível de concorrência (cent = centralizado):\n", largo)
        print("\nRazões centralizado / P2P (níveis presentes nos dois):\n", razao)
        if "lote" in req and req["lote"].notna().any():
            lotes = req[req["lote"].notna()].groupby("n_clientes").agg(
                lote_medio=("lote", "mean"), fila_media_s=("t_fila_s", "mean"))
            print("\nServidor central — tamanho de lote e espera na fila:\n", lotes)
    if a.csv:
        destino = Path(a.csv)
        destino.mkdir(parents=True, exist_ok=True)
        largo.join(razao).to_csv(destino / f"comparacao_{ids['p2p']}_{ids['centralizado']}.csv")
        print(f"\nCSV em {destino}")


if __name__ == "__main__":
    main()
