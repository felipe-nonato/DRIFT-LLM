"""
Leitura e resumo do banco do teste de carga (dados/carga.db) com pandas.

Uso:
    python analise/carga_pandas.py                       # última execução
    python analise/carga_pandas.py --execucao <id> --csv saida/   # exporta CSVs

Em um notebook:
    from analise.carga_pandas import carregar
    d = carregar("dados/carga.db")
    d["niveis"].plot(x="n_clientes", y="tokens_por_s")
"""

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
BANCO = RAIZ / "dados" / "carga.db"


def carregar(banco=BANCO, execucao=None):
    """Retorna dict de DataFrames: execucoes, niveis, requisicoes, amostras (filtrados pela execução)."""
    con = sqlite3.connect(banco)
    try:
        execucoes = pd.read_sql("SELECT * FROM execucoes ORDER BY inicio", con)
        if execucoes.empty:
            raise SystemExit(f"Nenhuma execução em {banco}")
        execucao = execucao or execucoes.iloc[-1]["id"]
        filtro = {"e": execucao}
        niveis = pd.read_sql("SELECT * FROM v_niveis WHERE execucao_id=:e ORDER BY n_clientes", con, params=filtro)
        req = pd.read_sql("SELECT * FROM v_requisicoes WHERE execucao_id=:e ORDER BY t_inicio", con, params=filtro)
        amostras = pd.read_sql("SELECT * FROM amostras WHERE execucao_id=:e ORDER BY t_ini", con, params=filtro)
    finally:
        con.close()
    for df, cols in ((req, ["t_inicio", "t_fim"]), (niveis, ["t_inicio", "t_fim"]), (amostras, ["t_ini", "t_fim"])):
        for c in cols:
            df[c + "_dt"] = pd.to_datetime(df[c], unit="s")
    if not amostras.empty:  # potência média por intervalo (W) quando há RAPL
        dur = amostras["t_fim"] - amostras["t_ini"]
        amostras["p_pkg_w"] = amostras["e_pkg_j"] / dur
    return {"execucoes": execucoes, "niveis": niveis, "requisicoes": req, "amostras": amostras,
            "execucao_id": execucao}


def resumo(d):
    req, niveis = d["requisicoes"], d["niveis"]
    ok = req[req["ok"] == 1]
    por_nivel = ok.groupby("n_clientes").agg(
        requisicoes=("id", "count"),
        latencia_p50_s=("latencia_s", "median"),
        latencia_p95_s=("latencia_s", lambda s: s.quantile(0.95)),
        s_por_token_med=("s_por_token", "median"),
        tok_s_por_cliente=("tokens_por_s", "mean"),
    )
    cols = ["n_clientes", "duracao_s", "tokens_saida", "tokens_por_s", "total_wh", "ti_marginal_wh",
            "wh_por_token", "carbono_g", "valido"]
    tabela = niveis[cols].set_index("n_clientes").join(por_nivel)
    por_orcamento = ok.groupby("max_new_tokens").agg(
        n=("id", "count"), latencia_media_s=("latencia_s", "mean"), s_por_token=("s_por_token", "median"),
        wh_atribuido_medio=("wh_atribuido", "mean"))
    return tabela, por_orcamento


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--banco", default=str(BANCO))
    p.add_argument("--execucao")
    p.add_argument("--csv", help="Pasta para exportar niveis/requisicoes/amostras em CSV")
    a = p.parse_args()
    d = carregar(a.banco, a.execucao)
    e = d["execucoes"].set_index("id").loc[d["execucao_id"]]
    print(f"Execução {d['execucao_id']} · {e['experimento']} · {e['modelo']} ({e['torch_dtype']}) · "
          f"{e['cpu_modelo']} · status {e['status']}")
    tabela, por_orc = resumo(d)
    with pd.option_context("display.width", 200, "display.max_columns", 30, "display.float_format", "{:.4g}".format):
        print("\nPor nível de concorrência:\n", tabela)
        print("\nPor orçamento de tokens:\n", por_orc)
        falhas = d["requisicoes"][d["requisicoes"]["ok"] == 0]
        print(f"\nRequisições com falha: {len(falhas)}")
    if a.csv:
        destino = Path(a.csv)
        destino.mkdir(parents=True, exist_ok=True)
        for nome in ("niveis", "requisicoes", "amostras"):
            d[nome].to_csv(destino / f"{nome}_{d['execucao_id']}.csv", index=False)
        print(f"CSVs em {destino}")


if __name__ == "__main__":
    main()
