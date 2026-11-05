"""
Teste de carga do enxame P2P: de 1 a N clientes simultâneos, com orçamentos de tokens variados.

Para cada nível de concorrência n (ex.: 1..10), n clientes rodam em paralelo; cada um faz
R requisições em sequência (laço fechado), com prompts e tamanhos de saída sorteados de forma
reprodutível (--semente). A janela do nível é registrada na API de telemetria, que calcula a
energia de TODAS as máquinas do enxame com os parâmetros de config_tcc.json.

Clientes = threads: cada thread abre a SUA sessão de inferência no enxame (o Drift guarda a sessão
ativa em ContextVar, isolada por thread), e todas compartilham um único modelo cliente
(embeddings + lm_head). Isso permite 10 clientes sem replicar o modelo na RAM; os clientes
competem pela CPU desta máquina, o que fica registrado em `execucoes.modo_clientes`.

Saída: SQLite (padrão dados/carga.db), pronto para pandas:
    execucoes      metadados da rodada (modelo, dtype, specs, parâmetros)
    niveis         uma linha por nível: duração, vazão e energia/carbono/água do enxame
    requisicoes    uma linha por requisição: tokens, latência, vazão, erro, resposta
    amostras       amostras brutas (1 s) de energia/CPU/rede de todas as máquinas medidas
    v_requisicoes  requisições + energia do nível atribuída proporcionalmente aos tokens
    v_niveis       níveis + metadados da execução

Exemplo (com o enxame no ar e a API em 127.0.0.1:5000):
    python -m bancada.teste_carga --api http://127.0.0.1:5000 --experimento carga01 \\
        --modelo Qwen/Qwen2.5-0.5B-Instruct --torch_dtype float32 --clientes 1-10
"""

import argparse
import json
import random
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from . import caminhos, experimento
from . import specs as specs_mod
from .medidor import SessaoMedida

ESQUEMA = """
CREATE TABLE IF NOT EXISTS execucoes (
    id TEXT PRIMARY KEY,
    experimento TEXT NOT NULL,
    modelo TEXT, torch_dtype TEXT, initial_peers TEXT, modo_clientes TEXT,
    hostname TEXT, cpu_modelo TEXT, metodo_cpu TEXT,
    inicio REAL, fim REAL, status TEXT,
    parametros_json TEXT, specs_json TEXT
);
CREATE TABLE IF NOT EXISTS niveis (
    execucao_id TEXT NOT NULL REFERENCES execucoes(id),
    n_clientes INTEGER NOT NULL,
    t_inicio REAL, t_fim REAL, duracao_s REAL,
    requisicoes INTEGER, requisicoes_ok INTEGER,
    tokens_entrada INTEGER, tokens_saida INTEGER, tokens_por_s REAL,
    latencia_media_s REAL, latencia_p95_s REAL,
    consulta_id INTEGER,
    ti_bruta_wh REAL, ti_marginal_wh REAL, rede_wh REAL, total_wh REAL,
    carbono_g REAL, agua_ml REAL, wh_por_token REAL, mg_co2_por_token REAL,
    maquinas INTEGER, valido INTEGER, problemas TEXT,
    PRIMARY KEY (execucao_id, n_clientes)
);
CREATE TABLE IF NOT EXISTS requisicoes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execucao_id TEXT NOT NULL REFERENCES execucoes(id),
    n_clientes INTEGER NOT NULL, cliente INTEGER NOT NULL, seq INTEGER NOT NULL,
    prompt_id TEXT, max_new_tokens INTEGER,
    tokens_entrada INTEGER, tokens_saida INTEGER,
    t_inicio REAL, t_fim REAL, latencia_s REAL, tokens_por_s REAL,
    ok INTEGER, erro TEXT, resposta TEXT
);
CREATE INDEX IF NOT EXISTS idx_req_exec ON requisicoes(execucao_id, n_clientes);
CREATE TABLE IF NOT EXISTS amostras (
    execucao_id TEXT NOT NULL REFERENCES execucoes(id),
    sessao_id TEXT, no_id TEXT, papel TEXT, fase TEXT,
    t_ini REAL, t_fim REAL, cpu_util REAL, e_pkg_j REAL, e_dram_j REAL,
    bytes_tx INTEGER, bytes_rx INTEGER
);
CREATE INDEX IF NOT EXISTS idx_amostras_exec ON amostras(execucao_id, t_ini);
CREATE VIEW IF NOT EXISTS v_requisicoes AS
SELECT r.*, n.duracao_s AS nivel_duracao_s, n.total_wh AS nivel_total_wh, n.valido AS nivel_valido,
       CASE WHEN n.tokens_saida > 0 THEN n.total_wh * r.tokens_saida * 1.0 / n.tokens_saida END AS wh_atribuido,
       CASE WHEN n.tokens_saida > 0 THEN n.carbono_g * r.tokens_saida * 1.0 / n.tokens_saida END AS carbono_g_atribuido,
       CASE WHEN r.tokens_saida > 0 THEN r.latencia_s / r.tokens_saida END AS s_por_token,
       r.t_inicio - n.t_inicio AS inicio_relativo_s
FROM requisicoes r JOIN niveis n USING (execucao_id, n_clientes);
CREATE VIEW IF NOT EXISTS v_niveis AS
SELECT n.*, e.experimento, e.modelo, e.torch_dtype, e.hostname, e.cpu_modelo, e.metodo_cpu
FROM niveis n JOIN execucoes e ON e.id = n.execucao_id;
"""


def faixa(texto):
    """'1-10' -> [1..10]; '1,2,4,8' -> [1,2,4,8]."""
    valores = []
    for parte in texto.split(","):
        if "-" in parte:
            a, b = (int(x) for x in parte.split("-"))
            valores += list(range(a, b + 1))
        elif parte.strip():
            valores.append(int(parte))
    if not valores or min(valores) < 1:
        raise argparse.ArgumentTypeError("use, por exemplo, 1-10 ou 1,2,4,8")
    return sorted(set(valores))


def lista_int(texto):
    return [int(x) for x in texto.split(",") if x.strip()]


def parse_args():
    p = argparse.ArgumentParser(description="Teste de carga do enxame P2P (1..N clientes)")
    specs_mod.argumentos_specs(p)
    p.add_argument("--api", required=True)
    p.add_argument("--experimento", required=True, help="Rótulo usado na API de telemetria")
    p.add_argument("--initial_peers", help="Multiaddr do bootstrap (padrão: o publicado na API)")
    p.add_argument("--modelo", default="Qwen/Qwen2.5-0.5B-Instruct")
    p.add_argument("--torch_dtype", default="float32", choices=list(experimento.DTYPES))
    p.add_argument("--clientes", type=faixa, default=faixa("1-10"), help="Níveis de concorrência (padrão 1-10)")
    p.add_argument("--tokens", type=lista_int, default=[16, 32, 64, 128, 256],
                   help="Orçamentos de tokens de saída sorteados (padrão 16,32,64,128,256)")
    p.add_argument("--requisicoes_por_cliente", type=int, default=2)
    p.add_argument("--pausa_s", type=float, default=20.0, help="Repouso entre níveis")
    p.add_argument("--semente", type=int, default=42)
    p.add_argument("--saida_livre", action="store_true",
                   help="Permite parar antes do orçamento (EOS); padrão: força exatamente max_new_tokens")
    p.add_argument("--prompts", default=str(caminhos.PROMPTS))
    p.add_argument("--banco", default=str(caminhos.DIR_DADOS / "carga.db"))
    p.add_argument("--baseline_s", type=float)
    p.add_argument("--no_id", default="carga")
    return p.parse_args()


def planejar(niveis, prompts, tokens, r, semente):
    """Plano reprodutível: para cada nível, lista por cliente de (seq, prompt, max_new_tokens)."""
    rng = random.Random(semente)
    plano = {}
    for n in niveis:
        orcamentos = [tokens[i % len(tokens)] for i in range(n * r)]
        rng.shuffle(orcamentos)
        plano[n] = [[(seq, rng.choice(prompts), orcamentos[c * r + seq]) for seq in range(r)] for c in range(n)]
    return plano


def p95(valores):
    if not valores:
        return None
    v = sorted(valores)
    return v[min(len(v) - 1, int(round(0.95 * (len(v) - 1))))]


class Carga:
    def __init__(self, args, sessao, modelo, tokenizer, forcar_tamanho):
        self.args, self.sessao, self.modelo, self.tokenizer = args, sessao, modelo, tokenizer
        self.forcar_tamanho = forcar_tamanho

    def _ids(self, texto):
        import torch
        ids = self.tokenizer.apply_chat_template([{"role": "user", "content": texto}],
                                                 add_generation_prompt=True, return_tensors="pt")
        return ids if isinstance(ids, torch.Tensor) else ids["input_ids"]

    def gerar(self, ids, max_new):
        import torch
        extra = {"min_new_tokens": max_new} if self.forcar_tamanho else {}
        with torch.inference_mode():
            return self.modelo.generate(input_ids=ids, attention_mask=torch.ones_like(ids),
                                        max_new_tokens=max_new, do_sample=False, **extra)

    def requisicao(self, n, cliente, seq, prompt, max_new):
        ids = self._ids(prompt["texto"])
        reg = {"n_clientes": n, "cliente": cliente, "seq": seq, "prompt_id": prompt["id"],
               "max_new_tokens": max_new, "tokens_entrada": int(ids.shape[1])}
        t0 = self.sessao.relogio.agora()
        try:
            saida = self.gerar(ids, max_new)
            t1 = self.sessao.relogio.agora()
            n_out = int(saida.shape[1] - ids.shape[1])
            reg.update(ok=1, erro=None, tokens_saida=n_out,
                       resposta=self.tokenizer.decode(saida[0, ids.shape[1]:], skip_special_tokens=True))
        except Exception as e:  # registra e segue: uma falha não derruba o nível
            t1 = self.sessao.relogio.agora()
            reg.update(ok=0, erro=f"{type(e).__name__}: {e}", tokens_saida=0, resposta=None)
        reg.update(t_inicio=t0, t_fim=t1, latencia_s=t1 - t0,
                   tokens_por_s=(reg["tokens_saida"] / (t1 - t0)) if t1 > t0 else None)
        return reg

    def cliente(self, n, cliente, tarefas):
        return [self.requisicao(n, cliente, seq, prompt, max_new) for seq, prompt, max_new in tarefas]

    def nivel(self, n, tarefas_por_cliente):
        barreira = threading.Barrier(n)

        def executar(c):
            barreira.wait()  # todos os clientes do nível começam juntos
            return self.cliente(n, c, tarefas_por_cliente[c])

        t0 = self.sessao.relogio.agora()
        with ThreadPoolExecutor(max_workers=n, thread_name_prefix=f"cliente-n{n}") as pool:
            resultados = [r for lote in pool.map(executar, range(n)) for r in lote]
        t1 = self.sessao.relogio.agora()
        return t0, t1, resultados


def registrar_janela(api, experimento_, n, t0, t1, regs, execucao_id):
    return experimento.registrar_consulta(api, {
        "experimento": experimento_, "cenario": "p2p", "prompt_id": f"carga_n{n:02d}", "repeticao": n,
        "t_inicio": t0, "t_fim": t1,
        "tokens_entrada": sum(x["tokens_entrada"] for x in regs),
        "tokens_saida": sum(x["tokens_saida"] for x in regs),
        "extras": {"tipo": "teste_carga", "n_clientes": n, "execucao_id": execucao_id},
    })


def main():
    args = parse_args()
    specs = specs_mod.coletar_ou_sair(args)
    specs_mod.imprimir(specs)
    prompts = experimento.carregar_prompts(args.prompts)
    plano = planejar(args.clientes, prompts, args.tokens, args.requisicoes_por_cliente, args.semente)
    total = sum(n * args.requisicoes_por_cliente for n in args.clientes)
    api = args.api.rstrip("/")

    Path(args.banco).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(args.banco)
    db.executescript(ESQUEMA)
    execucao_id = uuid.uuid4().hex[:12]

    peers = args.initial_peers
    if not peers:
        sess = requests.get(f"{api}/sessoes", params={"experimento": args.experimento}, timeout=10).json()["sessoes"]
        ativos = [s for s in sess if s["papel"] == "servidor" and s.get("multiaddr") and s.get("fim") is None]
        if not ativos:
            raise SystemExit(f"❌ Nenhum bootstrap ativo publicou endereço no experimento '{args.experimento}'. "
                             "Inicie o enxame (painel ou bancada.no_p2p) com o mesmo --experimento.")
        peers = ativos[-1]["multiaddr"]
    print(f"[carga] Enxame: {peers}")
    print(f"[carga] Níveis {args.clientes} · {args.requisicoes_por_cliente} req/cliente · tokens {args.tokens} "
          f"· {total} requisições · banco {args.banco}")

    parametros = {k: v for k, v in vars(args).items() if k not in ("tdp",)}
    db.execute("INSERT INTO execucoes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
        execucao_id, args.experimento, args.modelo, args.torch_dtype, peers, "threads/processo único",
        specs["hostname"], specs["cpu_modelo"], specs["metodo_cpu"], time.time(), None, "executando",
        json.dumps(parametros, ensure_ascii=False), json.dumps(specs, ensure_ascii=False)))
    db.commit()

    sessao = SessaoMedida(api, specs, args.experimento, "p2p", "cliente", args.no_id,
                          extras={"modelo": args.modelo, "torch_dtype": args.torch_dtype, "tipo": "teste_carga",
                                  "execucao_id": execucao_id})
    status = "concluido"
    janelas = []
    try:
        sessao.iniciar(args.baseline_s)
        import torch
        from drift import AutoDistributedModelForCausalLM
        from transformers import AutoTokenizer

        tokenizer = AutoTokenizer.from_pretrained(args.modelo)
        modelo = AutoDistributedModelForCausalLM.from_pretrained(
            args.modelo, initial_peers=[peers], torch_dtype=getattr(torch, args.torch_dtype))
        carga = Carga(args, sessao, modelo, tokenizer, forcar_tamanho=not args.saida_livre)
        print("[carga] Aquecimento (não registrado)...")
        carga.gerar(carga._ids(prompts[0]["texto"]), 8)

        feitas = 0
        for n in args.clientes:
            time.sleep(args.pausa_s)
            print(f"[carga] Nível {n}: {n} cliente(s) simultâneo(s)...", flush=True)
            t0, t1, regs = carga.nivel(n, plano[n])
            cid = registrar_janela(api, args.experimento, n, t0, t1, regs, execucao_id)
            janelas.append((n, cid))
            lat = [r["latencia_s"] for r in regs if r["ok"]]
            db.executemany(
                """INSERT INTO requisicoes (execucao_id, n_clientes, cliente, seq, prompt_id, max_new_tokens,
                   tokens_entrada, tokens_saida, t_inicio, t_fim, latencia_s, tokens_por_s, ok, erro, resposta)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(execucao_id, r["n_clientes"], r["cliente"], r["seq"], r["prompt_id"], r["max_new_tokens"],
                  r["tokens_entrada"], r["tokens_saida"], r["t_inicio"], r["t_fim"], r["latencia_s"],
                  r["tokens_por_s"], r["ok"], r["erro"], r["resposta"]) for r in regs])
            tok = sum(r["tokens_saida"] for r in regs)
            db.execute(
                """INSERT OR REPLACE INTO niveis (execucao_id, n_clientes, t_inicio, t_fim, duracao_s, requisicoes,
                   requisicoes_ok, tokens_entrada, tokens_saida, tokens_por_s, latencia_media_s, latencia_p95_s,
                   consulta_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (execucao_id, n, t0, t1, t1 - t0, len(regs), sum(r["ok"] for r in regs),
                 sum(r["tokens_entrada"] for r in regs), tok, tok / (t1 - t0) if t1 > t0 else None,
                 sum(lat) / len(lat) if lat else None, p95(lat), cid))
            db.commit()
            feitas += len(regs)
            falhas = sum(1 for r in regs if not r["ok"])
            print(f"[carga] Nível {n}: {tok} tokens em {t1 - t0:.1f}s ({tok / (t1 - t0):.2f} tok/s agregados), "
                  f"falhas={falhas} · {feitas}/{total}", flush=True)
    except KeyboardInterrupt:
        status = "interrompido"
    except Exception as e:
        status = f"erro: {type(e).__name__}: {e}"
        raise
    finally:
        espera = (sessao.cfg or {}).get("lote_envio_s", 5) * 2 + 2
        time.sleep(espera)  # últimas amostras chegam à API antes do cálculo
        sessao.encerrar(status)
        for n, cid in janelas:  # energia calculada pela API (mesmo pipeline do TCC)
            try:
                rel = requests.get(f"{api}/consultas/{cid}/relatorio", timeout=60).json()
                e = rel["energia"]
                pt = rel.get("por_token") or {}
                db.execute(
                    """UPDATE niveis SET ti_bruta_wh=?, ti_marginal_wh=?, rede_wh=?, total_wh=?, carbono_g=?,
                       agua_ml=?, wh_por_token=?, mg_co2_por_token=?, maquinas=?, valido=?, problemas=?
                       WHERE execucao_id=? AND n_clientes=?""",
                    (e["ti_bruta_wh"], e["ti_marginal_wh"], e["rede_wh"], e["total_wh"], rel["carbono_g"],
                     rel["agua_total_ml"], pt.get("wh"), pt.get("mg_co2"), len(rel["maquinas"]),
                     int(rel["valido"]), " | ".join(rel["problemas"]), execucao_id, n))
            except Exception as ex:
                print(f"[carga] ⚠️ relatório do nível {n} indisponível: {ex}")
        try:  # amostras brutas de todas as máquinas do experimento, para séries temporais no pandas
            sess = requests.get(f"{api}/sessoes", params={"experimento": args.experimento}, timeout=10).json()["sessoes"]
            for s in sess:
                ivs = requests.get(f"{api}/sessoes/{s['id']}/intervalos", timeout=60).json()["intervalos"]
                db.executemany(
                    "INSERT INTO amostras VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [(execucao_id, s["id"], s["no_id"], s["papel"], i["fase"], i["t_ini"], i["t_fim"],
                      i["cpu_util"], i["e_pkg_j"], i["e_dram_j"], i["bytes_tx"], i["bytes_rx"]) for i in ivs])
        except Exception as ex:
            print(f"[carga] ⚠️ não foi possível exportar as amostras: {ex}")
        db.execute("UPDATE execucoes SET fim=?, status=? WHERE id=?", (time.time(), status, execucao_id))
        db.commit()
        db.close()
        print(f"[carga] Execução {execucao_id} ({status}) salva em {args.banco}")


if __name__ == "__main__":
    main()
