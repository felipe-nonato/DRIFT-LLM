"""
Teste de carga do SERVIDOR CENTRALIZADO: 1..N clientes simultâneos fazendo requisições HTTP a
bancada.servidor_central — a contraparte de bancada.teste_carga (P2P) para comparação.

Usa o MESMO plano (planejar() com a mesma --semente, níveis, orçamentos de tokens e requisições por
cliente) e grava no MESMO banco (dados/carga.db, tabelas execucoes/niveis/requisicoes/amostras), de
modo que P2P e centralizado ficam lado a lado no pandas. A janela de cada nível é registrada na API
de telemetria com cenario='centralizado' (PUE e WUE de data center aplicados pela API).

Com --iniciar_servidor, sobe o servidor nesta máquina, espera ficar pronto, aplica a carga e o
encerra no fim (sobe também a telemetria local, se não houver uma no ar):

    python -m bancada.carga_central --api http://127.0.0.1:5000 --experimento central-carga-qwen05b \\
        --iniciar_servidor --modelo Qwen/Qwen2.5-0.5B-Instruct --torch_dtype float32 \\
        --clientes 1-10 --requisicoes_por_cliente 2 --tokens 16,32,64,128,256 --interface enp3s0

Contra um servidor já no ar:  --servidor http://IP:8000
"""

import argparse
import json
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import requests

from . import caminhos, experimento
from .medidor import Relogio
from .servidor_central import PRONTO
from .teste_carga import ESQUEMA, faixa, lista_int, p95, planejar

COLUNAS_EXTRAS = {
    "execucoes": {"cenario": "TEXT"},
    "requisicoes": {"t_fila_s": "REAL", "t_geracao_s": "REAL", "lote": "INTEGER"},
}


def parse_args():
    p = argparse.ArgumentParser(description="Teste de carga do servidor centralizado (1..N clientes HTTP)")
    p.add_argument("--api", required=True, help="API de telemetria")
    p.add_argument("--experimento", required=True, help="Rótulo na telemetria (use um exclusivo deste teste)")
    p.add_argument("--servidor", default="http://127.0.0.1:8000", help="URL do servidor_central")
    p.add_argument("--clientes", type=faixa, default=faixa("1-10"))
    p.add_argument("--tokens", type=lista_int, default=[16, 32, 64, 128, 256])
    p.add_argument("--requisicoes_por_cliente", type=int, default=2)
    p.add_argument("--pausa_s", type=float, default=20.0)
    p.add_argument("--semente", type=int, default=42)
    p.add_argument("--saida_livre", action="store_true")
    p.add_argument("--prompts", default=str(caminhos.PROMPTS))
    p.add_argument("--banco", default=str(caminhos.DIR_DADOS / "carga.db"))
    g = p.add_argument_group("servidor gerenciado (--iniciar_servidor)")
    g.add_argument("--iniciar_servidor", action="store_true", help="Sobe o servidor_central nesta máquina")
    g.add_argument("--modelo", default="Qwen/Qwen2.5-0.5B-Instruct")
    g.add_argument("--torch_dtype", default="float32", choices=list(experimento.DTYPES))
    g.add_argument("--modo", choices=("lote", "serial"), default="lote")
    g.add_argument("--lote_max", type=int, default=8)
    g.add_argument("--threads", type=int)
    g.add_argument("--baseline_s", type=float)
    g.add_argument("--interface", default="enp3s0")
    g.add_argument("--tdp", type=float)
    g.add_argument("--sem_rapl", action="store_true")
    return p.parse_args()


def no_ar(url, rota):
    try:
        return requests.get(f"{url}{rota}", timeout=2).ok
    except requests.RequestException:
        return False


def local(url):
    return urlparse(url).hostname in ("127.0.0.1", "localhost")


class Filhos:
    """Processos que este teste sobe (telemetria e servidor), encerrados em ordem no fim."""

    def __init__(self):
        self.procs = []

    def subir(self, nome, cmd, pronto, log, timeout_s):
        print(f"[carga-central] Iniciando {nome} (log: {log})", flush=True)
        arq = open(log, "a", encoding="utf-8")
        proc = subprocess.Popen(cmd, cwd=caminhos.RAIZ, stdout=arq, stderr=subprocess.STDOUT,
                                stdin=subprocess.DEVNULL, text=True)
        self.procs.append((nome, proc, arq))
        limite = time.time() + timeout_s
        while not pronto():
            if proc.poll() is not None:
                raise SystemExit(f"❌ {nome} terminou com código {proc.returncode}. Veja {log}.")
            if time.time() > limite:
                raise SystemExit(f"❌ {nome} não ficou pronto em {timeout_s:.0f}s. Veja {log}.")
            time.sleep(1)
        return proc

    def encerrar(self):
        for nome, proc, arq in reversed(self.procs):  # servidor antes da telemetria
            if proc.poll() is None:
                print(f"[carga-central] Encerrando {nome}...", flush=True)
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    proc.kill()
            arq.close()


def requisitar(sessao_http, servidor, relogio, n, cliente, seq, prompt, max_new, forcar):
    reg = {"n_clientes": n, "cliente": cliente, "seq": seq, "prompt_id": prompt["id"], "max_new_tokens": max_new,
           "tokens_entrada": 0, "tokens_saida": 0, "resposta": None, "erro": None, "ok": 0,
           "t_fila_s": None, "t_geracao_s": None, "lote": None}
    t0 = relogio.agora()
    try:
        r = sessao_http.post(f"{servidor}/v1/gerar", json={"prompt": prompt["texto"], "max_new_tokens": max_new,
                                                           "forcar_tamanho": forcar}, timeout=3600)
        d = r.json()
        if not r.ok:
            raise RuntimeError(d.get("erro") or f"HTTP {r.status_code}")
        reg.update(ok=1, tokens_entrada=d["tokens_entrada"], tokens_saida=d["tokens_saida"], resposta=d["texto"],
                   t_fila_s=d["t_fila_s"], t_geracao_s=d["t_geracao_s"], lote=d["lote"])
    except Exception as e:  # registra e segue: uma falha não derruba o nível
        reg["erro"] = f"{type(e).__name__}: {e}"
    t1 = relogio.agora()
    reg.update(t_inicio=t0, t_fim=t1, latencia_s=t1 - t0,
               tokens_por_s=(reg["tokens_saida"] / (t1 - t0)) if t1 > t0 else None)
    return reg


def nivel(servidor, relogio, n, tarefas_por_cliente, forcar):
    barreira = threading.Barrier(n)

    def cliente(c):
        with requests.Session() as s:
            barreira.wait()  # todos os clientes do nível começam juntos
            return [requisitar(s, servidor, relogio, n, c, seq, prompt, max_new, forcar)
                    for seq, prompt, max_new in tarefas_por_cliente[c]]

    t0 = relogio.agora()
    with ThreadPoolExecutor(max_workers=n, thread_name_prefix=f"cliente-n{n}") as pool:
        regs = [r for lote in pool.map(cliente, range(n)) for r in lote]
    return t0, relogio.agora(), regs


def preparar_banco(caminho):
    Path(caminho).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(caminho)
    db.executescript(ESQUEMA)
    for tabela, colunas in COLUNAS_EXTRAS.items():
        existentes = {r[1] for r in db.execute(f"PRAGMA table_info({tabela})")}
        for nome, tipo in colunas.items():
            if nome not in existentes:
                db.execute(f"ALTER TABLE {tabela} ADD COLUMN {nome} {tipo}")
    db.commit()
    return db


def main():
    args = parse_args()
    api, servidor = args.api.rstrip("/"), args.servidor.rstrip("/")
    prompts = experimento.carregar_prompts(args.prompts)
    plano = planejar(args.clientes, prompts, args.tokens, args.requisicoes_por_cliente, args.semente)
    total = sum(n * args.requisicoes_por_cliente for n in args.clientes)
    caminhos.garantir_diretorios()
    filhos = Filhos()
    carimbo = time.strftime("%Y%m%d-%H%M%S")

    try:
        if not no_ar(api, "/tempo"):
            if not (args.iniciar_servidor and local(api)):
                raise SystemExit(f"❌ Telemetria fora do ar em {api}.")
            filhos.subir("telemetria", [sys.executable, "-m", "bancada.telemetria"], lambda: no_ar(api, "/tempo"),
                         caminhos.DIR_LOGS / f"telemetria_central_{carimbo}.log", 30)
        if args.iniciar_servidor:
            if no_ar(servidor, "/saude"):
                raise SystemExit(f"❌ Já existe um servidor em {servidor}; use-o sem --iniciar_servidor.")
            porta = urlparse(servidor).port or 8000
            cmd = [sys.executable, "-m", "bancada.servidor_central", "--api", api, "--experimento", args.experimento,
                   "--modelo", args.modelo, "--torch_dtype", args.torch_dtype, "--modo", args.modo,
                   "--lote_max", str(args.lote_max), "--porta", str(porta), "--interface", args.interface]
            for nome in ("threads", "baseline_s", "tdp"):
                if getattr(args, nome) is not None:
                    cmd += [f"--{nome}", str(getattr(args, nome))]
            if args.sem_rapl:
                cmd.append("--sem_rapl")
            filhos.subir("servidor central", cmd, lambda: no_ar(servidor, "/saude"),
                         caminhos.DIR_LOGS / f"servidor_central_{carimbo}.log", 3600)
        elif not no_ar(servidor, "/saude"):
            raise SystemExit(f"❌ Servidor central fora do ar em {servidor} (use --iniciar_servidor).")

        info = requests.get(f"{servidor}/saude", timeout=5).json()
        relogio = Relogio(api).sincronizar()
        db = preparar_banco(args.banco)
        execucao_id = uuid.uuid4().hex[:12]
        modo_clientes = (f"HTTP: {max(args.clientes)} threads clientes → servidor central "
                         f"(modo {info['modo']}, lote máx. {info['lote_max']}, {info['threads']} threads torch)")
        sess = requests.get(f"{api}/sessoes", params={"experimento": args.experimento}, timeout=10).json()["sessoes"]
        srv = next((s for s in reversed(sess) if s["cenario"] == "centralizado" and s.get("fim") is None), None)
        specs = (srv or {}).get("specs") or {}
        db.execute("""INSERT INTO execucoes (id, experimento, modelo, torch_dtype, initial_peers, modo_clientes,
                      hostname, cpu_modelo, metodo_cpu, inicio, fim, status, parametros_json, specs_json, cenario)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            execucao_id, args.experimento, info["modelo"], info["torch_dtype"], servidor, modo_clientes,
            specs.get("hostname"), specs.get("cpu_modelo"), specs.get("metodo_cpu"), time.time(), None, "executando",
            json.dumps({**vars(args), "servidor_info": info}, ensure_ascii=False, default=list),
            json.dumps(specs, ensure_ascii=False), "centralizado"))
        db.commit()
        print(f"[carga-central] Servidor: {servidor} · {info['modelo']} ({info['torch_dtype']}) · {modo_clientes}")
        print(f"[carga-central] Níveis {args.clientes} · {args.requisicoes_por_cliente} req/cliente · tokens "
              f"{args.tokens} · {total} requisições · execução {execucao_id}", flush=True)

        status, janelas, feitas = "concluido", [], 0
        try:
            for n in args.clientes:
                time.sleep(args.pausa_s)
                print(f"[carga-central] Nível {n}: {n} cliente(s) simultâneo(s)...", flush=True)
                t0, t1, regs = nivel(servidor, relogio, n, plano[n], not args.saida_livre)
                tok = sum(r["tokens_saida"] for r in regs)
                cid = experimento.registrar_consulta(api, {
                    "experimento": args.experimento, "cenario": "centralizado", "prompt_id": f"carga_n{n:02d}",
                    "repeticao": n, "t_inicio": t0, "t_fim": t1,
                    "tokens_entrada": sum(r["tokens_entrada"] for r in regs), "tokens_saida": tok,
                    "extras": {"tipo": "carga_central", "n_clientes": n, "execucao_id": execucao_id},
                })
                janelas.append((n, cid))
                lat = [r["latencia_s"] for r in regs if r["ok"]]
                db.executemany(
                    """INSERT INTO requisicoes (execucao_id, n_clientes, cliente, seq, prompt_id, max_new_tokens,
                       tokens_entrada, tokens_saida, t_inicio, t_fim, latencia_s, tokens_por_s, ok, erro, resposta,
                       t_fila_s, t_geracao_s, lote) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    [(execucao_id, r["n_clientes"], r["cliente"], r["seq"], r["prompt_id"], r["max_new_tokens"],
                      r["tokens_entrada"], r["tokens_saida"], r["t_inicio"], r["t_fim"], r["latencia_s"],
                      r["tokens_por_s"], r["ok"], r["erro"], r["resposta"], r["t_fila_s"], r["t_geracao_s"],
                      r["lote"]) for r in regs])
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
                print(f"[carga-central] Nível {n}: {tok} tokens em {t1 - t0:.1f}s ({tok / (t1 - t0):.2f} tok/s "
                      f"agregados), falhas={falhas} · {feitas}/{total}", flush=True)
        except KeyboardInterrupt:
            status = "interrompido"

        cfg = requests.get(f"{api}/config", timeout=5).json()["amostragem"]
        time.sleep(cfg["lote_envio_s"] * 2 + 2)  # últimas amostras chegam à API antes do cálculo
        for n, cid in janelas:  # energia calculada pela API (mesmo pipeline do P2P)
            try:
                rel = requests.get(f"{api}/consultas/{cid}/relatorio", timeout=60).json()
                e, pt = rel["energia"], rel.get("por_token") or {}
                db.execute(
                    """UPDATE niveis SET ti_bruta_wh=?, ti_marginal_wh=?, rede_wh=?, total_wh=?, carbono_g=?, agua_ml=?,
                       wh_por_token=?, mg_co2_por_token=?, maquinas=?, valido=?, problemas=?
                       WHERE execucao_id=? AND n_clientes=?""",
                    (e["ti_bruta_wh"], e["ti_marginal_wh"], e["rede_wh"], e["total_wh"], rel["carbono_g"],
                     rel["agua_total_ml"], pt.get("wh"), pt.get("mg_co2"), len(rel["maquinas"]), int(rel["valido"]),
                     " | ".join(rel["problemas"]), execucao_id, n))
            except Exception as ex:
                print(f"[carga-central] ⚠️ relatório do nível {n} indisponível: {ex}")
        try:  # amostras brutas para séries temporais no pandas
            sess = requests.get(f"{api}/sessoes", params={"experimento": args.experimento}, timeout=10).json()["sessoes"]
            for s in sess:
                ivs = requests.get(f"{api}/sessoes/{s['id']}/intervalos", timeout=60).json()["intervalos"]
                db.executemany("INSERT INTO amostras VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                               [(execucao_id, s["id"], s["no_id"], s["papel"], i["fase"], i["t_ini"], i["t_fim"],
                                 i["cpu_util"], i["e_pkg_j"], i["e_dram_j"], i["bytes_tx"], i["bytes_rx"])
                                for i in ivs])
        except Exception as ex:
            print(f"[carga-central] ⚠️ não foi possível exportar as amostras: {ex}")
        db.execute("UPDATE execucoes SET fim=?, status=? WHERE id=?", (time.time(), status, execucao_id))
        db.commit()
        db.close()
        print(f"[carga-central] Execução {execucao_id} ({status}) salva em {args.banco}")
    finally:
        filhos.encerrar()


if __name__ == "__main__":
    main()
