"""
Acompanhamento de um experimento em andamento: validações + progresso estimado → HTML.

Lê (somente leitura) o banco da telemetria, os logs do cliente/centralizado e os
processos desta máquina; nada é escrito no banco nem enviado à API. A validade de
cada consulta concluída é calculada pelo mesmo código da API (bancada.calculo).

Como o cliente só registra a consulta ao terminar, o progresso da consulta atual é
ESTIMADO: tempo decorrido ÷ duração esperada. A duração esperada vem, nesta ordem:
  1. da mesma consulta (prompt) já concluída neste experimento (decodificação gulosa
     → mesma resposta, mesma duração);
  2. da mediana de s/token das consultas concluídas neste experimento;
  3. de um CSV de referência (--referencia, ex.: experimento_01.csv), mesmo prompt;
  4. do aquecimento (8 tokens), estimativa pessimista.

Uso (na raiz tccScripts/, na máquina da API):
    python -m analise.progresso                           # experimento mais recente
    python -m analise.progresso --experimento experimento-03 \\
        --referencia ~/Downloads/experimento_01.csv
    python -m analise.progresso --intervalo 300           # regera a cada 5 min

O script roda com prioridade mínima (nice 19) e leva < 1 s, mas ele roda na máquina
medida: prefira executá-lo poucas vezes (intervalo ≥ 300 s).
"""

import argparse
import csv
import glob
import html
import json
import os
import re
import sqlite3
import statistics
import time
from datetime import datetime
from pathlib import Path

from bancada import blocos, calculo, caminhos, telemetria

TOKENS_AQUECIMENTO = 8          # experimento.executar_consultas: gerar(..., 8)
TOK_S_MINIMO_ESPERADO = 0.2     # abaixo disso, 7B em CPU está anormalmente lento
LIMITE_RAM = 0.85               # mesmo critério da bancada
ALERTA_RAM = 0.80
TELEMETRIA_ATRASO_MAX_S = 30
ROTULOS = {"p2p": "P2P", "centralizado": "CENTRAL"}
ORDEM_CENARIOS = ["p2p", "centralizado"]


# ---------------------------------------------------------------------------
# Coleta
# ---------------------------------------------------------------------------

def abrir_banco(caminho):
    con = sqlite3.connect(f"file:{caminho}?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    return con


def processos():
    """Processos da bancada nesta máquina: {modulo: [{pid, args, inicio}]}."""
    try:
        btime = next(int(l.split()[1]) for l in open("/proc/stat") if l.startswith("btime"))
        hz = os.sysconf("SC_CLK_TCK")
    except Exception:
        btime, hz = None, 100
    achados = {}
    for d in glob.glob("/proc/[0-9]*"):
        try:
            args = Path(d, "cmdline").read_bytes().split(b"\0")
            args = [a.decode(errors="replace") for a in args if a]
            if "-m" not in args:
                continue
            mod = args[args.index("-m") + 1]
            if not (mod.startswith("bancada.") or mod == "drift.cli.run_server"):
                continue
            ppid_args = Path("/proc", Path(d, "stat").read_text().rsplit(")", 1)[1].split()[1], "cmdline")
            if mod == "drift.cli.run_server" and b"drift.cli.run_server" in ppid_args.read_bytes():
                continue  # subprocessos do próprio servidor
            inicio = None
            if btime:
                inicio = btime + int(Path(d, "stat").read_text().rsplit(")", 1)[1].split()[19]) / hz
            achados.setdefault(mod, []).append({"pid": int(Path(d).name), "args": args, "inicio": inicio})
        except (OSError, ValueError, IndexError, StopIteration):
            continue
    return achados


def arg(args, nome, padrao=None):
    return args[args.index(nome) + 1] if nome in args and args.index(nome) + 1 < len(args) else padrao


def memoria_local():
    info = {}
    for l in open("/proc/meminfo"):
        k, v = l.split(":")
        info[k] = int(v.split()[0]) * 1024
    def vmstat():
        d = {}
        for l in open("/proc/vmstat"):
            k, v = l.split()
            if k in ("pswpin", "pswpout", "pgmajfault"):
                d[k] = int(v)
        return d
    a = vmstat()
    time.sleep(2)
    b = vmstat()
    taxa = {k: (b[k] - a[k]) / 2 for k in a}
    return {
        "swap_in_pag_s": taxa.get("pswpin", 0),
        "swap_out_pag_s": taxa.get("pswpout", 0),
        "falhas_maiores_s": taxa.get("pgmajfault", 0),
        "swap_usado_gb": (info.get("SwapTotal", 0) - info.get("SwapFree", 0)) / 1024**3,
        "ram_disponivel_gb": info.get("MemAvailable", 0) / 1024**3,
        "ram_total_gb": info.get("MemTotal", 0) / 1024**3,
    }


def gib_por_bloco(modelo, dtype):
    """Memória de um bloco Transformer a partir do config.json em cache (sem importar torch)."""
    pasta = Path.home() / ".cache/huggingface/hub" / f"models--{modelo.replace('/', '--')}"
    cfgs = sorted(pasta.glob("snapshots/*/config.json"))
    if not cfgs:
        return None
    c = json.loads(cfgs[-1].read_text())
    h, i = c["hidden_size"], c["intermediate_size"]
    kv = h // c["num_attention_heads"] * c.get("num_key_value_heads", c["num_attention_heads"])
    params = h * h * 2 + h * kv * 2 + h + kv * 2 + 3 * h * i + 2 * h  # q,o + k,v + vieses + MLP + normas
    return params * telemetria_dtype_bytes(dtype) / 1024**3


def telemetria_dtype_bytes(dtype):
    return {"float32": 4, "bfloat16": 2, "float16": 2}.get(dtype, 2)


GERANDO = re.compile(r"^\[(\w+)\] gerando (\d+)/(\d+) tokens em (\d+)s · consulta (\d+)/(\d+) prompt=(\S+) rep=(\d+)")
LINHA_LOG = re.compile(r"^([A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2})\.\d+ \[(\w+)\] (.*)$")


def ler_log(caminho, rotulo):
    """Eventos do log do cliente/centralizado."""
    ano = datetime.now().year
    ev = {"arquivo": str(caminho), "aquecimento_idx": None, "inicios": [], "retentativas": [],
          "rotas": [], "concluidas": [], "avisos": set(), "erros": [], "relatorio_salvo": False,
          "relay": False, "linhas": 0, "geracao": None}
    ultimo_erro_t = None
    with open(caminho, encoding="utf-8", errors="replace") as f:
        for n, linha in enumerate(f):
            linha = linha.rstrip("\n")
            ev["linhas"] = n
            m = LINHA_LOG.match(linha)
            t = None
            if m:
                try:
                    t = datetime.strptime(f"{ano} {m.group(1)}", "%Y %b %d %H:%M:%S").timestamp()
                except ValueError:
                    t = None
            if f"[{rotulo}] Aquecimento" in linha:
                ev["aquecimento_idx"] = n
            elif m and "Route found:" in m.group(3):
                rota = m.group(3).split("Route found:", 1)[1].strip()
                if ev["aquecimento_idx"] is not None:
                    if ultimo_erro_t is not None and t - ultimo_erro_t <= 10:
                        ev["retentativas"].append(t)  # rota parcial: só o trecho que falhou
                    else:
                        ev["inicios"].append(t)
                        ev["rotas"].append((t, rota))  # 1ª = aquecimento; demais = consultas medidas
            elif m and "Caught exception" in m.group(3):
                ultimo_erro_t = t
                tipo = re.search(r"\): (\w+(?:Error|Exception)\w*)", m.group(3))
                ev["erros"].append((t, f"Erro ao inferir via servidor remoto ({tipo.group(1) if tipo else 'exceção'}); "
                                       "o cliente tentou de novo."))
            elif "AVX512" in linha:
                ev["avisos"].add("bfloat16 em CPU sem AVX-512: execução lenta (o próprio Drift recomenda float32).")
            elif re.match(rf"^\[{rotulo}\] \d+/\d+ prompt=", linha):
                ev["concluidas"].append(linha)
            elif linha.startswith(f"[{rotulo}] gerando "):
                g = GERANDO.match(linha)
                if g:
                    ev["geracao"] = {"tokens": int(g.group(2)), "max": int(g.group(3)), "decorrido_s": int(g.group(4)),
                                     "consulta": int(g.group(5)), "prompt": g.group(7), "rep": int(g.group(8))}
            elif f"[{rotulo}] Relatório salvo" in linha:
                ev["relatorio_salvo"] = True
            elif linha.startswith("Traceback") or linha.startswith("❌"):
                ev["erros"].append((t, linha[:200]))
            if "using_relay=True" in linha:
                ev["relay"] = True
    return ev


def log_do_cenario(cenario, desde):
    padroes = {"p2p": "cliente_*.log", "centralizado": "central*_*.log"}
    arquivos = [Path(p) for p in glob.glob(str(caminhos.DIR_LOGS / padroes[cenario]))]
    arquivos = [a for a in arquivos if desde is None or a.stat().st_mtime >= desde]
    return max(arquivos, key=lambda a: a.stat().st_mtime) if arquivos else None


def ler_referencias(caminhos_csv):
    """{(cenario, prompt_id): [ (duracao_s, tokens_saida, origem) ]}"""
    ref = {}
    for c in caminhos_csv:
        try:
            with open(os.path.expanduser(c), newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    if r.get("valido", "True") not in ("True", "true", "1"):
                        continue
                    ref.setdefault((r["cenario"], r["prompt_id"]), []).append(
                        (float(r["duracao_s"]), int(float(r["tokens_saida"])), Path(c).name))
        except (OSError, KeyError, ValueError):
            continue
    return ref


# ---------------------------------------------------------------------------
# Estimativa
# ---------------------------------------------------------------------------

def duracao_esperada(cenario, prompt_id, concluidas, ref, s_tok_aquec, max_new):
    """(segundos, origem)"""
    mesmas = [c["duracao_s"] for c in concluidas if c["cenario"] == cenario and c["prompt_id"] == prompt_id]
    if mesmas:
        return statistics.median(mesmas), "mesmo prompt já concluído neste experimento"
    taxas = [c["duracao_s"] / c["tokens_saida"] for c in concluidas
             if c["cenario"] == cenario and c["tokens_saida"]]
    tokens_ref = [t for _, t, _ in ref.get((cenario, prompt_id), [])]
    tokens = statistics.median(tokens_ref) if tokens_ref else max_new
    if taxas:
        return tokens * statistics.median(taxas), "s/token das consultas concluídas neste experimento"
    if ref.get((cenario, prompt_id)):
        d = statistics.median(x[0] for x in ref[(cenario, prompt_id)])
        return d, f"referência {ref[(cenario, prompt_id)][0][2]} (configuração pode diferir)"
    if s_tok_aquec:
        return tokens * s_tok_aquec, "aquecimento (pessimista: inclui o processamento da entrada)"
    return None, "sem base para estimar"


# ---------------------------------------------------------------------------
# Montagem do estado
# ---------------------------------------------------------------------------

def coletar(args):
    agora = time.time()
    procs = processos()
    con = abrir_banco(args.banco)
    exp = args.experimento or (con.execute(
        "SELECT experimento FROM sessoes ORDER BY inicio DESC LIMIT 1").fetchone() or [None])[0]
    if not exp:
        raise SystemExit("Nenhum experimento no banco.")

    sessoes = [dict(r) for r in con.execute(
        "SELECT * FROM sessoes WHERE experimento=? ORDER BY inicio", (exp,))]
    for s in sessoes:
        s["specs"] = json.loads(s["specs_json"])
        s["extras"] = json.loads(s["extras_json"] or "{}")
        ult = con.execute("SELECT MAX(t_fim), COUNT(*) FROM intervalos WHERE sessao_id=?", (s["id"],)).fetchone()
        s["ultima_amostra"], s["n_amostras"] = ult[0], ult[1]

    cfg = calculo.carregar_config(telemetria.CONFIG)
    concluidas = []
    memo = {}
    for r in con.execute("SELECT * FROM consultas WHERE experimento=? ORDER BY id", (exp,)):
        c = dict(r)
        rel = calculo.relatorio_consulta(c, telemetria._sessoes_da_janela(con, c, memo), cfg)
        concluidas.append({
            "id": c["id"], "cenario": c["cenario"], "prompt_id": c["prompt_id"], "repeticao": c["repeticao"],
            "t_inicio": c["t_inicio"], "t_fim": c["t_fim"], "duracao_s": c["t_fim"] - c["t_inicio"],
            "tokens_saida": c["tokens_saida"], "valido": rel["valido"], "problemas": rel["problemas"],
            "tok_s": rel["tokens_por_s"], "total_wh": rel["energia"]["total_wh"],
            "wh_token": rel["por_token"]["wh"] if rel["por_token"] else None,
            "maquinas": len(rel["maquinas"]),
        })
    con.close()

    ref = ler_referencias(args.referencia + glob.glob(str(caminhos.DIR_RESULTADOS / "*.csv")))
    prompts = [p["id"] for p in json.loads(Path(args.prompts).read_text(encoding="utf-8"))["prompts"]]

    cenarios = {}
    for cen in ORDEM_CENARIOS:
        mod = "bancada.cliente_p2p" if cen == "p2p" else "bancada.centralizado"
        proc = next((p for p in procs.get(mod, []) if arg(p["args"], "--experimento") == exp), None)
        sess_cen = [s for s in sessoes if s["cenario"] == cen]
        feitas = [c for c in concluidas if c["cenario"] == cen]
        if not proc and not sess_cen and not feitas:
            cenarios[cen] = {"estado": "nao_iniciado", "plano": [], "proc": None}
            continue
        a = proc["args"] if proc else []
        reps = int(arg(a, "--repeticoes", 3))
        max_new = int(arg(a, "--max_new_tokens", 128))
        pausa = float(arg(a, "--pausa_s", 15))
        inicio_carga = min((s["inicio"] for s in sess_cen if s["papel"] in ("cliente", "central", "centralizado")),
                           default=None)
        log = log_do_cenario(cen, (inicio_carga or agora) - 120)
        ev = ler_log(log, ROTULOS[cen]) if log else None

        s_tok_aquec = None
        if ev and ev["inicios"]:
            fim_aquec = (ev["inicios"][1] - pausa) if len(ev["inicios"]) > 1 else None
            ini_aquec = max([ev["inicios"][0]] + [t for t in ev["retentativas"]
                                                    if fim_aquec is None or t < fim_aquec])
            if fim_aquec:
                s_tok_aquec = (fim_aquec - ini_aquec) / TOKENS_AQUECIMENTO

        plano = []
        for rep in range(1, reps + 1):
            for pid in prompts:
                feita = next((c for c in feitas if c["prompt_id"] == pid and c["repeticao"] == rep), None)
                esp, origem = duracao_esperada(cen, pid, feitas, ref, s_tok_aquec, max_new)
                plano.append({"prompt_id": pid, "repeticao": rep, "consulta": feita,
                              "esperada_s": esp, "origem": origem, "estado": "concluida" if feita else "pendente"})

        atual = None
        if proc and len(feitas) < len(plano):
            atual = plano[len(feitas)]
            inicio_consultas = ev["inicios"][1:] if ev else []
            ultimo_fim = max((c["t_fim"] for c in feitas), default=None)
            ini = inicio_consultas[len(feitas)] if len(inicio_consultas) > len(feitas) else None
            if ini and (ultimo_fim is None or ini > ultimo_fim):
                atual["estado"] = "em_andamento"
                atual["inicio"] = ini
                atual["decorrido_s"] = agora - ini
                atual["pct"] = min(0.99, atual["decorrido_s"] / atual["esperada_s"]) if atual["esperada_s"] else None
                g = ev.get("geracao") if ev else None
                if g and g["consulta"] == len(feitas) + 1:  # contagem exata impressa pelo cliente
                    atual["tokens"], atual["max_tokens"] = g["tokens"], g["max"]
                    atual["pct"] = g["tokens"] / g["max"]
                    if g["tokens"] and g["decorrido_s"]:
                        atual["esperada_s"] = g["decorrido_s"] / g["tokens"] * g["max"]
                        atual["origem"] = f"contagem de tokens ({g['tokens']}/{g['max']}), ritmo atual até o limite"
            elif ev and not ev["inicios"]:
                atual["estado"] = "aquecimento"
            else:
                atual["estado"] = "pausa"

        # progresso do cenário em tempo: concluído + atual ÷ total esperado (com pausas)
        total_esp = sum((p["consulta"]["duracao_s"] if p["consulta"] else (p["esperada_s"] or 0)) + pausa
                        for p in plano)
        feito = sum(p["consulta"]["duracao_s"] + pausa for p in plano if p["consulta"])
        if atual and atual["estado"] == "em_andamento":
            feito += pausa + min(atual["decorrido_s"], atual["esperada_s"] or atual["decorrido_s"])
        sem_estimativa = any(p["esperada_s"] is None for p in plano if not p["consulta"])
        restante = None if sem_estimativa else max(0.0, total_esp - feito)
        atraso = None
        if atual and atual["estado"] == "em_andamento" and atual["esperada_s"] and \
                atual["decorrido_s"] > atual["esperada_s"]:
            # passou do esperado: a referência subestima; aplica o mesmo atraso às consultas restantes
            atraso = atual["decorrido_s"] / atual["esperada_s"]
            if restante is not None:
                restante = atraso * sum((q["esperada_s"] or 0) + pausa for q in plano
                                        if not q["consulta"] and q is not atual)

        if ev and ev["relatorio_salvo"]:
            estado = "concluido"
        elif proc:
            estado = "executando"
        elif feitas and len(feitas) >= len(plano):
            estado = "concluido"
        else:
            estado = "interrompido"

        cenarios[cen] = {
            "estado": estado, "proc": proc, "plano": plano, "atual": atual, "log": ev,
            "reps": reps, "max_new": max_new, "pausa": pausa, "feitas": feitas,
            "pct": (feito / total_esp) if total_esp and not sem_estimativa else len(feitas) / len(plano),
            "restante_s": restante, "s_tok_aquec": s_tok_aquec, "atraso": atraso,
        }

    return {"agora": agora, "experimento": exp, "procs": procs, "sessoes": sessoes,
            "concluidas": concluidas, "cenarios": cenarios, "mem": memoria_local(), "cfg": cfg}


# ---------------------------------------------------------------------------
# Validações
# ---------------------------------------------------------------------------

def validar(st):
    v = []  # (nivel: ok|aviso|erro|info, titulo, detalhe)
    procs, agora = st["procs"], st["agora"]

    v.append(("ok", "API de telemetria em execução", f"PID {procs['bancada.telemetria'][0]['pid']}")
             if procs.get("bancada.telemetria") else
             ("erro", "API de telemetria não está rodando nesta máquina", "Sem ela as amostras não são gravadas."))

    servidores_locais = procs.get("drift.cli.run_server", [])
    if servidores_locais:
        v.append(("ok", f"{len(servidores_locais)} servidor(es) Drift ativo(s) nesta máquina",
                  ", ".join(f"PID {p['pid']}" for p in servidores_locais)))

    # sessões: telemetria em dia, encerramentos, identidade das máquinas
    abertas = [s for s in st["sessoes"] if s["fim"] is None]
    for s in st["sessoes"]:
        nome = f"{s['papel']} {s['no_id']} ({s['specs']['cpu_modelo']})"
        if s["fim"] is not None:
            nivel = "info" if s["motivo"] in ("processo_drift_finalizado", "concluido", "fim") else "aviso"
            v.append((nivel, f"Sessão encerrada: {nome}", f"motivo: {s['motivo']}"))
            continue
        atraso = agora - (s["ultima_amostra"] or s["inicio"])
        if atraso > TELEMETRIA_ATRASO_MAX_S:
            v.append(("erro", f"Telemetria parada: {nome}",
                      f"Última amostra há {atraso:.0f} s. Consultas nesta janela ficarão inválidas (cobertura < 95%)."))
        else:
            v.append(("ok", f"Telemetria em dia: {nome}", f"última amostra há {atraso:.0f} s · {s['n_amostras']} amostras"
                                                         f" · CPU por {s['specs']['metodo_cpu']}"))

    por_no = {}
    for s in abertas:
        por_no.setdefault(s["no_id"], set()).add(s["specs"]["maquina_id"])
    for no, ids in por_no.items():
        if len(ids) > 1:
            v.append(("aviso", f"Nome '{no}' usado por {len(ids)} máquinas diferentes",
                      "Os cálculos usam o ID físico e estão corretos, mas os relatórios ficam ambíguos. "
                      "Use --no_id distintos nos próximos experimentos."))

    estimadas = [s for s in abertas if s["specs"]["metodo_cpu"] != "rapl"]
    for s in estimadas:
        v.append(("info", f"Energia ESTIMADA (sem RAPL): {s['no_id']} ({s['specs']['cpu_modelo']})",
                  "TDP × utilização; declarar como limitação."))

    for s in abertas:
        bat = s["specs"].get("bateria")
        if bat and not bat.get("na_tomada"):
            v.append(("aviso", f"{s['no_id']} estava na bateria ao iniciar", "O governador pode reduzir o desempenho."))

    # memória dos servidores (blocos × tamanho do bloco vs. 85% da RAM)
    for s in abertas:
        if s["papel"] != "servidor":
            continue
        nb, modelo = s["extras"].get("num_blocos"), s["extras"].get("modelo")
        gib = gib_por_bloco(modelo, s["extras"].get("torch_dtype")) if nb and modelo else None
        if not gib:
            continue
        uso = nb * gib / s["specs"]["ram_total_gb"]
        txt = f"{nb} blocos ≈ {nb * gib:.1f} GiB de {s['specs']['ram_total_gb']:.1f} GiB ({uso:.0%} da RAM)"
        if uso > LIMITE_RAM:
            v.append(("erro", f"Pesos excedem 85% da RAM: {s['no_id']} ({s['specs']['cpu_modelo']})",
                      txt + ". Swap provável: tempo e energia distorcidos."))
        elif uso > ALERTA_RAM:
            v.append(("aviso", f"Memória no limite: {s['no_id']} ({s['specs']['cpu_modelo']})",
                      txt + ". Com o sistema e o cache de KV, pode haver swap. Verifique com 'free -h' nessa máquina."))

    # cobertura das camadas pelos servidores abertos (intervalos fixos)
    srv = [{"papel": s["papel"], "fim": s["fim"], "no_id": s["no_id"], "extras": s["extras"]} for s in abertas]
    fixos, sem_intervalo = blocos.ocupados(srv)
    camadas = next((s["extras"].get("total_camadas") for s in abertas if s["extras"].get("total_camadas")), None)
    if sem_intervalo:
        v.append(("aviso", f"Servidor(es) sem intervalo fixo de blocos: {', '.join(sem_intervalo)}",
                  "Com --num_blocks o Drift escolhe e rebalanceia os blocos; a cadeia pode mudar entre consultas."))
    if fixos and camadas:
        buracos = blocos.lacunas(camadas, fixos)
        dup = sorted({f"{a}:{b} {no}" for a, b, no in fixos if len(blocos.sobrepostos(a, b, fixos)) > 1})
        if buracos:
            v.append(("erro", "Camadas sem servidor: " + ", ".join(f"{a}:{b}" for a, b in buracos),
                      "O cliente não consegue montar a cadeia completa."))
        if dup:
            v.append(("aviso", "Intervalos de blocos sobrepostos (réplicas)", "; ".join(dup)))
        if not buracos and not dup:
            v.append(("ok", "Camadas cobertas uma vez cada",
                      " · ".join(f"{a}:{b} {no}" for a, b, no in sorted(fixos))))

    mem = st["mem"]
    # pesos residentes: o Drift mapeia os pesos do arquivo (RssFile); o que não cabe é relido do disco
    for np_ in procs.get("bancada.no_p2p", []):
        nb, modelo, dtype = arg(np_["args"], "--num_blocks"), arg(np_["args"], "--modelo"), arg(np_["args"], "--torch_dtype")
        gib = gib_por_bloco(modelo, dtype) if nb and modelo else None
        filhos = [d for d in servidores_locais]
        if not gib or not filhos:
            continue
        try:
            rss = {k: int(v.split()[0]) / 1024**2 for k, v in
                   (l.split(":", 1) for l in open(f"/proc/{filhos[0]['pid']}/status") if l.startswith("Rss"))}
        except OSError:
            continue
        esperado = int(nb) * gib
        frac = rss.get("RssFile", 0) / esperado
        if frac < 0.95:
            v.append(("erro", f"Só {frac:.0%} dos pesos do servidor local estão na RAM",
                      f"{rss.get('RssFile', 0):.1f} de {esperado:.1f} GiB ({nb} blocos). Os pesos são mapeados do arquivo "
                      "e contam como 'cache', não como 'usada': o monitor do sistema mostra RAM folgada enquanto o "
                      "restante dos pesos é descartado e relido do disco a cada token."))
    if mem["swap_in_pag_s"] > 10 or mem["falhas_maiores_s"] > 50:
        v.append(("erro", "Paginação ativa nesta máquina (memória insuficiente)",
                  f"{mem['swap_in_pag_s']:.0f} páginas/s lidas do swap, {mem['swap_out_pag_s']:.0f}/s gravadas, "
                  f"{mem['falhas_maiores_s']:.0f} falhas de página/s (leituras de disco). A CPU espera o disco: "
                  "tempo e energia por token ficam inflados. Rodar cliente e servidor na mesma máquina de "
                  f"{mem['ram_total_gb']:.1f} GiB é a causa provável."))
    elif mem["swap_usado_gb"] > 0.25:
        v.append(("aviso", f"Swap em uso nesta máquina: {mem['swap_usado_gb']:.1f} GiB",
                  f"RAM disponível: {mem['ram_disponivel_gb']:.1f} de {mem['ram_total_gb']:.1f} GiB."))

    for cen, c in st["cenarios"].items():
        r = ROTULOS[cen]
        if c["estado"] == "nao_iniciado":
            continue
        if c["estado"] == "interrompido":
            v.append(("erro", f"{r}: carga interrompida antes do fim",
                      f"{len(c['feitas'])}/{len(c['plano'])} consultas registradas e nenhum processo ativo."))
        inval = [x for x in c["feitas"] if not x["valido"]]
        if c["feitas"]:
            v.append(("ok" if not inval else "erro", f"{r}: {len(c['feitas']) - len(inval)}/{len(c['feitas'])} "
                      "consultas concluídas válidas", "; ".join(" | ".join(x["problemas"]) for x in inval)))
            taxa = statistics.median(x["tok_s"] for x in c["feitas"] if x["tok_s"])
            if taxa < TOK_S_MINIMO_ESPERADO:
                v.append(("aviso", f"{r}: taxa de geração muito baixa ({taxa:.3f} tok/s)",
                          "Um modelo 7B em CPU costuma gerar de 1 a 5 tok/s: verifique swap, relay e dtype."))
        ev = c.get("log")
        if not ev:
            continue
        if ev["erros"]:
            v.append(("aviso", f"{r}: {len(ev['erros'])} erro(s)/retentativa(s) no log",
                      "; ".join(f"{datetime.fromtimestamp(t):%H:%M:%S} {m}" if t else m for t, m in ev["erros"][-3:])))
        for a in sorted(ev["avisos"]):
            v.append(("aviso", f"{r}: {a.split(':')[0]}", a))
        if ev["relay"]:
            v.append(("aviso", f"{r}: algum servidor usa relay", "Tráfego indireto aumenta a latência."))
        medidas = ev["rotas"][1:]  # sem o aquecimento
        if len({rota for _, rota in medidas}) > 1:
            v.append(("aviso", f"{r}: a rota mudou entre consultas medidas",
                      " → ".join(f"{datetime.fromtimestamp(t):%H:%M} {rota.count(' via ')} serv." for t, rota in medidas)
                      + ". Consultas com cadeias diferentes não são comparáveis entre si."))
        elif medidas:
            v.append(("info", f"{r}: rota das consultas com {medidas[-1][1].count(' via ')} servidor(es)",
                      medidas[-1][1]))
        if ev["retentativas"]:
            v.append(("info", f"{r}: {len(ev['retentativas'])} troca(s) de servidor após falha",
                      "Rota parcial refeita pelo cliente; não altera a cadeia das consultas seguintes."))
    return v


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

def hm(seg):
    if seg is None:
        return "—"
    seg = int(seg)
    h, m = divmod(seg // 60, 60)
    return f"{h} h {m:02d} min" if h else f"{m} min {seg % 60:02d} s"


def hora(ts):
    return datetime.fromtimestamp(ts).strftime("%H:%M") if ts else "—"


def barra(pct, rotulo=""):
    p = max(0.0, min(1.0, pct or 0))
    return (f'<div class="barra" role="progressbar" aria-valuenow="{p * 100:.0f}" aria-valuemin="0" '
            f'aria-valuemax="100"><span style="width:{p * 100:.1f}%"></span></div>'
            f'<div class="barra-rot">{p:.0%}{(" · " + rotulo) if rotulo else ""}</div>')


ESTADOS = {"executando": "Em execução", "concluido": "Concluído", "interrompido": "Interrompido",
           "nao_iniciado": "Ainda não iniciado"}
ICONES = {"ok": "✓", "aviso": "!", "erro": "✕", "info": "i"}


def gerar_html(st, validacoes, intervalo):
    e = html.escape
    agora = st["agora"]
    partes = []
    n_erros = sum(1 for n, *_ in validacoes if n == "erro")
    n_avisos = sum(1 for n, *_ in validacoes if n == "aviso")
    saude = ("erro", f"{n_erros} problema(s) grave(s)") if n_erros else \
            ("aviso", f"{n_avisos} aviso(s)") if n_avisos else ("ok", "tudo em ordem")

    # cartões por cenário
    cards = []
    for cen in ORDEM_CENARIOS:
        c = st["cenarios"][cen]
        r = ROTULOS[cen]
        if c["estado"] == "nao_iniciado":
            cards.append(f'<section class="card"><h2>{r}</h2><p class="estado neutro">Ainda não iniciado</p>'
                         f'<p class="sub">Nenhuma sessão nem consulta deste cenário no experimento.</p></section>')
            continue
        at = c.get("atual")
        linha_atual = ""
        if at:
            if at["estado"] == "em_andamento":
                rot = (f'{at["tokens"]}/{at["max_tokens"]} tokens · {hm(at["decorrido_s"])}' if "tokens" in at
                       else f'{hm(at["decorrido_s"])} de ~{hm(at["esperada_s"])}')
                linha_atual = (f'<h3>Consulta atual: {e(at["prompt_id"])} · rep. {at["repeticao"]}</h3>'
                               + barra(at.get("pct"), rot)
                               + f'<p class="sub">Início {hora(at["inicio"])} · término previsto '
                                 f'{hora(at["inicio"] + at["esperada_s"]) if at["esperada_s"] else "—"} · '
                                 f'estimativa: {e(at["origem"])}</p>')
                if at["esperada_s"] and at["decorrido_s"] > at["esperada_s"]:
                    linha_atual = (f'<h3>Consulta atual: {e(at["prompt_id"])} · rep. {at["repeticao"]}</h3>'
                                   + barra(0.99, f'{hm(at["decorrido_s"])} decorridos · esperado ~{hm(at["esperada_s"])}')
                                   + f'<p class="sub alerta">Sem previsão confiável: já passou '
                                     f'{hm(at["decorrido_s"] - at["esperada_s"])} do término esperado '
                                     f'({hora(at["inicio"] + at["esperada_s"])}), {c["atraso"]:.2f}× mais lenta que a '
                                     f'referência. O cliente não informa os tokens gerados, então só se sabe que ela '
                                     f'termina quando for registrada.</p>')
            elif at["estado"] == "aquecimento":
                linha_atual = '<h3>Aquecimento em andamento (não contabilizado)</h3>'
            else:
                linha_atual = f'<h3>Pausa antes de {e(at["prompt_id"])} · rep. {at["repeticao"]}</h3>'
        fim_prev = ""
        if c["restante_s"] is not None and c["estado"] == "executando":
            fim_prev = (f'término previsto do cenário: <b>{"após " if c["atraso"] else ""}{hora(agora + c["restante_s"])}'
                        f'</b> (faltam {"mais de " if c["atraso"] else "~"}{hm(c["restante_s"])}'
                        + (f', supondo o mesmo atraso de {c["atraso"]:.2f}× nas próximas consultas' if c["atraso"] else "")
                        + ")")
        cards.append(
            f'<section class="card"><h2>{r}</h2>'
            f'<p class="estado {c["estado"]}">{ESTADOS[c["estado"]]}</p>'
            f'<h3>Cenário: {len(c["feitas"])}/{len(c["plano"])} consultas</h3>'
            + barra(c["pct"], "tempo estimado, com pausas")
            + f'<p class="sub">{fim_prev}</p>' + linha_atual
            + f'<p class="sub">{c["reps"]} repetições × {len(c["plano"]) // max(1, c["reps"])} prompts · '
              f'máx. {c["max_new"]} tokens · pausa {c["pausa"]:.0f} s</p></section>')

    # tabela de consultas
    linhas = []
    for cen in ORDEM_CENARIOS:
        c = st["cenarios"][cen]
        for p in c["plano"]:
            q = p["consulta"]
            if q:
                est = '<span class="tag ok">válida</span>' if q["valido"] else \
                      f'<span class="tag erro" title="{e(" | ".join(q["problemas"]))}">inválida</span>'
                dados = (f'<td class="num">{hm(q["duracao_s"])}</td><td class="num">{q["tokens_saida"]}</td>'
                         f'<td class="num">{q["tok_s"]:.3f}</td><td class="num">{q["total_wh"]:.2f}</td>'
                         f'<td class="num">{q["wh_token"]:.4f}</td>' if q["wh_token"] is not None else
                         f'<td class="num">{hm(q["duracao_s"])}</td><td class="num">{q["tokens_saida"]}</td>'
                         f'<td colspan="3"></td>')
            else:
                if p["estado"] == "em_andamento":
                    est = f'<span class="tag andamento">em andamento · ~{(p.get("pct") or 0):.0%}</span>'
                elif p["estado"] in ("pausa", "aquecimento"):
                    est = '<span class="tag andamento">a seguir</span>'
                else:
                    est = '<span class="tag neutro">pendente</span>'
                dados = (f'<td class="num muted">~{hm(p["esperada_s"])}</td><td colspan="4" class="muted">'
                         f'{e(p["origem"])}</td>')
            linhas.append(f'<tr><td>{ROTULOS[cen]}</td><td>{e(p["prompt_id"])}</td><td class="num">{p["repeticao"]}'
                          f'</td><td>{est}</td>{dados}</tr>')

    vals = "".join(f'<li class="val {n}"><span class="ico" aria-hidden="true">{ICONES[n]}</span>'
                   f'<div><b>{e(t)}</b>{"<br><span class=sub>" + e(d) + "</span>" if d else ""}</div></li>'
                   for n, t, d in sorted(validacoes, key=lambda x: ["erro", "aviso", "ok", "info"].index(x[0])))

    sess = "".join(
        f'<tr><td>{e(s["papel"])}</td><td>{e(s["no_id"])}</td><td>{e(s["specs"]["cpu_modelo"])}</td>'
        f'<td class="num">{s["specs"]["ram_total_gb"]:.1f}</td><td class="num">{s["extras"].get("num_blocos") or "—"}</td>'
        f'<td>{e(s["specs"]["metodo_cpu"])}</td><td>{e(s["specs"].get("rede_ip") or "")}</td>'
        f'<td>{"aberta" if s["fim"] is None else e("encerrada: " + str(s["motivo"]))}</td></tr>'
        for s in st["sessoes"])

    refresh = f'<meta http-equiv="refresh" content="{intervalo}">' if intervalo else ""
    partes.append(f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">{refresh}
<title>Progresso {e(st['experimento'])}</title>
<style>
:root{{--bg:#f7f7f5;--card:#fff;--tx:#1d1d1b;--mut:#6b6b66;--bd:#e3e2dd;--acc:#2f6fdb;
--ok:#1f8a4c;--av:#b26a00;--er:#c62828;--neu:#8a8a85;}}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]){{--bg:#151514;--card:#1f1f1d;--tx:#ecebe6;
--mut:#a3a29c;--bd:#33332f;--acc:#6ea0ff;--ok:#4cc27e;--av:#f0a640;--er:#ff6b6b;--neu:#8f8f89;}}}}
:root[data-theme="dark"]{{--bg:#151514;--card:#1f1f1d;--tx:#ecebe6;--mut:#a3a29c;--bd:#33332f;--acc:#6ea0ff;
--ok:#4cc27e;--av:#f0a640;--er:#ff6b6b;--neu:#8f8f89;}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--tx);
font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}}
main{{max-width:1100px;margin:auto;padding:28px 16px 60px}}
h1{{font-size:1.5rem;margin:0 0 4px}}h2{{font-size:1.1rem;margin:0 0 6px}}h3{{font-size:.95rem;margin:14px 0 6px}}
.sub{{color:var(--mut);font-size:.85rem;margin:4px 0}}.alerta{{color:var(--av)}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px;margin:20px 0}}
.card{{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:18px}}
.estado{{display:inline-block;font-weight:600;font-size:.85rem;margin:0 0 4px}}
.executando{{color:var(--acc)}}.concluido{{color:var(--ok)}}.interrompido{{color:var(--er)}}.neutro{{color:var(--neu)}}
.barra{{height:10px;background:var(--bd);border-radius:6px;overflow:hidden}}
.barra span{{display:block;height:100%;background:var(--acc)}}
.barra-rot{{font-size:.85rem;margin-top:4px;font-variant-numeric:tabular-nums}}
.saude{{display:inline-block;padding:2px 10px;border-radius:99px;font-size:.85rem;font-weight:600;
border:1px solid currentColor}}.saude.ok{{color:var(--ok)}}.saude.aviso{{color:var(--av)}}.saude.erro{{color:var(--er)}}
ul.vals{{list-style:none;padding:0;margin:0}}
.val{{display:flex;gap:10px;padding:8px 0;border-top:1px solid var(--bd)}}.val:first-child{{border-top:0}}
.ico{{flex:none;width:22px;height:22px;border-radius:50%;display:grid;place-items:center;font-size:.75rem;
font-weight:700;color:var(--card)}}
.val.ok .ico{{background:var(--ok)}}.val.aviso .ico{{background:var(--av)}}.val.erro .ico{{background:var(--er)}}
.val.info .ico{{background:var(--neu)}}
.tabela{{overflow-x:auto}}table{{border-collapse:collapse;width:100%;font-size:.85rem}}
th,td{{text-align:left;padding:6px 8px;border-bottom:1px solid var(--bd);white-space:nowrap}}
th{{color:var(--mut);font-weight:600}}.num{{text-align:right;font-variant-numeric:tabular-nums}}
.muted{{color:var(--mut);white-space:normal}}
.tag{{font-size:.75rem;padding:1px 8px;border-radius:99px;border:1px solid currentColor}}
.tag.ok{{color:var(--ok)}}.tag.erro{{color:var(--er)}}.tag.andamento{{color:var(--acc)}}.tag.neutro{{color:var(--neu)}}
footer{{color:var(--mut);font-size:.8rem;margin-top:24px}}
</style></head><body><main>
<h1>Experimento {e(st['experimento'])}</h1>
<p class="sub">Gerado às {datetime.fromtimestamp(agora):%H:%M:%S} de {datetime.fromtimestamp(agora):%d/%m/%Y}
{f"· atualiza a cada {intervalo} s" if intervalo else ""} · <span class="saude {saude[0]}">{saude[1]}</span></p>
<div class="grid">{''.join(cards)}</div>
<section class="card"><h2>Validações</h2><ul class="vals">{vals}</ul></section>
<section class="card" style="margin-top:16px"><h2>Consultas</h2>
<p class="sub">Concluídas: valores calculados pelo mesmo código da API. Pendentes: duração esperada e a base da estimativa.</p>
<div class="tabela"><table><thead><tr><th>Cenário</th><th>Prompt</th><th class="num">Rep.</th><th>Situação</th>
<th class="num">Duração</th><th class="num">Tokens</th><th class="num">tok/s</th><th class="num">Wh</th>
<th class="num">Wh/token</th></tr></thead><tbody>{''.join(linhas)}</tbody></table></div></section>
<section class="card" style="margin-top:16px"><h2>Máquinas (sessões)</h2><div class="tabela"><table>
<thead><tr><th>Papel</th><th>Nó</th><th>CPU</th><th class="num">RAM (GiB)</th><th class="num">Blocos</th>
<th>Medição</th><th>IP</th><th>Sessão</th></tr></thead><tbody>{sess}</tbody></table></div></section>
<footer>O progresso da consulta atual é estimado (o cliente só registra a consulta ao terminar).
Leitura somente do banco, dos logs e de /proc; nada é enviado à API.</footer>
</main></body></html>""")
    return "".join(partes)


# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--experimento", help="Padrão: o mais recente no banco")
    p.add_argument("--banco", default=os.environ.get("TELEMETRIA_DB", str(caminhos.BANCO)))
    p.add_argument("--prompts", default=str(caminhos.PROMPTS))
    p.add_argument("--referencia", nargs="*", default=[],
                   help="CSV(s) de experimentos anteriores (relatorio.csv) para estimar durações")
    p.add_argument("--saida", help="Padrão: dados/resultados/progresso_<experimento>.html")
    p.add_argument("--intervalo", type=int, default=0, help="Regerar a cada N s (0 = uma vez)")
    args = p.parse_args()
    try:
        os.nice(19)
    except OSError:
        pass

    while True:
        st = coletar(args)
        validacoes = validar(st)
        saida = Path(args.saida or caminhos.DIR_RESULTADOS / f"progresso_{st['experimento']}.html")
        saida.parent.mkdir(parents=True, exist_ok=True)
        tmp = saida.with_suffix(".tmp")
        tmp.write_text(gerar_html(st, validacoes, args.intervalo), encoding="utf-8")
        tmp.replace(saida)
        resumo = []
        for cen in ORDEM_CENARIOS:
            c = st["cenarios"][cen]
            if c["estado"] == "nao_iniciado":
                resumo.append(f"{ROTULOS[cen]}: não iniciado")
                continue
            at = c.get("atual")
            extra = (f", consulta atual {at['prompt_id']} rep {at['repeticao']} ~{at['pct']:.0%}"
                     if at and at.get("pct") is not None else "")
            resumo.append(f"{ROTULOS[cen]}: {len(c['feitas'])}/{len(c['plano'])} ({c['pct']:.0%}){extra}")
        n_err = sum(1 for v in validacoes if v[0] == "erro")
        n_av = sum(1 for v in validacoes if v[0] == "aviso")
        print(f"[{datetime.now():%H:%M:%S}] {' | '.join(resumo)} | {n_err} erro(s), {n_av} aviso(s) → {saida}")
        if not args.intervalo:
            break
        time.sleep(args.intervalo)


if __name__ == "__main__":
    main()
