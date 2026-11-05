"""
API coletora de telemetria (roda no nó principal).

Rotas:
  GET  /tempo                         relógio de referência (sincronização dos nós)
  GET  /config                        parâmetros metodológicos em vigor
  POST /sessoes                       registra uma máquina medida (specs obrigatórias)
  POST /sessoes/<id>/encerrar         encerra a sessão
  POST /sessoes/<id>/anunciar         o nó publica o próprio multiaddr (fonte única do endereço)
  POST /amostras                      lote de intervalos brutos de uma sessão
  POST /consultas                     registra a janela de uma consulta (início/fim/tokens)
  GET  /consultas/<id>/relatorio      cálculo ambiental de uma consulta
  GET  /relatorio?experimento=X       todas as consultas + agregados por cenário
  GET  /relatorio.csv?experimento=X   uma linha por consulta (para planilha/gráficos)
  GET  /sessoes?experimento=X         máquinas registradas
  GET  /sessoes/<id>/intervalos       amostras brutas de uma sessão (exportação para análise)

Os cálculos são feitos a partir de config_tcc.json: mudar um parâmetro não exige
repetir experimentos. O relatório de uma consulta já encerrada fica em cache; o cache
é descartado quando config_tcc.json muda ou quando chegam amostras atrasadas.

Idempotência (para os nós poderem repetir envios após falha de rede):
  - POST /sessoes aceita 'sessao_id' proposto pelo nó; repetir devolve a mesma sessão.
  - POST /consultas aceita 'chave'; repetir com a mesma chave devolve a mesma consulta.
  - POST /amostras ignora intervalos já gravados (mesma sessão e mesmo t_ini/t_fim).

Uso:  python -m bancada.telemetria            (variáveis: TELEMETRIA_DB, TELEMETRIA_PORTA, TELEMETRIA_CONFIG)
"""

import csv
import http.server
import io
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from urllib.parse import parse_qs, urlparse

from . import calculo
from .caminhos import BANCO, garantir_diretorios

DB_PATH = os.environ.get("TELEMETRIA_DB", str(BANCO))
PORTA = int(os.environ.get("TELEMETRIA_PORTA", "5000"))
CONFIG = os.environ.get("TELEMETRIA_CONFIG", str(calculo.CONFIG_PADRAO))

ID_SESSAO = re.compile(r"[0-9a-f]{12}")

SPECS_OBRIGATORIAS = [
    "maquina_id", "hostname", "cpu_modelo", "cpu_nucleos_logicos", "tdp_w", "tdp_fonte",
    "ram_total_gb", "metodo_cpu", "metodo_ram", "rede_interface",
]


def conectar():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_banco():
    with conectar() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS sessoes (
            id TEXT PRIMARY KEY,
            experimento TEXT NOT NULL,
            cenario TEXT NOT NULL,
            papel TEXT NOT NULL,
            no_id TEXT NOT NULL,
            maquina_id TEXT NOT NULL,
            hostname TEXT,
            inicio REAL NOT NULL,
            fim REAL,
            motivo TEXT,
            relogio_offset_s REAL,
            relogio_incerteza_s REAL,
            specs_json TEXT NOT NULL,
            extras_json TEXT,
            registrado_em TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS intervalos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sessao_id TEXT NOT NULL REFERENCES sessoes(id),
            fase TEXT NOT NULL,
            t_ini REAL NOT NULL,
            t_fim REAL NOT NULL,
            cpu_util REAL NOT NULL,
            e_pkg_j REAL,
            e_dram_j REAL,
            bytes_tx INTEGER NOT NULL,
            bytes_rx INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_int_sessao_t ON intervalos(sessao_id, t_fim);
        CREATE TABLE IF NOT EXISTS consultas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experimento TEXT NOT NULL,
            cenario TEXT NOT NULL,
            prompt_id TEXT,
            repeticao INTEGER,
            t_inicio REAL NOT NULL,
            t_fim REAL NOT NULL,
            tokens_entrada INTEGER,
            tokens_saida INTEGER,
            resposta TEXT,
            extras_json TEXT,
            registrado_em TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_cons_exp ON consultas(experimento, cenario);
        """)
        colunas = {r["name"] for r in c.execute("PRAGMA table_info(sessoes)")}
        if "multiaddr" not in colunas:  # migração de bancos anteriores
            c.execute("ALTER TABLE sessoes ADD COLUMN multiaddr TEXT")
        colunas = {r["name"] for r in c.execute("PRAGMA table_info(consultas)")}
        if "chave" not in colunas:  # migração: chave de idempotência das consultas
            c.execute("ALTER TABLE consultas ADD COLUMN chave TEXT")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cons_chave ON consultas(chave)")
        # linha de base e deduplicação de amostras sem varrer a sessão inteira
        c.execute("CREATE INDEX IF NOT EXISTS idx_int_sessao_fase ON intervalos(sessao_id, fase, t_ini)")


def agora_iso():
    return datetime.now().isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Regras
# ---------------------------------------------------------------------------

class ErroRequisicao(Exception):
    def __init__(self, codigo, msg):
        self.codigo, self.msg = codigo, msg


def registrar_sessao(d):
    cfg = calculo.carregar_config(CONFIG)
    specs = d.get("specs") or {}
    faltando = [k for k in SPECS_OBRIGATORIAS if specs.get(k) in (None, "")]
    if faltando:
        raise ErroRequisicao(422, f"Specs obrigatórias ausentes: {', '.join(faltando)}")
    if specs["metodo_cpu"] not in ("rapl", "modelo_tdp"):
        raise ErroRequisicao(422, f"metodo_cpu inválido: {specs['metodo_cpu']}")
    if d.get("cenario") not in cfg["cenarios"]:
        raise ErroRequisicao(422, f"Cenário desconhecido: {d.get('cenario')} (use {list(cfg['cenarios'])})")
    for k in ("experimento", "papel", "no_id", "inicio"):
        if not d.get(k):
            raise ErroRequisicao(422, f"Campo obrigatório ausente: {k}")
    sid = d.get("sessao_id") or uuid.uuid4().hex[:12]
    if not ID_SESSAO.fullmatch(str(sid)):
        raise ErroRequisicao(422, f"sessao_id inválido: {sid!r} (12 caracteres hexadecimais)")
    with conectar() as c:
        existente = c.execute("SELECT no_id FROM sessoes WHERE id=?", (sid,)).fetchone()
        if existente:  # o nó repetiu o registro (a resposta anterior se perdeu)
            if existente["no_id"] != str(d["no_id"]):
                raise ErroRequisicao(409, f"sessao_id {sid} já pertence a outro nó.")
            return {"sessao_id": sid, "repetido": True}
        c.execute(
            """INSERT INTO sessoes (id, experimento, cenario, papel, no_id, maquina_id, hostname, inicio,
               relogio_offset_s, relogio_incerteza_s, specs_json, extras_json, registrado_em)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sid, d["experimento"], d["cenario"], d["papel"], str(d["no_id"]), specs["maquina_id"],
             specs["hostname"], d["inicio"], d.get("relogio_offset_s"), d.get("relogio_incerteza_s"),
             json.dumps(specs, ensure_ascii=False), json.dumps(d.get("extras") or {}, ensure_ascii=False),
             agora_iso()),
        )
    print(f"[API] Sessão {sid}: {d['papel']} '{d['no_id']}' ({specs['hostname']}, {specs['cpu_modelo']}, "
          f"CPU={specs['metodo_cpu']}, RAM={specs['metodo_ram']}) — exp '{d['experimento']}' / {d['cenario']}")
    return {"sessao_id": sid}


def gravar_amostras(d):
    sid = d.get("sessao_id")
    ivs = d.get("intervalos") or []
    with conectar() as c:
        if not c.execute("SELECT 1 FROM sessoes WHERE id=?", (sid,)).fetchone():
            raise ErroRequisicao(404, f"Sessão inexistente: {sid}")
        gravados = 0
        for i in ivs:
            # um lote reenviado (resposta perdida) não pode duplicar energia
            gravados += c.execute(
                """INSERT INTO intervalos (sessao_id, fase, t_ini, t_fim, cpu_util, e_pkg_j, e_dram_j, bytes_tx, bytes_rx)
                   SELECT ?,?,?,?,?,?,?,?,?
                   WHERE NOT EXISTS (SELECT 1 FROM intervalos WHERE sessao_id=? AND t_fim=? AND t_ini=?)""",
                (sid, i["fase"], i["t_ini"], i["t_fim"], i["cpu_util"], i.get("e_pkg_j"), i.get("e_dram_j"),
                 int(i["bytes_tx"]), int(i["bytes_rx"]), sid, i["t_fim"], i["t_ini"]),
            ).rowcount
    if ivs and min(i["t_fim"] for i in ivs) < time.time() - ATRASO_MAXIMO_S:
        cache.invalidar()  # amostras atrasadas podem mudar relatórios já calculados
    return {"status": "ok", "gravados": gravados, "repetidos": len(ivs) - gravados}


def encerrar_sessao(sid, d):
    with conectar() as c:
        n = c.execute("UPDATE sessoes SET fim=?, motivo=? WHERE id=?",
                      (d.get("fim", time.time()), d.get("motivo"), sid)).rowcount
    if not n:
        raise ErroRequisicao(404, f"Sessão inexistente: {sid}")
    print(f"[API] Sessão {sid} encerrada: {d.get('motivo')}")
    return {"status": "ok"}


def anunciar_multiaddr(sid, d):
    multiaddr = (d.get("multiaddr") or "").strip()
    if not multiaddr.startswith("/ip4/") or "/p2p/" not in multiaddr:
        raise ErroRequisicao(422, f"multiaddr inválido: {multiaddr!r}")
    with conectar() as c:
        n = c.execute("UPDATE sessoes SET multiaddr=? WHERE id=?", (multiaddr, sid)).rowcount
    if not n:
        raise ErroRequisicao(404, f"Sessão inexistente: {sid}")
    print(f"[API] Sessão {sid} anunciou {multiaddr}")
    return {"status": "ok"}


def registrar_consulta(d):
    cfg = calculo.carregar_config(CONFIG)
    if d.get("cenario") not in cfg["cenarios"]:
        raise ErroRequisicao(422, f"Cenário desconhecido: {d.get('cenario')}")
    if not d.get("experimento"):
        raise ErroRequisicao(422, "Campo obrigatório ausente: experimento")
    if not (d.get("t_fim", 0) > d.get("t_inicio", 0)):
        raise ErroRequisicao(422, "Janela inválida (t_fim deve ser maior que t_inicio).")
    chave = d.get("chave") or None
    with conectar() as c:
        if chave:
            existente = c.execute("SELECT id FROM consultas WHERE chave=?", (chave,)).fetchone()
            if existente:  # o cliente repetiu o envio (a resposta anterior se perdeu)
                return {"consulta_id": existente["id"], "repetido": True}
        cid = c.execute(
            """INSERT INTO consultas (experimento, cenario, prompt_id, repeticao, t_inicio, t_fim,
               tokens_entrada, tokens_saida, resposta, extras_json, registrado_em, chave)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (d["experimento"], d["cenario"], d.get("prompt_id"), d.get("repeticao"), d["t_inicio"], d["t_fim"],
             d.get("tokens_entrada"), d.get("tokens_saida"), d.get("resposta"),
             json.dumps(d.get("extras") or {}, ensure_ascii=False), agora_iso(), chave),
        ).lastrowid
    return {"consulta_id": cid}


def _sessoes_da_janela(c, consulta, memo=None):
    """Sessões que cruzam a janela, com os intervalos da janela e os da linha de base.

    memo guarda, por sessão, a duração máxima de um intervalo e a linha de base: num
    relatório de experimento elas são lidas uma vez só, não uma vez por consulta.
    """
    memo = {} if memo is None else memo
    t0, t1 = consulta["t_inicio"], consulta["t_fim"]
    rows = c.execute(
        "SELECT * FROM sessoes WHERE experimento=? AND cenario=? AND inicio<=? AND (fim IS NULL OR fim>=?)",
        (consulta["experimento"], consulta["cenario"], t1, t0),
    ).fetchall()
    sessoes = []
    for s in rows:
        if s["id"] not in memo:
            dur_max = c.execute("SELECT MAX(t_fim - t_ini) FROM intervalos WHERE sessao_id=?",
                                (s["id"],)).fetchone()[0] or 0.0
            base = c.execute(
                "SELECT * FROM intervalos WHERE sessao_id=? AND fase='baseline' ORDER BY t_ini", (s["id"],)
            ).fetchall()
            memo[s["id"]] = (dur_max, _sem_repetidos(base))
        dur_max, base = memo[s["id"]]
        # Todo intervalo que cruza [t0, t1] termina em (t0, t1 + dur_max]: limitar t_fim pelos
        # dois lados deixa o índice (sessao_id, t_fim) ler só a janela, não o resto da sessão.
        ivs = c.execute(
            "SELECT * FROM intervalos WHERE sessao_id=? AND t_fim>? AND t_fim<=? AND t_ini<? ORDER BY t_ini",
            (s["id"], t0, t1 + dur_max, t1),
        ).fetchall()
        sessoes.append({
            "id": s["id"], "no_id": s["no_id"], "papel": s["papel"], "inicio": s["inicio"],
            "specs": json.loads(s["specs_json"]),
            "intervalos": _sem_repetidos(ivs),
            "intervalos_base": base,
        })
    return sessoes


def _sem_repetidos(rows):
    """Bancos antigos podem ter lotes gravados duas vezes (reenvio após falha): conta cada intervalo uma vez."""
    vistos, saida = set(), []
    for r in rows:
        k = (r["t_ini"], r["t_fim"])
        if k not in vistos:
            vistos.add(k)
            saida.append(dict(r))
    return saida


# ---------------------------------------------------------------------------
# Cache e controle de concorrência dos relatórios
# ---------------------------------------------------------------------------

ESTAVEL_APOS_S = 120     # consulta encerrada há mais que isso: as amostras dela já chegaram
ATRASO_MAXIMO_S = 60     # amostras mais velhas que isso chegando = atraso (invalida o cache)
TTL_EXPERIMENTO_S = 10   # relatório completo reaproveitado por requisições simultâneas
CALCULOS_SIMULTANEOS = 2  # protege a CPU do nó central (ele também roda blocos do modelo)


class CacheRelatorios:
    def __init__(self):
        self._lock = threading.Lock()
        self._consultas = {}      # consulta_id -> relatório
        self._experimentos = {}   # experimento -> (instante, relatório)
        self._locks_exp = {}
        self._mtime_config = None
        self.calculos = threading.BoundedSemaphore(CALCULOS_SIMULTANEOS)

    def _conferir_config(self):
        try:
            m = os.path.getmtime(CONFIG)
        except OSError:
            m = None
        if m != self._mtime_config:
            self._mtime_config = m
            self._consultas.clear()
            self._experimentos.clear()

    def invalidar(self):
        with self._lock:
            self._consultas.clear()
            self._experimentos.clear()

    def consulta(self, cid):
        with self._lock:
            self._conferir_config()
            return self._consultas.get(cid)

    def guardar_consulta(self, cid, rel, t_fim):
        if t_fim < time.time() - ESTAVEL_APOS_S:
            with self._lock:
                self._consultas[cid] = rel

    def lock_experimento(self, experimento):
        with self._lock:
            return self._locks_exp.setdefault(experimento, threading.Lock())

    def experimento(self, experimento):
        with self._lock:
            self._conferir_config()
            item = self._experimentos.get(experimento)
            if item and time.time() - item[0] < TTL_EXPERIMENTO_S:
                return item[1]
            return None

    def guardar_experimento(self, experimento, rel):
        with self._lock:
            self._experimentos[experimento] = (time.time(), rel)


cache = CacheRelatorios()


def _relatorio_consulta(c, cid, cfg, memo):
    pronto = cache.consulta(cid)
    if pronto is not None:
        return pronto
    row = c.execute("SELECT * FROM consultas WHERE id=?", (cid,)).fetchone()
    if not row:
        raise ErroRequisicao(404, f"Consulta inexistente: {cid}")
    consulta = dict(row)
    rel = calculo.relatorio_consulta(consulta, _sessoes_da_janela(c, consulta, memo), cfg)
    cache.guardar_consulta(cid, rel, consulta["t_fim"])
    return rel


def relatorio_consulta(cid, cfg=None):
    pronto = cache.consulta(cid)
    if pronto is not None:
        return pronto
    cfg = cfg or calculo.carregar_config(CONFIG)
    with cache.calculos, conectar() as c:
        return _relatorio_consulta(c, cid, cfg, {})


def relatorio_experimento(experimento):
    pronto = cache.experimento(experimento)
    if pronto is not None:
        return pronto
    # Um cálculo por experimento por vez: quem chega enquanto ele roda espera e reaproveita.
    with cache.lock_experimento(experimento):
        pronto = cache.experimento(experimento)
        if pronto is not None:
            return pronto
        cfg = calculo.carregar_config(CONFIG)
        with cache.calculos, conectar() as c:
            ids = [r["id"] for r in c.execute(
                "SELECT id FROM consultas WHERE experimento=? ORDER BY id", (experimento,))]
            memo = {}
            rels = [_relatorio_consulta(c, i, cfg, memo) for i in ids]
        rel = {"experimento": experimento, "agregado": calculo.agregar(rels), "consultas": rels}
        cache.guardar_experimento(experimento, rel)
        return rel


COLUNAS_CSV = [
    ("consulta_id", lambda r: r["consulta_id"]),
    ("cenario", lambda r: r["cenario"]),
    ("prompt_id", lambda r: r["prompt_id"]),
    ("repeticao", lambda r: r["repeticao"]),
    ("valido", lambda r: r["valido"]),
    ("duracao_s", lambda r: r["duracao_s"]),
    ("tokens_entrada", lambda r: r["tokens_entrada"]),
    ("tokens_saida", lambda r: r["tokens_saida"]),
    ("tokens_por_s", lambda r: r["tokens_por_s"]),
    ("n_maquinas", lambda r: len(r["maquinas"])),
    ("ti_bruta_wh", lambda r: r["energia"]["ti_bruta_wh"]),
    ("ti_marginal_wh", lambda r: r["energia"]["ti_marginal_wh"]),
    ("pue", lambda r: r["energia"]["pue"]),
    ("rede_wh", lambda r: r["energia"]["rede_wh"]),
    ("total_wh", lambda r: r["energia"]["total_wh"]),
    ("carbono_g", lambda r: r["carbono_g"]),
    ("agua_total_ml", lambda r: r["agua_total_ml"]),
    ("wh_por_token", lambda r: r["por_token"]["wh"] if r["por_token"] else None),
    ("mg_co2_por_token", lambda r: r["por_token"]["mg_co2"] if r["por_token"] else None),
    ("problemas", lambda r: " | ".join(r["problemas"])),
]


def para_csv(rel):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([n for n, _ in COLUNAS_CSV])
    for r in rel["consultas"]:
        w.writerow([f(r) for _, f in COLUNAS_CSV])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

class Handler(http.server.BaseHTTPRequestHandler):
    def _json(self, codigo, corpo):
        dados = json.dumps(corpo, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def _corpo(self):
        n = int(self.headers.get("Content-Length", 0))
        try:
            d = json.loads(self.rfile.read(n).decode("utf-8") or "{}")
        except json.JSONDecodeError as e:
            raise ErroRequisicao(400, f"JSON inválido: {e}")
        if not isinstance(d, dict):
            raise ErroRequisicao(400, "O corpo deve ser um objeto JSON.")
        return d

    def _executar(self, fn):
        try:
            fn()
        except ErroRequisicao as e:
            self._json(e.codigo, {"erro": e.msg})
        except calculo.ParametroInvalido as e:
            self._json(500, {"erro": f"Configuração inválida: {e}"})
        except Exception as e:
            print(f"[API] ❌ {type(e).__name__}: {e}")
            self._json(500, {"erro": f"{type(e).__name__}: {e}"})

    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        partes = [p for p in url.path.split("/") if p]

        def rota():
            if url.path == "/tempo":
                return self._json(200, {"t": time.time()})
            if url.path == "/config":
                return self._json(200, calculo.carregar_config(CONFIG))
            if url.path in ("/relatorio", "/relatorio.csv"):
                if not q.get("experimento"):
                    raise ErroRequisicao(400, "Informe ?experimento=")
                rel = relatorio_experimento(q["experimento"])
                if url.path.endswith(".csv"):
                    dados = para_csv(rel).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/csv; charset=utf-8")
                    self.send_header("Content-Length", str(len(dados)))
                    self.end_headers()
                    return self.wfile.write(dados)
                return self._json(200, rel)
            if len(partes) == 3 and partes[0] == "consultas" and partes[2] == "relatorio":
                return self._json(200, relatorio_consulta(int(partes[1])))
            if len(partes) == 3 and partes[0] == "sessoes" and partes[2] == "intervalos":
                with conectar() as c:
                    rows = [dict(r) for r in c.execute(
                        "SELECT * FROM intervalos WHERE sessao_id=? ORDER BY t_ini", (partes[1],))]
                return self._json(200, {"sessao_id": partes[1], "intervalos": rows})
            if url.path == "/sessoes":
                with conectar() as c:
                    sql, args = "SELECT * FROM sessoes", ()
                    if q.get("experimento"):
                        sql, args = sql + " WHERE experimento=?", (q["experimento"],)
                    rows = [dict(r) for r in c.execute(sql + " ORDER BY inicio", args)]
                for r in rows:
                    r["specs"] = json.loads(r.pop("specs_json"))
                    r["extras"] = json.loads(r.pop("extras_json") or "{}")
                return self._json(200, {"total": len(rows), "sessoes": rows})
            raise ErroRequisicao(404, f"Rota inexistente: {url.path}")

        self._executar(rota)

    def do_POST(self):
        url = urlparse(self.path)
        partes = [p for p in url.path.split("/") if p]

        def rota():
            d = self._corpo()
            if url.path == "/sessoes":
                return self._json(200, registrar_sessao(d))
            if url.path == "/amostras":
                return self._json(200, gravar_amostras(d))
            if url.path == "/consultas":
                return self._json(200, registrar_consulta(d))
            if len(partes) == 3 and partes[0] == "sessoes" and partes[2] == "encerrar":
                return self._json(200, encerrar_sessao(partes[1], d))
            if len(partes) == 3 and partes[0] == "sessoes" and partes[2] == "anunciar":
                return self._json(200, anunciar_multiaddr(partes[1], d))
            raise ErroRequisicao(404, f"Rota inexistente: {url.path}")

        self._executar(rota)

    def log_message(self, *args):
        return


class ServidorAPI(http.server.ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128  # o padrão (5) recusa conexões quando vários nós enviam ao mesmo tempo


if __name__ == "__main__":
    calculo.carregar_config(CONFIG)  # falha cedo se a configuração estiver inválida
    garantir_diretorios()
    inicializar_banco()
    servidor = ServidorAPI(("0.0.0.0", PORTA), Handler)
    print(f"[API] Banco: {os.path.abspath(DB_PATH)}")
    print(f"[API] Configuração: {CONFIG}")
    print(f"[API] Ouvindo na porta {PORTA}...", flush=True)
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        print("\n[API] Encerrada.")
