"""Plano de controle: interface web + orquestração dos experimentos.

Uso (na raiz do projeto, em qualquer máquina que alcance os agentes):
    export BANCADA_TOKEN=<o mesmo dos agentes>
    python -m painel.servidor               # http://127.0.0.1:8080 (somente local)
    python -m painel.servidor --host 0.0.0.0  # exposto: exige o token (abra /login?token=...)

Rotas:
    GET  /                       interface
    GET  /login?token=...        grava o token em cookie (quando o painel está exposto)
    GET  /api/estado             configuração, máquinas e experimento atual
    GET  /api/eventos            Server-Sent Events (máquinas, logs, experimento, relatório)
    POST /api/specs              valida as specs de todas as máquinas
    POST /api/experimentos       {"cenario": "p2p"|"centralizado", "parametros": {...}}
    POST /api/experimentos/cancelar
    POST /api/parar-tudo         para servidores, clientes e centralizado em todas as máquinas
    GET  /api/relatorio?experimento=X
    GET  /api/logs?maquina=M&papel=P   histórico do log (o SSE envia só as linhas novas)
"""

import argparse
import hmac
import os
import signal
import threading
from pathlib import Path

from .adaptadores import ClienteAgente, ClienteTelemetria, ErroRemoto, Indisponivel
from .configuracao import ConfiguracaoInvalida, carregar
from .eventos import Barramento
from .http_base import (COOKIE_TOKEN, ErroHTTP, ManipuladorBase, criar_servidor, endereco_local,
                        exigir_token_se_exposto)
from .monitor import Monitor, resumir_relatorio
from .orquestrador import Orquestrador, ParametrosInvalidos

ESTATICOS = Path(__file__).resolve().parent / "static"


class Painel:
    """Facade: compõe as dependências e oferece as operações usadas pela interface."""

    def __init__(self, config, token):
        self.config = config
        self.agentes = {m.nome: ClienteAgente(m.agente, token) for m in config.maquinas}
        self.telemetria = ClienteTelemetria(config.telemetria_url)
        self.barramento = Barramento()
        self.orquestrador = Orquestrador(config, self.agentes, self.telemetria, self.barramento)
        self.experimento_exibido = None
        self.monitor = Monitor(config, self.agentes, self.telemetria, self.barramento,
                               lambda: self.orquestrador.parametros.get("experimento") or self.experimento_exibido)

    def estado(self):
        return {
            "config": {"modelo": self.config.modelo, "camadas": self.config.camadas,
                       "telemetria": self.config.telemetria_url,
                       "maquinas": [{"nome": m.nome, "papeis": list(m.papeis), "num_blocks": m.num_blocks,
                                     "interface": m.interface} for m in self.config.maquinas]},
            "maquinas": self.barramento.ultimo("maquinas"),
            "experimento": self.orquestrador.instantaneo(),
        }

    def validar_specs(self):
        resultado = []
        for m in self.config.maquinas:
            try:
                r = self.agentes[m.nome].specs(m.interface, m.tdp, m.sem_rapl, m.papeis)
                s = r.get("specs") or {}
                resultado.append({"nome": m.nome, "ok": r["ok"], "problemas": r["problemas"],
                                  "resumo": {k: s.get(k) for k in ("cpu_modelo", "tdp_w", "ram_total_gb",
                                                                   "metodo_cpu", "metodo_ram", "rede_ip")},
                                  "avisos": s.get("avisos", [])})
            except (Indisponivel, ErroRemoto) as e:
                resultado.append({"nome": m.nome, "ok": False, "problemas": [f"agente indisponível: {e}"],
                                  "resumo": {}, "avisos": []})
        self.barramento.publicar("specs", resultado)
        return resultado

    def parar_tudo(self):
        resultado = {}
        for m in self.config.maquinas:
            for papel in ("cliente", "centralizado", "servidor"):
                if papel in m.papeis:
                    try:
                        self.agentes[m.nome].parar(papel)
                        resultado[f"{m.nome}/{papel}"] = "parando"
                    except (Indisponivel, ErroRemoto) as e:
                        resultado[f"{m.nome}/{papel}"] = f"erro: {e}"
        self.orquestrador.cancelar()
        return resultado


class ManipuladorPainel(ManipuladorBase):
    painel: Painel = None
    ROTAS = [
        ("GET", r"/", "rota_indice", True),
        ("GET", r"/login", "rota_login", False),
        ("GET", r"/static/(?P<nome>[A-Za-z0-9_.-]+)", "rota_estatico", True),
        ("GET", r"/api/estado", "rota_estado", True),
        ("GET", r"/api/eventos", "rota_eventos", True),
        ("GET", r"/api/relatorio", "rota_relatorio", True),
        ("GET", r"/api/logs", "rota_logs", True),
        ("POST", r"/api/specs", "rota_specs", True),
        ("POST", r"/api/experimentos", "rota_experimento", True),
        ("POST", r"/api/experimentos/cancelar", "rota_cancelar", True),
        ("POST", r"/api/parar-tudo", "rota_parar_tudo", True),
    ]

    def rota_indice(self, q):
        self.arquivo(ESTATICOS / "index.html")

    def rota_login(self, q):
        token = q.get("token", "")
        if self.token and hmac.compare_digest(token.encode(), self.token.encode()):
            return self.redirecionar("/", f"{COOKIE_TOKEN}={token}; HttpOnly; SameSite=Strict; Path=/")
        raise ErroHTTP(401, "Token inválido")

    def rota_estatico(self, q, nome):
        self.arquivo(ESTATICOS / nome)

    def rota_estado(self, q):
        self.json(200, self.painel.estado())

    def rota_eventos(self, q):
        fila = self.painel.barramento.assinar()
        self.sse(fila, lambda: self.painel.barramento.cancelar(fila))

    def rota_relatorio(self, q):
        exp = q.get("experimento")
        if not exp:
            raise ErroHTTP(400, "Informe ?experimento=")
        self.painel.experimento_exibido = exp
        try:
            rel = resumir_relatorio(self.painel.telemetria.relatorio(exp))
        except Indisponivel as e:
            raise ErroHTTP(503, f"Telemetria indisponível: {e}")
        self.painel.barramento.publicar("relatorio", rel)
        self.json(200, rel)

    def rota_logs(self, q):
        agente = self.painel.agentes.get(q.get("maquina", ""))
        if not agente:
            raise ErroHTTP(404, "Máquina desconhecida")
        try:
            linhas = agente.logs(q.get("papel", ""), 0)
        except ErroRemoto as e:
            raise ErroHTTP(e.codigo, str(e))
        except Indisponivel as e:
            raise ErroHTTP(503, f"Agente indisponível: {e}")
        self.json(200, {"linhas": [l["linha"] for l in linhas]})

    def rota_specs(self, q):
        self.json(200, self.painel.validar_specs())

    def rota_experimento(self, q):
        corpo = self.corpo_json()
        try:
            self.json(200, self.painel.orquestrador.iniciar(corpo.get("cenario"), corpo.get("parametros") or {}))
        except ParametrosInvalidos as e:
            raise ErroHTTP(422, str(e))

    def rota_cancelar(self, q):
        self.json(200, self.painel.orquestrador.cancelar())

    def rota_parar_tudo(self, q):
        self.json(200, self.painel.parar_tudo())


def main():
    p = argparse.ArgumentParser(description="Painel de controle da bancada")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--porta", type=int, default=8080)
    p.add_argument("--config", type=Path, default=None, help="Inventário (padrão: config/maquinas.json)")
    a = p.parse_args()
    token = os.environ.get("BANCADA_TOKEN") or None
    exigir_token_se_exposto(a.host, token, "Painel")
    try:
        config = carregar(a.config) if a.config else carregar()
    except ConfiguracaoInvalida as e:
        raise SystemExit(f"❌ Inventário de máquinas: {e}")

    painel = Painel(config, token)
    painel.monitor.start()
    # o token sempre autentica o painel junto aos agentes; o navegador só precisa dele se o painel estiver exposto
    token_navegador = None if endereco_local(a.host) else token
    servidor = criar_servidor(a.host, a.porta, ManipuladorPainel, token_navegador, painel=painel)
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=servidor.shutdown, daemon=True).start())
    print(f"[painel] http://{a.host}:{a.porta}  |  telemetria: {config.telemetria_url}  |  "
          f"{len(config.maquinas)} máquinas", flush=True)
    if token_navegador:
        print(f"[painel] Acesse primeiro http://{a.host}:{a.porta}/login?token=<BANCADA_TOKEN>", flush=True)
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        painel.monitor.parar()
        servidor.server_close()


if __name__ == "__main__":
    main()
