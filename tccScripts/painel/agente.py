"""Agente local: roda em CADA máquina do experimento e expõe seus processos por HTTP.

Facade sobre GerenciadorProcesso: o painel nunca executa comandos arbitrários — só pode
iniciar/parar os papéis registrados em processos.PAPEIS, com argumentos de uma lista fechada.

Uso (na raiz do projeto):
    export BANCADA_TOKEN=<segredo compartilhado>
    python -m painel.agente                  # escuta no IP do Tailscale, porta 7000
    python -m painel.agente --interface enp3s0   # escuta no IP da rede local
    python -m painel.agente --host 127.0.0.1 # só local (token opcional)

Rotas (todas exigem o token):
    GET  /saude
    GET  /specs?interface=&tdp=&sem_rapl=1&permitir_virtualizado=1&papeis=servidor,cliente
    GET  /processos
    GET  /processos/<papel>/logs?desde=N
    POST /processos/<papel>/iniciar   {"args": {...}}
    POST /processos/<papel>/parar
"""

import argparse
import os
import platform
import signal
import socket
import sys
import threading

from bancada import specs as specs_mod

from .http_base import ErroHTTP, ManipuladorBase, criar_servidor, exigir_token_se_exposto
from .processos import PAPEIS, ErroProcesso, GerenciadorProcesso


class Agente:
    def __init__(self, papeis=PAPEIS):
        self.processos = {nome: GerenciadorProcesso(p) for nome, p in papeis.items()}
        self.hostname = socket.gethostname()
        self.maquina_id = specs_mod.maquina_id()

    def processo(self, papel):
        if papel not in self.processos:
            raise ErroHTTP(404, f"Papel desconhecido: {papel}")
        return self.processos[papel]

    def saude(self):
        return {"hostname": self.hostname, "maquina_id": self.maquina_id, "papeis": list(self.processos)}

    def specs(self, interface, tdp=None, sem_rapl=False, permitir_virtualizado=False, papeis=()):
        """Specs de hardware + dependências Python dos papéis que esta máquina vai exercer."""
        problemas = []
        for papel in papeis:
            if papel in self.processos:
                faltando = self.processos[papel].papel.dependencias_faltando()
                if faltando:
                    problemas.append(f"Papel '{papel}': módulos ausentes no Python do agente "
                                     f"({sys.executable}, {platform.python_version()}): {', '.join(faltando)}")
        try:
            s = specs_mod.coletar(tdp=tdp, interface=interface, sem_rapl=sem_rapl,
                                  permitir_virtualizado=permitir_virtualizado)
        except specs_mod.SpecsIncompletas as e:
            return {"ok": False, "specs": None, "problemas": e.problemas + problemas}
        return {"ok": not problemas, "specs": s, "problemas": problemas}

    def estados(self):
        return {nome: g.instantaneo() for nome, g in self.processos.items()}

    def encerrar_tudo(self):
        for g in self.processos.values():
            g.parar(tolerancia_s=20)
        for g in self.processos.values():
            g.aguardar_fim(40)


def _bool(valor):
    return str(valor).lower() in ("1", "true", "sim", "yes")


class ManipuladorAgente(ManipuladorBase):
    agente: Agente = None
    ROTAS = [
        ("GET", r"/saude", "rota_saude", True),
        ("GET", r"/specs", "rota_specs", True),
        ("GET", r"/processos", "rota_processos", True),
        ("GET", r"/processos/(?P<papel>[a-z_]+)/logs", "rota_logs", True),
        ("POST", r"/processos/(?P<papel>[a-z_]+)/iniciar", "rota_iniciar", True),
        ("POST", r"/processos/(?P<papel>[a-z_]+)/parar", "rota_parar", True),
    ]

    def rota_saude(self, q):
        self.json(200, self.agente.saude())

    def rota_specs(self, q):
        tdp = float(q["tdp"]) if q.get("tdp") else None
        self.json(200, self.agente.specs(q.get("interface", "tailscale0"), tdp, _bool(q.get("sem_rapl")),
                                         _bool(q.get("permitir_virtualizado")),
                                         [p for p in q.get("papeis", "").split(",") if p]))

    def rota_processos(self, q):
        self.json(200, self.agente.estados())

    def rota_logs(self, q, papel):
        desde = int(q.get("desde", 0) or 0)
        self.json(200, {"papel": papel, "linhas": self.agente.processo(papel).logs(desde)})

    def rota_iniciar(self, q, papel):
        corpo = self.corpo_json()
        try:
            self.json(200, self.agente.processo(papel).iniciar(corpo.get("args") or {}))
        except ErroProcesso as e:
            raise ErroHTTP(409, str(e))

    def rota_parar(self, q, papel):
        self.json(200, self.agente.processo(papel).parar())


def host_da_interface(interface):
    """IP IPv4 da interface (ex.: enp3s0 na rede local, tailscale0 na VPN)."""
    nome, ip, disponiveis = specs_mod.interface_rede(interface)
    if not nome or not ip:
        raise SystemExit(f"❌ Interface '{interface}' sem IPv4. Disponíveis: {', '.join(disponiveis)}")
    return ip


def main():
    p = argparse.ArgumentParser(description="Agente local da bancada")
    p.add_argument("--host", default=None, help="Endereço de escuta explícito (tem prioridade sobre --interface)")
    p.add_argument("--interface", default="tailscale0",
                   help="Escuta no IP desta interface (padrão: tailscale0; na rede local, ex.: enp3s0)")
    p.add_argument("--porta", type=int, default=7000)
    a = p.parse_args()
    host = a.host or host_da_interface(a.interface)
    token = os.environ.get("BANCADA_TOKEN") or None
    exigir_token_se_exposto(host, token, "Agente")

    agente = Agente()
    servidor = criar_servidor(host, a.porta, ManipuladorAgente, token, agente=agente)

    def encerrar(*_):
        threading.Thread(target=servidor.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, encerrar)
    signal.signal(signal.SIGHUP, encerrar)  # terminal fechado: também encerra os processos filhos
    print(f"[agente] {agente.hostname} ouvindo em http://{host}:{a.porta} "
          f"({'com' if token else 'sem'} token)", flush=True)
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("[agente] Encerrando processos locais...", flush=True)
        agente.encerrar_tudo()
        servidor.server_close()


if __name__ == "__main__":
    main()
