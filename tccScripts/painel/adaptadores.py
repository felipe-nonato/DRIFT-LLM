"""Adapters: traduzem as APIs HTTP do agente e da telemetria em chamadas Python."""

import json
import urllib.error
import urllib.parse
import urllib.request


class Indisponivel(Exception):
    """O serviço remoto não respondeu."""


class ErroRemoto(Exception):
    """O serviço respondeu com erro."""

    def __init__(self, codigo, mensagem):
        super().__init__(mensagem)
        self.codigo = codigo


class ClienteHTTP:
    def __init__(self, base, token=None, timeout=5):
        self.base = base.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _req(self, metodo, caminho, corpo=None, consulta=None, timeout=None):
        url = self.base + caminho
        if consulta:
            url += "?" + urllib.parse.urlencode({k: v for k, v in consulta.items() if v not in (None, "", False)})
        dados = json.dumps(corpo).encode() if corpo is not None else (b"" if metodo == "POST" else None)
        req = urllib.request.Request(url, data=dados, method=metodo)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as r:
                return json.loads(r.read().decode("utf-8") or "null")
        except urllib.error.HTTPError as e:
            try:
                msg = json.loads(e.read().decode("utf-8")).get("erro", str(e))
            except Exception:
                msg = str(e)
            raise ErroRemoto(e.code, msg) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            raise Indisponivel(f"{self.base}: {getattr(e, 'reason', e)}") from None


class ClienteAgente(ClienteHTTP):
    def saude(self):
        return self._req("GET", "/saude", timeout=3)

    def specs(self, interface, tdp=None, sem_rapl=False, papeis=()):
        return self._req("GET", "/specs", consulta={"interface": interface, "tdp": tdp,
                                                    "sem_rapl": "1" if sem_rapl else None,
                                                    "papeis": ",".join(papeis)}, timeout=30)

    def processos(self):
        return self._req("GET", "/processos", timeout=3)

    def iniciar(self, papel, args):
        return self._req("POST", f"/processos/{papel}/iniciar", corpo={"args": args})

    def parar(self, papel):
        return self._req("POST", f"/processos/{papel}/parar", corpo={})

    def logs(self, papel, desde=0):
        return self._req("GET", f"/processos/{papel}/logs", consulta={"desde": desde}, timeout=3)["linhas"]


class ClienteTelemetria(ClienteHTTP):
    def tempo(self):
        return self._req("GET", "/tempo", timeout=3)["t"]

    def sessoes(self, experimento):
        return self._req("GET", "/sessoes", consulta={"experimento": experimento})["sessoes"]

    def relatorio(self, experimento):
        return self._req("GET", "/relatorio", consulta={"experimento": experimento}, timeout=60)

    def multiaddr_bootstrap(self, experimento):
        """Multiaddr publicado pelo próprio nó bootstrap (fonte única do endereço)."""
        candidatos = [s for s in self.sessoes(experimento)
                      if s["papel"] == "servidor" and s.get("multiaddr") and s.get("fim") is None
                      and s.get("extras", {}).get("bootstrap")]
        return candidatos[-1]["multiaddr"] if candidatos else None
