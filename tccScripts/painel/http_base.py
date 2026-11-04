"""Servidor HTTP mínimo (biblioteca padrão) com roteamento declarativo, JSON, SSE e token.

Template Method: subclasses declaram ROTAS = [(método, regex, nome_do_método, exige_token)]
e implementam os métodos; o despacho, a autenticação e o tratamento de erros ficam aqui.
"""

import hmac
import http.server
import ipaddress
import json
import mimetypes
import queue
import re
from http.cookies import SimpleCookie
from pathlib import Path
from urllib.parse import parse_qs, urlparse

COOKIE_TOKEN = "bancada_token"


class ErroHTTP(Exception):
    def __init__(self, codigo, mensagem):
        super().__init__(mensagem)
        self.codigo = codigo
        self.mensagem = mensagem


def endereco_local(host):
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


def exigir_token_se_exposto(host, token, nome):
    """Recusa expor um serviço que inicia processos sem autenticação."""
    if not token and not endereco_local(host):
        raise SystemExit(
            f"❌ {nome}: escutar em {host} sem token permitiria que qualquer máquina da rede inicie "
            "processos. Defina a variável BANCADA_TOKEN (a mesma em todas as máquinas) ou use --host 127.0.0.1."
        )


class ManipuladorBase(http.server.BaseHTTPRequestHandler):
    ROTAS = []          # [(metodo, regex, nome_metodo, exige_token)]
    token = None        # definido pela fábrica do servidor
    protocol_version = "HTTP/1.1"

    # ------------------------------------------------------------ respostas
    def json(self, codigo, corpo):
        dados = json.dumps(corpo, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(dados)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(dados)

    def arquivo(self, caminho: Path):
        if not caminho.is_file():
            raise ErroHTTP(404, "Arquivo inexistente")
        dados = caminho.read_bytes()
        tipo = mimetypes.guess_type(caminho.name)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", f"{tipo}; charset=utf-8" if tipo.startswith("text/") or tipo.endswith("javascript") else tipo)
        self.send_header("Content-Length", str(len(dados)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(dados)

    def redirecionar(self, destino, cookie=None):
        self.send_response(303)
        self.send_header("Location", destino)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def sse(self, fila: "queue.Queue", ao_encerrar):
        """Transmite eventos {'tipo','dados'} da fila até o cliente desconectar."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.close_connection = True
        try:
            while True:
                try:
                    evento = fila.get(timeout=15)
                    bloco = f"event: {evento['tipo']}\ndata: {json.dumps(evento['dados'], ensure_ascii=False, default=str)}\n\n"
                except queue.Empty:
                    bloco = ": ping\n\n"
                self.wfile.write(bloco.encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            ao_encerrar()

    # ------------------------------------------------------------ entrada
    def corpo_json(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        if n > 1_000_000:
            raise ErroHTTP(413, "Corpo grande demais")
        try:
            dados = json.loads(self.rfile.read(n).decode("utf-8") or "{}")
        except json.JSONDecodeError as e:
            raise ErroHTTP(400, f"JSON inválido: {e}")
        if not isinstance(dados, dict):
            raise ErroHTTP(400, "O corpo deve ser um objeto JSON")
        return dados

    def _token_recebido(self):
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:].strip()
        cookie = SimpleCookie(self.headers.get("Cookie", ""))
        return cookie[COOKIE_TOKEN].value if COOKIE_TOKEN in cookie else ""

    def autenticado(self):
        if not self.token:
            return True
        return hmac.compare_digest(self._token_recebido().encode(), self.token.encode())

    # ------------------------------------------------------------ despacho
    def _despachar(self, metodo):
        url = urlparse(self.path)
        consulta = {k: v[0] for k, v in parse_qs(url.query).items()}
        try:
            for m, padrao, nome, exige_token in self.ROTAS:
                if m != metodo:
                    continue
                casamento = re.fullmatch(padrao, url.path)
                if not casamento:
                    continue
                if exige_token and not self.autenticado():
                    raise ErroHTTP(401, "Token ausente ou inválido")
                return getattr(self, nome)(consulta, **casamento.groupdict())
            raise ErroHTTP(404, f"Rota inexistente: {metodo} {url.path}")
        except ErroHTTP as e:
            self.json(e.codigo, {"erro": e.mensagem})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:  # erro inesperado: responde e registra
            self.log_error("%s: %s", type(e).__name__, e)
            try:
                self.json(500, {"erro": f"{type(e).__name__}: {e}"})
            except Exception:
                pass

    def do_GET(self):
        self._despachar("GET")

    def do_POST(self):
        self._despachar("POST")

    def log_message(self, formato, *args):  # silencia o log de acesso
        return


def criar_servidor(host, porta, manipulador, token, **atributos):
    """Fábrica: injeta token e dependências na classe do manipulador."""
    classe = type(manipulador.__name__, (manipulador,), {"token": token, **atributos})
    servidor = http.server.ThreadingHTTPServer((host, porta), classe)
    servidor.daemon_threads = True
    return servidor
