"""Página local da rede P2P: o MESMO programa em todos os PCs, com dois modos.

  Nó central (o PC onde a rede começa)
      Um botão liga a telemetria e o nó bootstrap; a página mostra o ID do enxame (multiaddr)
      com botão "Copiar", os PCs que entraram, o botão de rodar as consultas e os resultados
      de consumo (energia, CO₂, água) calculados pela telemetria.

  Entrar na rede (qualquer outro PC)
      Cola-se o ID e o PC entra sozinho como servidor de blocos: a página descobre a API de
      telemetria, o experimento, o modelo, a precisão, a interface de rede e quantos blocos
      cabem na RAM — sem config/maquinas.json, sem token, sem formulário.

Uso (na raiz do projeto):
    ./rede.sh                       # abre http://127.0.0.1:8090
    python -m painel.rede --porta 8090
"""

import argparse
import hmac
import json
import os
import re
import signal
import socket
import threading
import time
from pathlib import Path

import psutil
import requests

from bancada import blocos as blocos_mod
from bancada import experimento as exp_mod

from .agente import Agente
from .http_base import COOKIE_TOKEN, ErroHTTP, ManipuladorBase, criar_servidor, exigir_token_se_exposto
from .monitor import resumir_relatorio
from .processos import PAPEIS, ErroProcesso

DIR_STATIC = Path(__file__).resolve().parent / "static"

PADRAO_MULTIADDR = re.compile(
    r"^/ip4/(?P<ip>\d{1,3}(?:\.\d{1,3}){3})/tcp/(?P<porta>\d{1,5})/p2p/"
    r"(?P<peer>12D3KooW[1-9A-HJ-NP-Za-km-z]{40,50}|Qm[1-9A-HJ-NP-Za-km-z]{44})$"
)
PADRAO_ROTULO = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
MODELO_PADRAO = "Qwen/Qwen2.5-7B-Instruct"
DTYPE_PADRAO = "bfloat16"
PORTA_TELEMETRIA = 5000
PORTA_P2P = 31337
PAPEIS_LOCAIS = ("telemetria", "servidor", "cliente")


# ---------------------------------------------------------------- utilidades de rede/memória

def analisar_multiaddr(texto):
    m = PADRAO_MULTIADDR.match((texto or "").strip())
    if not m:
        raise ErroHTTP(422, "ID inválido. Formato esperado: /ip4/IP/tcp/PORTA/p2p/PEER_ID")
    return m.group("ip"), int(m.group("porta")), m.group("peer")


def interfaces_ipv4():
    """[(nome, ip)] das interfaces com IPv4, sem loopback."""
    saida = []
    for nome, enderecos in psutil.net_if_addrs().items():
        for a in enderecos:
            if a.family == socket.AF_INET and not a.address.startswith("127."):
                saida.append({"nome": nome, "ip": a.address})
    return saida


def interface_para(ip_destino):
    """Interface e IP locais que o sistema usaria para alcançar ip_destino (sem enviar pacotes)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect((ip_destino, 9))
            ip_local = s.getsockname()[0]
        except OSError:
            return None, None
    for i in interfaces_ipv4():
        if i["ip"] == ip_local:
            return i["nome"], ip_local
    return None, ip_local


def ip_da_interface(nome):
    return next((i["ip"] for i in interfaces_ipv4() if i["nome"] == nome), None)


def nome_do_no():
    return re.sub(r"[^A-Za-z0-9_.-]", "-", socket.gethostname())[:40] or "no"


def ram_gb():
    return psutil.virtual_memory().total / 1024**3


_cache_bloco = {}
_cache_cliente = {}
# O servidor HTTP atende cada requisição numa thread. A primeira estimativa importa torch e
# transformers (vários segundos); duas requisições simultâneas (ex.: /api/sugestao e /api/central)
# importando ao mesmo tempo podem falhar no carregamento preguiçoso do transformers. Serializa.
_lock_modelo = threading.Lock()


def gb_por_bloco(modelo, torch_dtype):
    """(GiB por bloco, total de camadas) — instancia a arquitetura em 'meta' (só baixa o config)."""
    chave = (modelo, torch_dtype)
    with _lock_modelo:
        if chave not in _cache_bloco:
            try:
                _cache_bloco[chave] = exp_mod.estimar_memoria_gb(modelo, torch_dtype, 1)
            except BaseException as e:  # estimar_memoria_gb usa SystemExit para erros de importação
                raise ErroHTTP(500, f"Não foi possível estimar a memória do modelo: {e}")
        return _cache_bloco[chave]


def gib_cliente(modelo, torch_dtype):
    chave = (modelo, torch_dtype)
    with _lock_modelo:
        if chave not in _cache_cliente:
            try:
                _cache_cliente[chave] = blocos_mod.memoria_cliente_gib(modelo, torch_dtype)
            except Exception as e:
                raise ErroHTTP(500, f"Não foi possível estimar a memória do cliente: {type(e).__name__}: {e}")
        return _cache_cliente[chave]


def aquecer_estimativas():
    """Importa torch/transformers e calcula as estimativas padrão logo ao abrir a página."""
    try:
        gb_por_bloco(MODELO_PADRAO, DTYPE_PADRAO)
        gib_cliente(MODELO_PADRAO, DTYPE_PADRAO)
        print("[rede] Estimativas de memória do modelo prontas.", flush=True)
    except ErroHTTP as e:
        print(f"[rede] ⚠️  {e}", flush=True)


def blocos_que_cabem(modelo, torch_dtype, reservar_cliente):
    """Capacidade pela RAM DISPONÍVEL agora (ver bancada/blocos.py): (dict, GiB/bloco, camadas)."""
    por_bloco, camadas = gb_por_bloco(modelo, torch_dtype)
    reserva = gib_cliente(modelo, torch_dtype) if reservar_cliente else 0.0
    total, disponivel = blocos_mod.memoria_ram()
    cap = blocos_mod.capacidade(por_bloco, total, disponivel, reserva)
    cap["blocos"] = min(cap["blocos"], camadas)
    return cap, por_bloco, camadas


def blocos_pedidos(corpo, cap, camadas):
    """Número de blocos escolhido à mão (None = automático) e se pode passar da capacidade calculada."""
    bruto = corpo.get("num_blocks")
    if bruto in (None, "", 0, "0"):
        return None, False
    try:
        pedido = int(bruto)
    except (TypeError, ValueError):
        raise ErroHTTP(422, "Blocos neste PC: use um número inteiro (ou deixe vazio para automático).")
    if not 1 <= pedido <= camadas:
        raise ErroHTTP(422, f"Blocos neste PC: entre 1 e {camadas} (o modelo tem {camadas} camadas).")
    forcar = bool(corpo.get("forcar_blocos"))
    if pedido > cap["blocos"] and not forcar:
        raise ErroHTTP(422, f"Você pediu {pedido} blocos, mas este PC comporta {blocos_mod.descrever(cap)}. "
                            "Acima disso o sistema pagina os pesos do disco e o tempo/energia por token ficam "
                            "inflados. Use um número menor ou marque “permitir acima da capacidade”.")
    return pedido, forcar


def _sem_capacidade(cap):
    return ErroHTTP(422, "A RAM disponível deste PC não comporta nenhum bloco do modelo. "
                         f"{blocos_mod.descrever(cap)}. Feche programas (navegador, IDE) e tente de novo.")


def telemetria_no_ar(api):
    try:
        return requests.get(f"{api}/tempo", timeout=2).ok
    except requests.RequestException:
        return False


# ---------------------------------------------------------------- estado da página

class Rede:
    """Facade: guarda o modo deste PC e opera os processos locais (telemetria, servidor, cliente)."""

    def __init__(self):
        self.agente = Agente({k: PAPEIS[k] for k in PAPEIS_LOCAIS})
        self._lock = threading.Lock()
        self.modo = None   # {"tipo": "central"|"membro", "api", "experimento", "modelo", "torch_dtype", ...}

    def processo(self, papel):
        return self.agente.processo(papel)

    def multiaddr(self):
        if not self.modo:
            return None
        if self.modo["tipo"] == "membro":
            return self.modo["multiaddr_central"]
        return self.processo("servidor").instantaneo()["artefatos"].get("multiaddr")

    def estado(self):
        procs = self.agente.estados()
        return {
            "hostname": socket.gethostname(), "no_id": nome_do_no(), "ram_gb": round(ram_gb(), 1),
            "interfaces": interfaces_ipv4(), "modo": self.modo, "multiaddr": self.multiaddr(),
            "processos": procs,
        }

    # -------------------------------------------------- nó central
    def iniciar_central(self, corpo):
        experimento = (corpo.get("experimento") or "").strip()
        if not PADRAO_ROTULO.match(experimento):
            raise ErroHTTP(422, "Rótulo do experimento: use letras, números, ponto, hífen ou sublinhado.")
        interface = corpo.get("interface") or ""
        ip = ip_da_interface(interface)
        if not ip:
            raise ErroHTTP(422, f"Interface '{interface}' sem IPv4.")
        modelo, dtype = MODELO_PADRAO, corpo.get("torch_dtype") or DTYPE_PADRAO
        if any(p["ativo"] for p in self.agente.estados().values()):
            raise ErroHTTP(409, "Já há processos rodando neste PC. Desligue antes de iniciar de novo.")
        try:
            previstas = int(corpo.get("maquinas_previstas") or 0)
        except (TypeError, ValueError):
            raise ErroHTTP(422, "PCs previstos deve ser um número inteiro.")
        if not 0 <= previstas <= 64:
            raise ErroHTTP(422, "PCs previstos: entre 1 e 64 (ou vazio).")
        # o cliente roda neste PC (botão "Rodar consultas"): a capacidade já reserva a memória dele
        cap, _, camadas = blocos_que_cabem(modelo, dtype, reservar_cliente=True)
        pedido, forcar = blocos_pedidos(corpo, cap, camadas)
        if pedido is None and cap["blocos"] < 1:
            raise _sem_capacidade(cap)
        # manual: exatamente o pedido (sem cota); automático: capacidade limitada à cota balanceada
        num_blocks = pedido or blocos_mod.proximo_intervalo(camadas, [], cap["blocos"], previstas)[1]
        block_indices = f"0:{num_blocks}"
        plano = {"block_indices": block_indices, "capacidade": cap, "maquinas_previstas": previstas or None,
                 "reserva_cliente": True, "modo": "manual" if pedido else "automatico",
                 "pedido_manual": pedido, "acima_da_capacidade": bool(pedido and pedido > cap["blocos"])}

        api = f"http://{ip}:{PORTA_TELEMETRIA}"
        if not telemetria_no_ar(f"http://127.0.0.1:{PORTA_TELEMETRIA}"):
            self.processo("telemetria").iniciar({})
            limite = time.time() + 30
            while not telemetria_no_ar(f"http://127.0.0.1:{PORTA_TELEMETRIA}"):
                if not self.processo("telemetria").instantaneo()["ativo"] or time.time() > limite:
                    raise ErroHTTP(500, "A telemetria não subiu (porta 5000 ocupada?). Veja o log 'telemetria'.")
                time.sleep(0.5)

        args = {"api": api, "experimento": experimento, "no_id": nome_do_no(), "modelo": modelo,
                "torch_dtype": dtype, "interface": interface, "block_indices": block_indices, "bootstrap": True,
                "porta": PORTA_P2P, "sem_rapl": bool(corpo.get("sem_rapl")), "plano": json.dumps(plano)}
        try:
            self.processo("servidor").iniciar(args)
        except ErroProcesso as e:
            raise ErroHTTP(409, str(e))
        with self._lock:
            self.modo = {"tipo": "central", "api": api, "experimento": experimento, "modelo": modelo,
                         "torch_dtype": dtype, "interface": interface, "ip": ip, "num_blocks": num_blocks,
                         "block_indices": block_indices, "camadas": camadas, "capacidade": cap}
        return self.estado()

    def rodar_consultas(self, corpo):
        if not self.modo:
            raise ErroHTTP(409, "Inicie ou entre numa rede primeiro.")
        multiaddr = self.multiaddr()
        if not multiaddr:
            raise ErroHTTP(409, "O nó central ainda não anunciou o ID: aguarde a linha de base e o carregamento.")
        args = {"api": self.modo["api"], "experimento": self.modo["experimento"], "no_id": nome_do_no(),
                "modelo": self.modo["modelo"], "torch_dtype": self.modo["torch_dtype"],
                "interface": self.modo["interface"], "initial_peers": multiaddr,
                "repeticoes": int(corpo.get("repeticoes") or 3), "max_new_tokens": int(corpo.get("max_new_tokens") or 128),
                "sem_rapl": bool(corpo.get("sem_rapl"))}
        try:
            return self.processo("cliente").iniciar(args)
        except ErroProcesso as e:
            raise ErroHTTP(409, str(e))

    # -------------------------------------------------- entrar como membro
    def entrar(self, corpo):
        multiaddr = (corpo.get("multiaddr") or "").strip()
        ip, porta, peer = analisar_multiaddr(multiaddr)
        if any(p["ativo"] for p in self.agente.estados().values()):
            raise ErroHTTP(409, "Este PC já está numa rede. Saia antes de entrar em outra.")
        interface, ip_local = interface_para(ip)
        if not interface:
            raise ErroHTTP(422, f"Este PC não alcança {ip}. Ele está na mesma rede local / Tailscale que o nó central?")
        if ip_local == ip:
            raise ErroHTTP(422, "Esse ID é deste próprio PC: aqui ele já é o nó central.")

        api = (corpo.get("api") or f"http://{ip}:{PORTA_TELEMETRIA}").rstrip("/")
        try:
            r = requests.get(f"{api}/sessoes", timeout=5)
        except requests.RequestException as e:
            raise ErroHTTP(502, f"Não consegui falar com a telemetria do nó central em {api} ({type(e).__name__}). "
                                "Libere a porta 5000 no firewall do nó central.")
        if not r.ok:
            try:
                detalhe = r.json().get("erro") or r.text
            except ValueError:
                detalhe = r.text
            raise ErroHTTP(502, f"A telemetria do nó central respondeu com erro HTTP {r.status_code}: {detalhe[:300]}. "
                                "Reinicie o ./rede.sh no nó central.")
        sessoes = [s for s in r.json().get("sessoes", []) if peer in (s.get("multiaddr") or "")]
        abertas = [s for s in sessoes if not s.get("fim")]
        if not abertas:
            raise ErroHTTP(404, "O nó central com esse ID não está no ar (ou já foi desligado). Copie o ID de novo.")
        central = abertas[-1]
        extras = central.get("extras") or {}
        modelo = extras.get("modelo") or MODELO_PADRAO
        dtype = extras.get("torch_dtype") or DTYPE_PADRAO
        cap, _, camadas = blocos_que_cabem(modelo, dtype, reservar_cliente=False)
        pedido, forcar = blocos_pedidos(corpo, cap, camadas)
        if pedido is None and cap["blocos"] < 1:
            raise _sem_capacidade(cap)

        # próximo intervalo de camadas ainda não coberto pelos servidores já na rede
        try:
            r = requests.get(f"{api}/sessoes", params={"experimento": central["experimento"]}, timeout=5)
            r.raise_for_status()
            sessoes_exp = r.json().get("sessoes", [])
        except requests.RequestException as e:
            raise ErroHTTP(502, f"Não consegui ler os servidores da rede em {api} ({type(e).__name__}).")
        fixos, sem_intervalo = blocos_mod.ocupados(sessoes_exp)
        if sem_intervalo:
            raise ErroHTTP(409, f"Servidor(es) sem intervalo fixo de blocos na rede ({', '.join(sem_intervalo)}): "
                                "o Drift escolhe e rebalanceia os blocos deles, então não há como planejar este "
                                "PC. Recrie a rede com a versão atual da página.")
        previstas = ((extras.get("plano_blocos") or {}).get("maquinas_previstas")) or None
        if pedido:
            livres = blocos_mod.lacunas(camadas, fixos)
            if not livres:
                raise ErroHTTP(409, f"A rede já cobre as {camadas} camadas do modelo: não há trecho livre para este PC.")
            intervalo = blocos_mod.intervalo_manual(camadas, fixos, pedido)
            if intervalo is None:
                maior = max(b - a for a, b in livres)
                raise ErroHTTP(409, f"Nenhum trecho livre comporta {pedido} blocos seguidos sem sobrepor outro "
                                    f"servidor. Trechos livres: {', '.join(f'{a}:{b}' for a, b in livres)}. "
                                    f"Peça no máximo {maior} ou deixe vazio para automático.")
        else:
            intervalo = blocos_mod.proximo_intervalo(camadas, fixos, cap["blocos"], previstas)
        if intervalo is None:
            raise ErroHTTP(409, f"A rede já cobre as {camadas} camadas do modelo. Este PC entraria como réplica, "
                                "criando rotas alternativas que mudam a cadeia entre consultas. Não é necessário.")
        block_indices = f"{intervalo[0]}:{intervalo[1]}"
        num_blocks = intervalo[1] - intervalo[0]
        plano = {"block_indices": block_indices, "capacidade": cap, "maquinas_previstas": previstas,
                 "reserva_cliente": False, "modo": "manual" if pedido else "automatico",
                 "pedido_manual": pedido, "acima_da_capacidade": bool(pedido and pedido > cap["blocos"]),
                 "ocupados_antes": [f"{a}:{b} ({no})" for a, b, no in fixos],
                 "lacunas_antes": [f"{a}:{b}" for a, b in blocos_mod.lacunas(camadas, fixos)]}

        args = {"api": api, "experimento": central["experimento"], "no_id": nome_do_no(), "modelo": modelo,
                "torch_dtype": dtype, "interface": interface, "block_indices": block_indices,
                "initial_peers": multiaddr, "porta": PORTA_P2P, "sem_rapl": bool(corpo.get("sem_rapl")),
                "plano": json.dumps(plano)}
        try:
            self.processo("servidor").iniciar(args)
        except ErroProcesso as e:
            raise ErroHTTP(409, str(e))
        with self._lock:
            self.modo = {"tipo": "membro", "api": api, "experimento": central["experimento"], "modelo": modelo,
                         "torch_dtype": dtype, "interface": interface, "ip": ip_local, "num_blocks": num_blocks,
                         "block_indices": block_indices, "camadas": camadas, "capacidade": cap,
                         "multiaddr_central": multiaddr, "central": central.get("hostname")}
        return self.estado()

    # -------------------------------------------------- comum
    def encerrar_em_ordem(self, aguardar_s=45):
        """Para cliente e servidor e só depois a telemetria (eles registram o encerramento nela)."""
        for papel in ("cliente", "servidor"):
            self.processo(papel).parar(tolerancia_s=20)
        for papel in ("cliente", "servidor"):
            self.processo(papel).aguardar_fim(aguardar_s)
        self.processo("telemetria").parar(tolerancia_s=10)
        self.processo("telemetria").aguardar_fim(20)

    def desligar(self):
        threading.Thread(target=self.encerrar_em_ordem, daemon=True).start()
        with self._lock:
            self.modo = None
        return self.estado()

    def membros(self):
        """Sessões abertas do experimento atual (quem está na rede agora)."""
        if not self.modo:
            return []
        r = requests.get(f"{self.modo['api']}/sessoes", params={"experimento": self.modo["experimento"]}, timeout=5)
        r.raise_for_status()
        saida = []
        for s in r.json().get("sessoes", []):
            if s.get("fim"):
                continue
            sp, ex = s.get("specs") or {}, s.get("extras") or {}
            saida.append({"no_id": s["no_id"], "papel": s["papel"], "hostname": s.get("hostname"),
                          "ip": sp.get("rede_ip"), "cpu": sp.get("cpu_modelo"), "ram_gb": sp.get("ram_total_gb"),
                          "metodo": sp.get("metodo_cpu"), "blocos": ex.get("num_blocos"),
                          "intervalo": ex.get("block_indices"),
                          "bootstrap": bool(ex.get("bootstrap")), "inicio": s.get("inicio")})
        return saida

    def relatorio(self, csv=False):
        if not self.modo:
            raise ErroHTTP(409, "Nenhuma rede ativa neste PC.")
        caminho = "/relatorio.csv" if csv else "/relatorio"
        r = requests.get(f"{self.modo['api']}{caminho}", params={"experimento": self.modo["experimento"]}, timeout=30)
        r.raise_for_status()
        return r.text if csv else resumir_relatorio(r.json())


# ---------------------------------------------------------------- HTTP

class ManipuladorRede(ManipuladorBase):
    rede: Rede = None
    ROTAS = [
        ("GET", r"/", "rota_pagina", False),
        ("GET", r"/login", "rota_login", False),
        ("GET", r"/static/(?P<nome>[A-Za-z0-9_.-]+)", "rota_estatico", False),
        ("GET", r"/api/estado", "rota_estado", True),
        ("GET", r"/api/sugestao", "rota_sugestao", True),
        ("POST", r"/api/central", "rota_central", True),
        ("POST", r"/api/entrar", "rota_entrar", True),
        ("POST", r"/api/consultas", "rota_consultas", True),
        ("POST", r"/api/parar-consultas", "rota_parar_consultas", True),
        ("POST", r"/api/desligar", "rota_desligar", True),
        ("GET", r"/api/membros", "rota_membros", True),
        ("GET", r"/api/relatorio", "rota_relatorio", True),
        ("GET", r"/api/relatorio.csv", "rota_csv", True),
        ("GET", r"/api/logs", "rota_logs", True),
    ]

    def rota_pagina(self, q):
        self.arquivo(DIR_STATIC / "rede.html")

    def rota_login(self, q):
        token = q.get("token", "")
        if self.token and hmac.compare_digest(token.encode(), self.token.encode()):
            return self.redirecionar("/", f"{COOKIE_TOKEN}={token}; HttpOnly; SameSite=Strict; Path=/")
        raise ErroHTTP(401, "Token inválido")

    def rota_estatico(self, q, nome):
        self.arquivo(DIR_STATIC / nome)

    def rota_estado(self, q):
        self.json(200, self.rede.estado())

    def rota_sugestao(self, q):
        """Capacidade deste PC. Nó central reserva RAM para o cliente (roda aqui); membro (?membro=1) não."""
        dtype = q.get("torch_dtype") or DTYPE_PADRAO
        membro = q.get("membro") in ("1", "true")
        cap, por_bloco, camadas = blocos_que_cabem(MODELO_PADRAO, dtype, reservar_cliente=not membro)
        self.json(200, {"num_blocks": cap["blocos"], "gb_por_bloco": round(por_bloco, 3), "camadas": camadas,
                        "ram_gb": round(ram_gb(), 1), "capacidade": cap, "explicacao": blocos_mod.descrever(cap)})

    def rota_central(self, q):
        self.json(200, self.rede.iniciar_central(self.corpo_json()))

    def rota_entrar(self, q):
        self.json(200, self.rede.entrar(self.corpo_json()))

    def rota_consultas(self, q):
        self.json(200, self.rede.rodar_consultas(self.corpo_json()))

    def rota_parar_consultas(self, q):
        self.json(200, self.rede.processo("cliente").parar())

    def rota_desligar(self, q):
        self.json(200, self.rede.desligar())

    def rota_membros(self, q):
        try:
            self.json(200, {"membros": self.rede.membros()})
        except requests.RequestException as e:
            raise ErroHTTP(502, f"Telemetria indisponível: {type(e).__name__}")

    def rota_relatorio(self, q):
        try:
            self.json(200, self.rede.relatorio())
        except requests.RequestException as e:
            raise ErroHTTP(502, f"Telemetria indisponível: {type(e).__name__}")

    def rota_csv(self, q):
        try:
            dados = self.rede.relatorio(csv=True).encode("utf-8")
        except requests.RequestException as e:
            raise ErroHTTP(502, f"Telemetria indisponível: {type(e).__name__}")
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{self.rede.modo["experimento"]}.csv"')
        self.send_header("Content-Length", str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def rota_logs(self, q):
        papel = q.get("papel", "servidor")
        if papel not in PAPEIS_LOCAIS:
            raise ErroHTTP(400, f"Papel deve ser um de {PAPEIS_LOCAIS}")
        self.json(200, {"linhas": self.rede.processo(papel).logs(int(q.get("desde", 0) or 0))})


def main():
    p = argparse.ArgumentParser(description="Página local da rede P2P (nó central ou entrada de novos PCs)")
    p.add_argument("--host", default="127.0.0.1", help="Endereço de escuta (padrão: só local)")
    p.add_argument("--porta", type=int, default=8090)
    a = p.parse_args()
    token = os.environ.get("BANCADA_TOKEN") or None
    exigir_token_se_exposto(a.host, token, "Página da rede")

    # Os filhos são parados com SIGINT; se este processo herdou SIGINT ignorado (ex.: lançado em
    # segundo plano), os filhos herdariam o mesmo e nunca parariam.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    rede = Rede()
    threading.Thread(target=aquecer_estimativas, daemon=True).start()
    servidor = criar_servidor(a.host, a.porta, ManipuladorRede, token, rede=rede)

    def encerrar(*_):
        threading.Thread(target=servidor.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, encerrar)
    signal.signal(signal.SIGHUP, encerrar)
    print(f"[rede] Página em http://{a.host}:{a.porta}  (Ctrl+C desliga tudo deste PC)", flush=True)
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("[rede] Desligando processos deste PC...", flush=True)
        rede.encerrar_em_ordem()
        servidor.server_close()


if __name__ == "__main__":
    main()
