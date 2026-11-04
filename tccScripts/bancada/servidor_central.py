"""
Servidor de inferência CENTRALIZADO convencional: o modelo inteiro em uma máquina, atendendo
clientes por HTTP, como um serviço de LLM hospedado (cenário de comparação com o enxame P2P).

A máquina é medida pelo mesmo pipeline do P2P (SessaoMedida → API de telemetria, cenário
'centralizado', em que PUE e WUE de data center são aplicados sobre a energia de TI).

Escalonamento das requisições (--modo):
  lote    (padrão) lote dinâmico: o trabalhador junta as requisições que chegam em até
          --janela_lote_ms (até --lote_max) e gera todas juntas, com preenchimento à esquerda.
          É o comportamento de servidores de LLM em produção (batching), na forma estática:
          o lote dura o maior orçamento do grupo.
  serial  fila FIFO: uma requisição por vez (servidor de um único "slot").

Rotas:
  GET  /saude       {"pronto", "modelo", "torch_dtype", "modo", "lote_max", "fila"}
  POST /v1/gerar    {"prompt", "max_new_tokens", "forcar_tamanho"} ->
                    {"texto", "tokens_entrada", "tokens_saida", "t_fila_s", "t_geracao_s", "lote"}

Exemplo:
  python -m bancada.servidor_central --api http://127.0.0.1:5000 --experimento central01 \\
      --modelo Qwen/Qwen2.5-0.5B-Instruct --torch_dtype float32 --interface enp3s0
"""

import argparse
import http.server
import json
import queue
import signal
import threading
import time
from dataclasses import dataclass, field

from . import experimento
from . import specs as specs_mod
from .medidor import SessaoMedida

PRONTO = "[CENTRAL] Servidor pronto em"


def parse_args():
    p = argparse.ArgumentParser(description="Servidor de inferência centralizado (HTTP) com medição")
    specs_mod.argumentos_specs(p)
    p.add_argument("--api", required=True, help="API de telemetria")
    p.add_argument("--experimento", required=True)
    p.add_argument("--no_id", default="central")
    p.add_argument("--modelo", default="Qwen/Qwen2.5-7B-Instruct")
    p.add_argument("--torch_dtype", required=True, choices=list(experimento.DTYPES),
                   help="DEVE ser a mesma precisão usada no P2P")
    p.add_argument("--threads", type=int, help="Threads do PyTorch (padrão: todos os núcleos lógicos)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--porta", type=int, default=8000)
    p.add_argument("--modo", choices=("lote", "serial"), default="lote")
    p.add_argument("--lote_max", type=int, default=8)
    p.add_argument("--janela_lote_ms", type=float, default=50.0)
    p.add_argument("--baseline_s", type=float)
    p.add_argument("--forcar_memoria", action="store_true")
    return p.parse_args()


@dataclass
class Pedido:
    prompt: str
    max_new_tokens: int
    forcar_tamanho: bool
    chegada: float = field(default_factory=time.time)
    feito: threading.Event = field(default_factory=threading.Event)
    resultado: dict = None
    erro: str = None


class Motor:
    """Único dono do modelo: só a thread trabalhadora chama generate()."""

    def __init__(self, modelo, tokenizer, modo, lote_max, janela_s):
        self.modelo, self.tokenizer = modelo, tokenizer
        self.modo, self.lote_max, self.janela_s = modo, lote_max, janela_s
        self.fila = queue.Queue()
        self.tokenizer.padding_side = "left"
        if self.tokenizer.pad_token_id is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self._parar = threading.Event()
        self.trabalhador = threading.Thread(target=self._laco, daemon=True, name="motor")

    def iniciar(self):
        self.trabalhador.start()

    def parar(self):
        self._parar.set()

    def ids(self, texto):
        import torch
        ids = self.tokenizer.apply_chat_template([{"role": "user", "content": texto}],
                                                 add_generation_prompt=True, return_tensors="pt")
        ids = ids if isinstance(ids, torch.Tensor) else ids["input_ids"]
        return ids[0].tolist()

    def gerar_lote(self, pedidos):
        import torch
        seqs = [self.ids(p.prompt) for p in pedidos]
        lote = self.tokenizer.pad({"input_ids": seqs}, return_tensors="pt")
        max_new = max(p.max_new_tokens for p in pedidos)
        forcar = any(p.forcar_tamanho for p in pedidos)
        extra = {"min_new_tokens": max_new} if forcar else {}
        with torch.inference_mode():
            saida = self.modelo.generate(input_ids=lote["input_ids"], attention_mask=lote["attention_mask"],
                                         max_new_tokens=max_new, do_sample=False,
                                         pad_token_id=self.tokenizer.pad_token_id, **extra)
        largura = lote["input_ids"].shape[1]
        fim_ids = {self.tokenizer.eos_token_id, self.tokenizer.pad_token_id}
        resultados = []
        for k, p in enumerate(pedidos):
            gerados = saida[k, largura:largura + p.max_new_tokens].tolist()
            if not p.forcar_tamanho:  # saída livre: corta no primeiro fim de sequência
                for i, t in enumerate(gerados):
                    if t in fim_ids:
                        gerados = gerados[:i]
                        break
            resultados.append({"texto": self.tokenizer.decode(gerados, skip_special_tokens=True),
                               "tokens_entrada": len(seqs[k]), "tokens_saida": len(gerados)})
        return resultados

    def _coletar(self):
        try:
            primeiro = self.fila.get(timeout=0.5)
        except queue.Empty:
            return []
        pedidos = [primeiro]
        if self.modo == "serial":
            return pedidos
        limite = time.time() + self.janela_s
        while len(pedidos) < self.lote_max:
            restante = limite - time.time()
            if restante <= 0:
                break
            try:
                pedidos.append(self.fila.get(timeout=restante))
            except queue.Empty:
                break
        return pedidos

    def _laco(self):
        while not self._parar.is_set():
            pedidos = self._coletar()
            if not pedidos:
                continue
            t0 = time.time()
            try:
                resultados = self.gerar_lote(pedidos)
                erro = None
            except Exception as e:  # uma falha do lote volta para cada cliente, o servidor segue
                resultados, erro = [None] * len(pedidos), f"{type(e).__name__}: {e}"
            t1 = time.time()
            for p, r in zip(pedidos, resultados):
                if r is not None:
                    p.resultado = {**r, "t_fila_s": t0 - p.chegada, "t_geracao_s": t1 - t0, "lote": len(pedidos)}
                p.erro = erro
                p.feito.set()


class Manipulador(http.server.BaseHTTPRequestHandler):
    motor: Motor = None
    info: dict = None
    protocol_version = "HTTP/1.1"

    def _json(self, codigo, corpo):
        dados = json.dumps(corpo, ensure_ascii=False).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def do_GET(self):
        if self.path.split("?")[0] == "/saude":
            return self._json(200, {**self.info, "pronto": True, "fila": self.motor.fila.qsize()})
        self._json(404, {"erro": f"Rota inexistente: GET {self.path}"})

    def do_POST(self):
        if self.path != "/v1/gerar":
            return self._json(404, {"erro": f"Rota inexistente: POST {self.path}"})
        try:
            d = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0) or 0)) or b"{}")
            prompt = str(d["prompt"])
            max_new = int(d.get("max_new_tokens", 128))
            if not 1 <= max_new <= 4096:
                raise ValueError("max_new_tokens fora de 1..4096")
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            return self._json(400, {"erro": f"Pedido inválido: {e}"})
        pedido = Pedido(prompt, max_new, bool(d.get("forcar_tamanho", True)))
        self.motor.fila.put(pedido)
        if not pedido.feito.wait(timeout=3600):
            return self._json(504, {"erro": "Tempo esgotado na fila"})
        if pedido.erro:
            return self._json(500, {"erro": pedido.erro})
        self._json(200, pedido.resultado)

    def log_message(self, *args):
        return


def main():
    args = parse_args()
    specs = specs_mod.coletar_ou_sair(args)
    specs_mod.imprimir(specs)
    mem_gb, camadas = experimento.estimar_memoria_gb(args.modelo, args.torch_dtype)
    experimento.checar_memoria(specs, mem_gb, args.forcar_memoria, f"modelo completo ({camadas} camadas)")

    import torch
    threads = args.threads or specs["cpu_nucleos_logicos"]
    torch.set_num_threads(threads)
    info = {"modelo": args.modelo, "torch_dtype": args.torch_dtype, "threads": threads, "modo": args.modo,
            "lote_max": args.lote_max if args.modo == "lote" else 1, "janela_lote_ms": args.janela_lote_ms}
    sessao = SessaoMedida(args.api, specs, args.experimento, "centralizado", "servidor", args.no_id,
                          extras={**info, "tipo": "servidor_http"})

    def ao_sinal(signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, ao_sinal)
    motivo, servidor, motor = "encerrado", None, None
    try:
        sessao.iniciar(args.baseline_s)
        from transformers import AutoModelForCausalLM, AutoTokenizer

        tokenizer = AutoTokenizer.from_pretrained(args.modelo)
        print(f"[CENTRAL] Carregando {args.modelo} ({args.torch_dtype}, {threads} threads) na CPU...", flush=True)
        modelo = AutoModelForCausalLM.from_pretrained(args.modelo, dtype=getattr(torch, args.torch_dtype))
        modelo.eval()
        motor = Motor(modelo, tokenizer, args.modo, args.lote_max, args.janela_lote_ms / 1000)
        print("[CENTRAL] Aquecimento (não contabilizado)...", flush=True)
        motor.gerar_lote([Pedido("Olá", 8, True)])
        motor.iniciar()

        classe = type("Manipulador", (Manipulador,), {"motor": motor, "info": info})
        servidor = http.server.ThreadingHTTPServer((args.host, args.porta), classe)
        servidor.daemon_threads = True
        print(f"{PRONTO} http://{args.host}:{args.porta} (modo {args.modo}, lote máx. {info['lote_max']})", flush=True)
        servidor.serve_forever()
    except KeyboardInterrupt:
        motivo = "interrompido"
    except Exception as e:
        motivo = f"erro: {type(e).__name__}: {e}"
        raise
    finally:
        if motor:
            motor.parar()
        if servidor:
            servidor.server_close()
        sessao.encerrar(motivo)


if __name__ == "__main__":
    main()
