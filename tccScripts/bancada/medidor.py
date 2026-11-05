"""
Medição contínua e envio de amostras BRUTAS para a API de telemetria.

Cada intervalo enviado contém apenas grandezas observadas na máquina:
  - energia RAPL (package e, se houver, dram) em joules  -> medido
  - utilização da CPU da máquina inteira (0–1)          -> para o modelo estimado
  - bytes enviados/recebidos na interface do enxame
com timestamps no RELÓGIO DA API (offset sincronizado), para que janelas de
consulta de máquinas diferentes sejam comparáveis.

A conversão em Wh (TDP, fração ociosa, W/GB de RAM etc.) é feita só na API,
com os parâmetros de config_tcc.json.

A medição é da MÁQUINA (não do processo): captura servidor Drift, cliente,
tokenização e o que mais rodar. Por isso existe a linha de base ociosa,
medida antes de qualquer carga, e a API deduplica máquinas (maquina_id).
"""

import json
import threading
import time
import uuid
from pathlib import Path

import psutil
import requests

from .caminhos import DIR_LOGS


def com_tentativas(descricao, fn, tentativas=8, espera_s=2.0, espera_max_s=30.0):
    """Repete fn() em falhas de rede/HTTP 5xx com espera crescente. Erros 4xx não são repetidos.

    Só use com operações idempotentes (a API aceita sessao_id/chave para isso).
    """
    for n in range(1, tentativas + 1):
        try:
            r = fn()
            if r.status_code < 500:
                return r
            erro = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:
            erro = f"{type(e).__name__}: {e}"
        if n == tentativas:
            raise RuntimeError(f"{descricao}: falhou {tentativas} vezes (último erro: {erro})")
        print(f"[api] ⚠️ {descricao} falhou ({erro}); tentativa {n + 1}/{tentativas} em {espera_s:.0f}s", flush=True)
        time.sleep(espera_s)
        espera_s = min(espera_s * 2, espera_max_s)


class Relogio:
    """Offset entre o relógio local e o da API (estilo NTP: menor RTT vence)."""

    def __init__(self, api):
        self.api = api
        self.offset = 0.0
        self.incerteza_s = None

    def sincronizar(self, rodadas=9):
        medidas = []
        for _ in range(rodadas):
            t0 = time.time()
            try:
                r = requests.get(f"{self.api}/tempo", timeout=5)
                r.raise_for_status()
            except requests.RequestException:
                continue  # uma rodada perdida não estraga a sincronização
            t1 = time.time()
            medidas.append((t1 - t0, r.json()["t"] - (t0 + t1) / 2))
        if not medidas:
            com_tentativas("sincronizar relógio", lambda: requests.get(f"{self.api}/tempo", timeout=5))
            return self.sincronizar(rodadas)
        rtt, offset = min(medidas)
        self.offset, self.incerteza_s = offset, rtt / 2
        return self

    def agora(self):
        return time.time() + self.offset


class Medidor(threading.Thread):
    def __init__(self, specs, intervalo_s, relogio, ao_coletar):
        super().__init__(daemon=True)
        self.dominios = [d for d in specs.get("rapl_dominios", [])]
        self.iface = specs["rede_interface"]
        self.intervalo = intervalo_s
        self.relogio = relogio
        self.ao_coletar = ao_coletar
        self.fase = "baseline"
        self._parar = threading.Event()

    def _ler_rapl(self):
        return {d["zona"]: int(Path(d["arquivo"]).read_text()) for d in self.dominios}

    def _rede(self):
        c = psutil.net_io_counters(pernic=True).get(self.iface)
        return (c.bytes_sent, c.bytes_recv) if c else (0, 0)

    def _energia(self, ant, atual, tipo):
        if ant is None:
            return None
        total, achou = 0, False
        for d in self.dominios:
            if d["tipo"] != tipo:
                continue
            achou = True
            delta = atual[d["zona"]] - ant[d["zona"]]
            if delta < 0:  # contador deu a volta
                delta += d["max_uj"] or 0
            total += delta
        return total / 1e6 if achou else None

    def run(self):
        psutil.cpu_percent(None)
        rapl_ant = self._ler_rapl() if self.dominios else None
        rede_ant = self._rede()
        t_ant = self.relogio.agora()
        proximo = time.monotonic() + self.intervalo
        while True:
            parar = self._parar.wait(max(0.0, proximo - time.monotonic()))
            proximo += self.intervalo
            t = self.relogio.agora()
            util = psutil.cpu_percent(None) / 100.0
            rapl = self._ler_rapl() if self.dominios else None
            rede = self._rede()
            self.ao_coletar({
                "t_ini": t_ant,
                "t_fim": t,
                "fase": self.fase,
                "cpu_util": util,
                "e_pkg_j": self._energia(rapl_ant, rapl, "pkg"),
                "e_dram_j": self._energia(rapl_ant, rapl, "dram"),
                "bytes_tx": max(0, rede[0] - rede_ant[0]),
                "bytes_rx": max(0, rede[1] - rede_ant[1]),
            })
            t_ant, rapl_ant, rede_ant = t, rapl, rede
            if parar:
                break

    def parar(self):
        self._parar.set()
        self.join(timeout=self.intervalo * 3)


class Enviador(threading.Thread):
    """Envia lotes para a API e mantém cópia local (JSONL): nenhum dado se perde."""

    def __init__(self, api, sessao_id, lote_s):
        super().__init__(daemon=True)
        self.api = api
        self.sessao_id = sessao_id
        self.lote_s = lote_s
        self.buffer = []
        self.lock = threading.Lock()
        self._parar = threading.Event()
        DIR_LOGS.mkdir(parents=True, exist_ok=True)
        self.backup = open(DIR_LOGS / f"amostras_{sessao_id}.jsonl", "a", encoding="utf-8")

    def adicionar(self, intervalo):
        with self.lock:
            self.buffer.append(intervalo)
            self.backup.write(json.dumps(intervalo) + "\n")
            self.backup.flush()

    def _enviar(self):
        with self.lock:
            lote, self.buffer = self.buffer, []
        if not lote:
            return True
        try:
            r = requests.post(
                f"{self.api}/amostras",
                json={"sessao_id": self.sessao_id, "intervalos": lote},
                timeout=30,  # reenvio é seguro: a API ignora intervalos já gravados
            )
            r.raise_for_status()
            return True
        except Exception as e:
            print(f"[medidor] ⚠️ falha ao enviar {len(lote)} amostras ({e}); nova tentativa no próximo lote")
            with self.lock:
                self.buffer = lote + self.buffer
            return False

    def run(self):
        while not self._parar.wait(self.lote_s):
            self._enviar()

    def parar(self):
        self._parar.set()
        self.join(timeout=self.lote_s * 2)
        for _ in range(5):
            if self._enviar():
                break
            time.sleep(2)
        self.backup.close()


class SessaoMedida:
    """
    Ciclo de vida de uma máquina medida:
      registrar specs na API -> linha de base ociosa -> fase 'operacao' -> encerrar.
    """

    def __init__(self, api, specs, experimento, cenario, papel, no_id, extras=None):
        self.api = api.rstrip("/")
        self.specs = specs
        self.experimento = experimento
        self.cenario = cenario
        self.papel = papel
        self.no_id = no_id
        self.extras = extras or {}
        self.relogio = Relogio(self.api)
        self.id = None
        self.medidor = None
        self.enviador = None
        self.cfg = None

    def iniciar(self, baseline_s=None):
        self.relogio.sincronizar()
        print(f"[{self.no_id}] Relógio sincronizado com a API (offset {self.relogio.offset:+.3f}s, "
              f"incerteza ±{self.relogio.incerteza_s * 1000:.1f} ms)")
        self.cfg = com_tentativas("ler /config", lambda: requests.get(f"{self.api}/config", timeout=10)
                                  ).json()["amostragem"]
        # id gerado aqui: se a resposta se perder, repetir o registro devolve a mesma sessão
        corpo = {
            "sessao_id": uuid.uuid4().hex[:12],
            "experimento": self.experimento,
            "cenario": self.cenario,
            "papel": self.papel,
            "no_id": self.no_id,
            "inicio": self.relogio.agora(),
            "relogio_offset_s": self.relogio.offset,
            "relogio_incerteza_s": self.relogio.incerteza_s,
            "specs": self.specs,
            "extras": self.extras,
        }
        r = com_tentativas("registrar sessão", lambda: requests.post(f"{self.api}/sessoes", json=corpo, timeout=30))
        if r.status_code != 200:
            raise SystemExit(f"[{self.no_id}] ❌ API recusou o registro: {r.text}")
        self.id = r.json()["sessao_id"]
        print(f"[{self.no_id}] Sessão registrada: {self.id}")

        self.enviador = Enviador(self.api, self.id, self.cfg["lote_envio_s"])
        self.medidor = Medidor(self.specs, self.cfg["intervalo_s"], self.relogio, self.enviador.adicionar)
        self.enviador.start()
        self.medidor.start()

        segundos = self.cfg["baseline_s"] if baseline_s is None else baseline_s
        print(f"[{self.no_id}] Medindo linha de base OCIOSA por {segundos}s — não use a máquina...")
        time.sleep(segundos)
        self.medidor.fase = "operacao"
        print(f"[{self.no_id}] Linha de base concluída. Fase de operação iniciada.")
        return self

    def encerrar(self, motivo):
        if self.medidor:
            self.medidor.parar()
        if self.enviador:
            self.enviador.parar()
        if self.id:
            try:
                corpo = {"fim": self.relogio.agora(), "motivo": motivo}
                com_tentativas("encerrar sessão", lambda: requests.post(
                    f"{self.api}/sessoes/{self.id}/encerrar", json=corpo, timeout=30), tentativas=4)
            except Exception as e:
                print(f"[{self.no_id}] ⚠️ não foi possível registrar o encerramento: {e}")
        print(f"[{self.no_id}] Sessão encerrada ({motivo}). Backup local em logs/amostras_{self.id}.jsonl")
