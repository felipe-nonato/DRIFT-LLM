"""Observer: barramento de eventos em memória, consumido pelo navegador via SSE."""

import queue
import threading
import time

# tipos cujo último valor é reenviado a quem se inscreve depois (estado atual da tela)
TIPOS_DE_ESTADO = {"maquinas", "experimento", "relatorio", "specs"}


class Barramento:
    def __init__(self, capacidade=2000):
        self._assinantes = set()
        self._ultimos = {}
        self._lock = threading.Lock()
        self._capacidade = capacidade

    def assinar(self):
        fila = queue.Queue(maxsize=self._capacidade)
        with self._lock:
            self._assinantes.add(fila)
            for evento in self._ultimos.values():
                fila.put_nowait(evento)
        return fila

    def cancelar(self, fila):
        with self._lock:
            self._assinantes.discard(fila)

    def publicar(self, tipo, dados):
        evento = {"tipo": tipo, "dados": dados, "t": time.time()}
        with self._lock:
            if tipo in TIPOS_DE_ESTADO:
                self._ultimos[tipo] = evento
            assinantes = list(self._assinantes)
        for fila in assinantes:
            try:
                fila.put_nowait(evento)
            except queue.Full:  # navegador lento: descarta em vez de travar o painel
                pass

    def ultimo(self, tipo):
        with self._lock:
            e = self._ultimos.get(tipo)
        return e["dados"] if e else None
