"""Observer: acompanha agentes e telemetria e publica no barramento apenas o que mudou."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .adaptadores import ErroRemoto, Indisponivel


def resumir_relatorio(rel):
    """Remove o detalhamento por máquina das consultas (o painel exibe só a tabela-resumo)."""
    consultas = [{k: c.get(k) for k in ("consulta_id", "cenario", "prompt_id", "repeticao", "duracao_s",
                                         "tokens_saida", "tokens_por_s", "valido", "problemas", "carbono_g",
                                         "agua_total_ml")}
                 | {"total_wh": c["energia"]["total_wh"], "maquinas": len(c.get("maquinas", []))}
                 for c in rel.get("consultas", [])]
    return {"experimento": rel.get("experimento"), "agregado": rel.get("agregado", {}), "consultas": consultas}


class Monitor(threading.Thread):
    def __init__(self, config, agentes, telemetria, barramento, experimento_atual, intervalo_s=1.5,
                 intervalo_relatorio_s=5.0):
        super().__init__(daemon=True)
        self.config, self.agentes, self.telemetria, self.barramento = config, agentes, telemetria, barramento
        self.experimento_atual = experimento_atual   # função -> nome do experimento exibido
        self.intervalo_s, self.intervalo_relatorio_s = intervalo_s, intervalo_relatorio_s
        self._parar = threading.Event()
        self._ultimo_snapshot = None
        self._seq_logs = {}          # (maquina, papel) -> última linha enviada
        self._ultimo_relatorio = 0.0
        self._assinatura_relatorio = None
        self._pool = ThreadPoolExecutor(max_workers=max(2, len(config.maquinas)), thread_name_prefix="monitor")

    def run(self):
        while not self._parar.wait(self.intervalo_s):
            self._coletar_maquinas()
            if time.time() - self._ultimo_relatorio >= self.intervalo_relatorio_s:
                self._ultimo_relatorio = time.time()
                self._coletar_relatorio()

    def parar(self):
        self._parar.set()

    def _consultar(self, m):
        item = {"nome": m.nome, "agente": m.agente, "papeis": list(m.papeis), "num_blocks": m.num_blocks,
                "online": False, "processos": {}, "erro": None}
        try:
            item["processos"] = {k: v for k, v in self.agentes[m.nome].processos().items() if k in m.papeis}
            item["online"] = True
        except (Indisponivel, ErroRemoto) as e:
            item["erro"] = str(e)
        for papel, proc in item["processos"].items():
            self._coletar_logs(m.nome, papel, proc)
        return item

    def _coletar_maquinas(self):
        # em paralelo: uma máquina fora do ar (timeout) não atrasa as demais
        snapshot = list(self._pool.map(self._consultar, self.config.maquinas))
        if snapshot != self._ultimo_snapshot:
            self._ultimo_snapshot = snapshot
            self.barramento.publicar("maquinas", snapshot)

    def _coletar_logs(self, maquina, papel, proc):
        chave = (maquina, papel)
        ultima = self._seq_logs.get(chave, 0)
        if proc["ultima_seq"] < ultima:      # processo reiniciado: recomeça a numeração
            ultima = 0
        if proc["ultima_seq"] == ultima:
            return
        try:
            linhas = self.agentes[maquina].logs(papel, ultima)
        except (Indisponivel, ErroRemoto):
            return
        if linhas:
            self._seq_logs[chave] = linhas[-1]["seq"]
            self.barramento.publicar("log", {"maquina": maquina, "papel": papel,
                                             "linhas": [l["linha"] for l in linhas], "reinicio": ultima == 0})

    def _coletar_relatorio(self):
        exp = self.experimento_atual()
        if not exp:
            return
        try:
            rel = resumir_relatorio(self.telemetria.relatorio(exp))
        except (Indisponivel, ErroRemoto):
            return
        assinatura = (exp, len(rel["consultas"]), str(rel["agregado"]))
        if assinatura != self._assinatura_relatorio:
            self._assinatura_relatorio = assinatura
            self.barramento.publicar("relatorio", rel)
