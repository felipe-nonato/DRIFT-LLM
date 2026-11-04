"""Ciclo de vida dos processos da bancada em uma máquina.

State:    Estado + TRANSICOES — cada processo só muda de estado por transições válidas.
Strategy: Papel — cada papel (telemetria, servidor, cliente, centralizado) define o módulo,
          a lista FECHADA de argumentos aceitos e os detectores que leem a saída do processo.

Toda a saída dos processos filhos é drenada por uma thread e gravada em arquivo, de modo
que nenhum processo bloqueia por pipe cheio.
"""

import importlib.util
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional

from bancada import caminhos


class Estado(str, Enum):
    PARADO = "parado"
    INICIANDO = "iniciando"
    LINHA_DE_BASE = "linha_de_base"
    OPERANDO = "operando"
    PARANDO = "parando"
    CONCLUIDO = "concluido"
    FALHOU = "falhou"


ATIVOS = {Estado.INICIANDO, Estado.LINHA_DE_BASE, Estado.OPERANDO, Estado.PARANDO}
FINAIS = {Estado.PARADO, Estado.CONCLUIDO, Estado.FALHOU}

TRANSICOES = {
    Estado.PARADO: {Estado.INICIANDO},
    Estado.INICIANDO: {Estado.LINHA_DE_BASE, Estado.OPERANDO, Estado.PARANDO, Estado.CONCLUIDO, Estado.FALHOU},
    Estado.LINHA_DE_BASE: {Estado.OPERANDO, Estado.PARANDO, Estado.CONCLUIDO, Estado.FALHOU},
    Estado.OPERANDO: {Estado.PARANDO, Estado.CONCLUIDO, Estado.FALHOU},
    Estado.PARANDO: {Estado.PARADO, Estado.CONCLUIDO, Estado.FALHOU},
    Estado.CONCLUIDO: {Estado.INICIANDO},
    Estado.FALHOU: {Estado.INICIANDO},
}


class ErroProcesso(Exception):
    pass


# ---------------------------------------------------------------- papéis (Strategy)

VALOR, FLAG = "valor", "flag"


@dataclass(frozen=True)
class Detector:
    """Reconhece uma linha da saída e produz um novo estado e/ou um artefato."""
    padrao: re.Pattern
    estado: Optional[Estado] = None
    artefato: Optional[str] = None
    extrair: Callable[[re.Match], str] = lambda m: m.group(1) if m.groups() else m.group(0)


@dataclass(frozen=True)
class Papel:
    nome: str
    modulo: str
    argumentos: dict                      # nome -> VALOR | FLAG (lista fechada)
    obrigatorios: tuple = ()
    detectores: tuple = ()
    ambiente: dict = field(default_factory=dict)   # argumento -> variável de ambiente
    dependencias: tuple = ()                       # módulos Python exigidos pelo papel

    def dependencias_faltando(self):
        """Verifica no MESMO interpretador que executará o papel (sys.executable)."""
        return [m for m in self.dependencias if importlib.util.find_spec(m) is None]

    def montar(self, args: dict):
        """Converte o dicionário de argumentos em linha de comando, recusando o que não é permitido."""
        desconhecidos = set(args) - set(self.argumentos) - set(self.ambiente)
        if desconhecidos:
            raise ErroProcesso(f"Argumentos não permitidos para '{self.nome}': {sorted(desconhecidos)}")
        faltando = [a for a in self.obrigatorios if args.get(a) in (None, "")]
        if faltando:
            raise ErroProcesso(f"Argumentos obrigatórios ausentes para '{self.nome}': {faltando}")
        comando = [sys.executable, "-m", self.modulo]
        env = {}
        for nome, valor in args.items():
            if valor is None or valor is False or valor == "":
                continue
            if nome in self.ambiente:
                env[self.ambiente[nome]] = str(valor)
                continue
            if self.argumentos[nome] == FLAG:
                comando.append(f"--{nome}")
                continue
            if not isinstance(valor, (str, int, float)) or isinstance(valor, bool):
                raise ErroProcesso(f"Valor inválido para --{nome}")
            texto = str(valor)
            if texto.startswith("-") and not re.fullmatch(r"-?\d+(\.\d+)?", texto):
                raise ErroProcesso(f"Valor de --{nome} não pode começar com '-'")
            comando += [f"--{nome}", texto]
        return comando, env


_LINHA_DE_BASE = Detector(re.compile(r"Medindo linha de base"), estado=Estado.LINHA_DE_BASE)
_OPERANDO = Detector(re.compile(r"Fase de operação iniciada"), estado=Estado.OPERANDO)
_SPECS = Detector(re.compile(r"Especificações obrigatórias incompletas"), artefato="erro",
                  extrair=lambda m: "especificações incompletas (veja o log)")
_RELATORIO = Detector(re.compile(r"Relatório salvo em (\S+)\.json"), artefato="relatorio")
_COMUNS_SPECS = {"interface": VALOR, "tdp": VALOR, "sem_rapl": FLAG, "permitir_virtualizado": FLAG}
_COMUNS_EXP = {"api": VALOR, "experimento": VALOR, "no_id": VALOR, "modelo": VALOR, "torch_dtype": VALOR,
               "baseline_s": VALOR}


def _progresso(rotulo):
    return Detector(re.compile(rf"\[{rotulo}\] (\d+)/(\d+) "), artefato="progresso",
                    extrair=lambda m: f"{m.group(1)}/{m.group(2)}")


PAPEIS = {
    "telemetria": Papel(
        nome="telemetria", modulo="bancada.telemetria", argumentos={},
        ambiente={"porta": "TELEMETRIA_PORTA"},
        detectores=(Detector(re.compile(r"\[API\] Ouvindo na porta"), estado=Estado.OPERANDO),),
    ),
    "servidor": Papel(
        nome="servidor", modulo="bancada.no_p2p", dependencias=("torch", "transformers", "drift"),
        argumentos={**_COMUNS_EXP, **_COMUNS_SPECS, "num_blocks": VALOR, "block_indices": VALOR,
                    "bootstrap": FLAG, "initial_peers": VALOR, "porta": VALOR, "forcar_memoria": FLAG,
                    "session_timeout": VALOR, "step_timeout": VALOR},
        obrigatorios=("api", "experimento", "no_id", "torch_dtype"),
        detectores=(_LINHA_DE_BASE, _OPERANDO, _SPECS,
                    Detector(re.compile(r"\[MULTIADDR\] (\S+)"), artefato="multiaddr")),
    ),
    "cliente": Papel(
        nome="cliente", modulo="bancada.cliente_p2p", dependencias=("torch", "transformers", "drift"),
        argumentos={**_COMUNS_EXP, **_COMUNS_SPECS, "initial_peers": VALOR, "repeticoes": VALOR,
                    "max_new_tokens": VALOR, "pausa_s": VALOR},
        obrigatorios=("api", "experimento", "initial_peers", "torch_dtype"),
        detectores=(_LINHA_DE_BASE, _OPERANDO, _SPECS, _progresso("P2P"), _RELATORIO),
    ),
    "centralizado": Papel(
        nome="centralizado", modulo="bancada.centralizado", dependencias=("torch", "transformers"),
        argumentos={**_COMUNS_EXP, **_COMUNS_SPECS, "threads": VALOR, "repeticoes": VALOR,
                    "max_new_tokens": VALOR, "pausa_s": VALOR, "forcar_memoria": FLAG},
        obrigatorios=("api", "experimento", "torch_dtype"),
        detectores=(_LINHA_DE_BASE, _OPERANDO, _SPECS, _progresso("CENTRAL"), _RELATORIO),
    ),
}


# ---------------------------------------------------------------- gerenciador (State)

class GerenciadorProcesso:
    def __init__(self, papel: Papel, limite_linhas=600, ao_mudar=None):
        self.papel = papel
        self.ao_mudar = ao_mudar
        self._lock = threading.RLock()
        self._proc = None
        self._parada_solicitada = False
        self.estado = Estado.PARADO
        self.codigo_saida = None
        self.iniciado_em = None
        self.finalizado_em = None
        self.arquivo_log = None
        self.comando = None
        self.artefatos = {}
        self._linhas = deque(maxlen=limite_linhas)
        self._seq = 0

    # -------------------------------------------------- transições
    def _ir_para(self, novo: Estado):
        with self._lock:
            if novo == self.estado:
                return
            if novo not in TRANSICOES[self.estado]:
                return  # transição inválida é ignorada (ex.: linha atrasada após falha)
            self.estado = novo
        if self.ao_mudar:
            self.ao_mudar(self)

    # -------------------------------------------------- operações
    def iniciar(self, args: dict):
        with self._lock:
            if self.estado in ATIVOS:
                raise ErroProcesso(f"'{self.papel.nome}' já está em execução ({self.estado.value}).")
            faltando = self.papel.dependencias_faltando()
            if faltando:
                raise ErroProcesso(f"Dependências ausentes para '{self.papel.nome}' em {sys.executable}: "
                                   f"{', '.join(faltando)}. Instale-as no ambiente usado pelo agente.")
            comando, env_extra = self.papel.montar(args)
            caminhos.garantir_diretorios()
            self.arquivo_log = caminhos.DIR_LOGS / f"{self.papel.nome}_{time.strftime('%Y%m%d-%H%M%S')}.log"
            env = {**os.environ, **env_extra, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
            self._proc = subprocess.Popen(
                comando, cwd=caminhos.RAIZ, env=env, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
                encoding="utf-8", errors="replace", start_new_session=True,
            )
            self.comando = comando
            self.codigo_saida = None
            self.iniciado_em = time.time()
            self.finalizado_em = None
            self.artefatos = {}
            self._parada_solicitada = False
            self._linhas.clear()
        self._ir_para(Estado.INICIANDO)  # todo estado final admite reinício
        threading.Thread(target=self._drenar, args=(self._proc,), daemon=True).start()
        return self.instantaneo()

    def parar(self, tolerancia_s=30):
        with self._lock:
            proc = self._proc
            if self.estado not in ATIVOS or proc is None:
                return self.instantaneo()
            self._parada_solicitada = True
        self._ir_para(Estado.PARANDO)
        # SIGINT -> os scripts tratam KeyboardInterrupt e encerram a sessão de medição na API
        try:
            proc.send_signal(signal.SIGINT)
        except ProcessLookupError:
            pass
        threading.Thread(target=self._escalonar, args=(proc, tolerancia_s), daemon=True).start()
        return self.instantaneo()

    def aguardar_fim(self, timeout_s=40):
        """Bloqueia até o processo terminar (usado no desligamento do agente)."""
        proc = self._proc
        if proc is None:
            return
        try:
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    @staticmethod
    def _escalonar(proc, tolerancia_s):
        try:
            proc.wait(timeout=tolerancia_s)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()

    def _drenar(self, proc):
        with open(self.arquivo_log, "a", encoding="utf-8") as log:
            for linha in proc.stdout:
                linha = linha.rstrip("\n")
                log.write(linha + "\n")
                log.flush()
                with self._lock:
                    self._seq += 1
                    self._linhas.append((self._seq, linha))
                for d in self.papel.detectores:
                    m = d.padrao.search(linha)
                    if not m:
                        continue
                    if d.artefato:
                        with self._lock:
                            self.artefatos[d.artefato] = d.extrair(m)
                        if self.ao_mudar:
                            self.ao_mudar(self)
                    if d.estado:
                        self._ir_para(d.estado)
        codigo = proc.wait()
        with self._lock:
            self.codigo_saida = codigo
            self.finalizado_em = time.time()
            if self._parada_solicitada:
                final = Estado.PARADO
            else:
                final = Estado.CONCLUIDO if codigo == 0 else Estado.FALHOU
        self._ir_para(final)

    # -------------------------------------------------- consulta
    def logs(self, desde=0):
        with self._lock:
            return [{"seq": s, "linha": l} for s, l in self._linhas if s > desde]

    def instantaneo(self):
        with self._lock:
            return {
                "papel": self.papel.nome,
                "estado": self.estado.value,
                "ativo": self.estado in ATIVOS,
                "codigo_saida": self.codigo_saida,
                "iniciado_em": self.iniciado_em,
                "finalizado_em": self.finalizado_em,
                "artefatos": dict(self.artefatos),
                "ultima_seq": self._seq,
                "log": str(self.arquivo_log) if self.arquivo_log else None,
            }
