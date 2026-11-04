"""Command: cada passo do protocolo experimental é um objeto executável e auditável.

Os comandos não sabem nada de HTTP: falam com as máquinas pelo Contexto (adapters).
"""

import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable

from .adaptadores import ClienteAgente, ClienteTelemetria, ErroRemoto, Indisponivel
from .configuracao import Configuracao


class Cancelado(Exception):
    pass


class FalhaComando(Exception):
    pass


@dataclass
class Contexto:
    config: Configuracao
    agentes: dict                 # nome da máquina -> ClienteAgente
    telemetria: ClienteTelemetria
    parametros: dict
    cancelado: threading.Event = field(default_factory=threading.Event)
    dados: dict = field(default_factory=dict)   # valores produzidos por um passo e usados por outros

    def agente(self, maquina) -> ClienteAgente:
        return self.agentes[maquina]

    def esperar(self, condicao: Callable[[], object], timeout_s, intervalo_s=2.0, descricao="condição"):
        """Repete `condicao` até retornar valor verdadeiro, respeitando cancelamento e prazo."""
        limite = time.time() + timeout_s if timeout_s else None
        while True:
            if self.cancelado.is_set():
                raise Cancelado()
            valor = condicao()
            if valor:
                return valor
            if limite and time.time() > limite:
                raise FalhaComando(f"Tempo esgotado aguardando {descricao} ({timeout_s:.0f}s).")
            self.cancelado.wait(intervalo_s)


class Comando(ABC):
    @property
    @abstractmethod
    def descricao(self) -> str: ...

    @abstractmethod
    def executar(self, ctx: Contexto) -> str | None:
        """Executa o passo; retorna um detalhe opcional para o histórico."""


def _estado(ctx, maquina, papel):
    try:
        return ctx.agente(maquina).processos()[papel]
    except Indisponivel:
        return None


class ValidarSpecs(Comando):
    def __init__(self, maquinas):
        self.maquinas = maquinas

    @property
    def descricao(self):
        return f"Validar especificações ({', '.join(m.nome for m in self.maquinas)})"

    def executar(self, ctx):
        problemas = []
        for m in self.maquinas:
            try:
                r = ctx.agente(m.nome).specs(m.interface, m.tdp, m.sem_rapl, m.papeis)
            except Indisponivel as e:
                problemas.append(f"{m.nome}: agente indisponível ({e})")
                continue
            problemas += [f"{m.nome}: {p}" for p in r["problemas"]]
        if problemas:
            raise FalhaComando("\n".join(problemas))
        return "todas as máquinas com specs completas"


class GarantirProcesso(Comando):
    """Inicia o processo se ainda não estiver ativo e aguarda o estado 'operando'."""

    def __init__(self, maquina, papel, args, timeout_s=60):
        self.maquina, self.papel, self.args, self.timeout_s = maquina, papel, args, timeout_s

    @property
    def descricao(self):
        return f"Garantir {self.papel} em {self.maquina}"

    def executar(self, ctx):
        atual = _estado(ctx, self.maquina, self.papel)
        if atual is None:
            raise FalhaComando(f"Agente de {self.maquina} indisponível.")
        if not atual["ativo"]:
            ctx.agente(self.maquina).iniciar(self.papel, self.args)
        ctx.esperar(lambda: self._operando(ctx), self.timeout_s, 1.0, f"{self.papel} em {self.maquina}")
        return "em operação"

    def _operando(self, ctx):
        e = _estado(ctx, self.maquina, self.papel)
        if e and e["estado"] in ("falhou", "concluido", "parado") and e["iniciado_em"]:
            raise FalhaComando(f"{self.papel} em {self.maquina} terminou ({e['estado']}). Veja o log.")
        return e and e["estado"] == "operando"


class IniciarProcesso(Comando):
    """Inicia um papel; `args` pode ser função do contexto (ex.: usa o multiaddr obtido antes)."""

    def __init__(self, maquina, papel, args):
        self.maquina, self.papel, self.args = maquina, papel, args

    @property
    def descricao(self):
        return f"Iniciar {self.papel} em {self.maquina}"

    def executar(self, ctx):
        args = self.args(ctx) if callable(self.args) else self.args
        try:
            ctx.agente(self.maquina).iniciar(self.papel, args)
        except (ErroRemoto, Indisponivel) as e:
            raise FalhaComando(f"{self.maquina}: {e}")
        return None


class AguardarEstado(Comando):
    def __init__(self, maquina, papel, estado, timeout_s):
        self.maquina, self.papel, self.estado, self.timeout_s = maquina, papel, estado, timeout_s

    @property
    def descricao(self):
        return f"Aguardar {self.papel} em {self.maquina} chegar a '{self.estado}'"

    def executar(self, ctx):
        def pronto():
            e = _estado(ctx, self.maquina, self.papel)
            if e and e["estado"] in ("falhou", "concluido", "parado"):
                raise FalhaComando(f"{self.papel} em {self.maquina} terminou ({e['estado']}). Veja o log.")
            return e and e["estado"] == self.estado
        ctx.esperar(pronto, self.timeout_s, 2.0, self.descricao.lower())
        return None


class AguardarMultiaddr(Comando):
    """Obtém o endereço do bootstrap a partir da API (publicado pelo próprio nó)."""

    def __init__(self, maquina_bootstrap, timeout_s):
        self.maquina, self.timeout_s = maquina_bootstrap, timeout_s

    @property
    def descricao(self):
        return "Aguardar o nó bootstrap publicar seu endereço (PEER_ID)"

    def executar(self, ctx):
        exp = ctx.parametros["experimento"]

        def obter():
            e = _estado(ctx, self.maquina, "servidor")
            if e and e["estado"] in ("falhou", "concluido", "parado"):
                raise FalhaComando(f"O bootstrap em {self.maquina} terminou ({e['estado']}). Veja o log.")
            try:
                return ctx.telemetria.multiaddr_bootstrap(exp)
            except Indisponivel:
                return None

        ctx.dados["multiaddr"] = ctx.esperar(obter, self.timeout_s, 3.0, "o multiaddr do bootstrap")
        return ctx.dados["multiaddr"]


class Aguardar(Comando):
    def __init__(self, segundos, motivo):
        self.segundos, self.motivo = segundos, motivo

    @property
    def descricao(self):
        return f"Aguardar {self.segundos:.0f}s ({self.motivo})"

    def executar(self, ctx):
        if ctx.cancelado.wait(self.segundos):
            raise Cancelado()
        return None


class AguardarConclusao(Comando):
    def __init__(self, maquina, papel, timeout_s=None):
        self.maquina, self.papel, self.timeout_s = maquina, papel, timeout_s

    @property
    def descricao(self):
        return f"Aguardar {self.papel} em {self.maquina} concluir a carga de prompts"

    def executar(self, ctx):
        def fim():
            e = _estado(ctx, self.maquina, self.papel)
            if e is None:
                return None
            if e["estado"] == "falhou" or (e["estado"] == "parado" and e["iniciado_em"]):
                raise FalhaComando(f"{self.papel} em {self.maquina} terminou com estado '{e['estado']}' "
                                   f"(código {e['codigo_saida']}). Veja o log.")
            return e if e["estado"] == "concluido" else None

        e = ctx.esperar(fim, self.timeout_s, 3.0, self.descricao.lower())
        return e["artefatos"].get("relatorio")


class PararProcesso(Comando):
    def __init__(self, maquina, papel):
        self.maquina, self.papel = maquina, papel

    @property
    def descricao(self):
        return f"Parar {self.papel} em {self.maquina}"

    def executar(self, ctx):
        try:
            ctx.agente(self.maquina).parar(self.papel)
        except Indisponivel as e:
            return f"agente indisponível: {e}"
        return None
