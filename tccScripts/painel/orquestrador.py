"""Orquestração dos experimentos.

Builder: `plano_p2p` e `plano_centralizado` montam a sequência de comandos do protocolo
         (Seção 3.5 do TCC) a partir do inventário de máquinas e dos parâmetros.
State:   o Orquestrador executa um plano por vez (ocioso → executando → concluído/falhou/cancelado),
         registra o histórico de cada passo e, em falha ou cancelamento, executa a limpeza.
"""

import re
import threading
import time
from enum import Enum

from bancada.experimento import DTYPES

from .comandos import (Aguardar, AguardarConclusao, AguardarEstado, AguardarMultiaddr, Cancelado, Contexto,
                       FalhaComando, GarantirProcesso, IniciarProcesso, PararProcesso, ValidarSpecs)
from .configuracao import Configuracao


class EstadoExperimento(str, Enum):
    OCIOSO = "ocioso"
    EXECUTANDO = "executando"
    CONCLUIDO = "concluido"
    FALHOU = "falhou"
    CANCELADO = "cancelado"


class ParametrosInvalidos(Exception):
    pass


PADRAO = {"repeticoes": 3, "max_new_tokens": 128, "pausa_s": 15.0, "baseline_s": 60.0, "espera_blocos_s": 120.0}


def validar_parametros(config: Configuracao, p: dict, cenario: str) -> dict:
    p = {**PADRAO, **{k: v for k, v in p.items() if v not in (None, "")}}
    erros = []
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", str(p.get("experimento", ""))):
        erros.append("experimento: use 1–64 caracteres entre letras, números, '.', '_' e '-'.")
    if p.get("torch_dtype") not in DTYPES:
        erros.append(f"torch_dtype: escolha entre {list(DTYPES)}.")
    limites = {"repeticoes": (1, 50), "max_new_tokens": (1, 4096), "pausa_s": (0, 600),
               "baseline_s": (10, 600), "espera_blocos_s": (0, 3600)}
    for nome, (mn, mx) in limites.items():
        try:
            v = float(p[nome])
            if not mn <= v <= mx:
                raise ValueError
            p[nome] = int(v) if nome in ("repeticoes", "max_new_tokens") else v
        except (TypeError, ValueError):
            erros.append(f"{nome}: valor entre {mn} e {mx}.")
    nomes = {m.nome for m in config.maquinas}
    for chave, papel in (("bootstrap", "servidor"), ("cliente", "cliente"), ("central", "centralizado")):
        if p.get(chave) and (p[chave] not in nomes or papel not in config.maquina(p[chave]).papeis):
            erros.append(f"{chave}: a máquina '{p[chave]}' não tem o papel '{papel}'.")
    if cenario == "p2p":
        servidores = config.com_papel("servidor")
        total = sum(m.num_blocks for m in servidores)
        if not servidores:
            erros.append("Nenhuma máquina com papel 'servidor'.")
        elif total < config.camadas:
            erros.append(f"Os servidores somam {total} blocos; o modelo tem {config.camadas} camadas.")
        if not config.com_papel("cliente"):
            erros.append("Nenhuma máquina com papel 'cliente'.")
    elif not config.com_papel("centralizado"):
        erros.append("Nenhuma máquina com papel 'centralizado'.")
    if erros:
        raise ParametrosInvalidos("\n".join(erros))
    return p


def _args_comuns(config, maquina, p, papel):
    return {"api": config.telemetria_url, "experimento": p["experimento"], "modelo": config.modelo,
            "torch_dtype": p["torch_dtype"], "baseline_s": p["baseline_s"], "no_id": maquina.nome
            if papel != "cliente" else f"{maquina.nome}-cliente", **maquina.args_specs()}


def plano_p2p(config: Configuracao, p: dict):
    servidores = config.com_papel("servidor")
    bootstrap = config.maquina(p.get("bootstrap") or servidores[0].nome)
    cliente = config.maquina(p.get("cliente") or config.com_papel("cliente")[0].nome)
    outros = [m for m in servidores if m.nome != bootstrap.nome]
    tel = config.maquina(config.telemetria_maquina)
    envolvidas = list({m.nome: m for m in [tel, *servidores, cliente]}.values())
    prazo_inicio = p["baseline_s"] + 900  # linha de base + download/carregamento dos blocos

    def args_servidor(m, extra):
        return {**_args_comuns(config, m, p, "servidor"), "num_blocks": m.num_blocks, **extra}

    comandos = [
        ValidarSpecs(envolvidas),
        GarantirProcesso(tel.nome, "telemetria", {"porta": config.telemetria_porta}),
        IniciarProcesso(bootstrap.nome, "servidor", args_servidor(bootstrap, {"bootstrap": True})),
        AguardarMultiaddr(bootstrap.nome, prazo_inicio),
        *[IniciarProcesso(m.nome, "servidor",
                          (lambda m: lambda ctx: args_servidor(m, {"initial_peers": ctx.dados["multiaddr"]}))(m))
          for m in outros],
        *[AguardarEstado(m.nome, "servidor", "operando", prazo_inicio) for m in outros],
        Aguardar(p["espera_blocos_s"], "carregamento e anúncio dos blocos na DHT"),
        IniciarProcesso(cliente.nome, "cliente", lambda ctx: {
            **_args_comuns(config, cliente, p, "cliente"), "initial_peers": ctx.dados["multiaddr"],
            "repeticoes": p["repeticoes"], "max_new_tokens": p["max_new_tokens"], "pausa_s": p["pausa_s"]}),
        AguardarConclusao(cliente.nome, "cliente"),
        *[PararProcesso(m.nome, "servidor") for m in reversed(servidores)],
    ]
    limpeza = [PararProcesso(cliente.nome, "cliente"), *[PararProcesso(m.nome, "servidor") for m in servidores]]
    return comandos, limpeza


def plano_centralizado(config: Configuracao, p: dict):
    central = config.maquina(p.get("central") or config.com_papel("centralizado")[0].nome)
    tel = config.maquina(config.telemetria_maquina)
    envolvidas = list({m.nome: m for m in [tel, central]}.values())
    comandos = [
        ValidarSpecs(envolvidas),
        GarantirProcesso(tel.nome, "telemetria", {"porta": config.telemetria_porta}),
        IniciarProcesso(central.nome, "centralizado", {
            **_args_comuns(config, central, p, "centralizado"), "repeticoes": p["repeticoes"],
            "max_new_tokens": p["max_new_tokens"], "pausa_s": p["pausa_s"]}),
        AguardarConclusao(central.nome, "centralizado"),
    ]
    return comandos, [PararProcesso(central.nome, "centralizado")]


PLANOS = {"p2p": plano_p2p, "centralizado": plano_centralizado}


class Orquestrador:
    def __init__(self, config, agentes, telemetria, barramento):
        self.config, self.agentes, self.telemetria, self.barramento = config, agentes, telemetria, barramento
        self._lock = threading.Lock()
        self._ctx = None
        self.estado = EstadoExperimento.OCIOSO
        self.cenario = None
        self.parametros = {}
        self.historico = []
        self.erro = None
        self.iniciado_em = self.finalizado_em = None

    def iniciar(self, cenario, parametros):
        if cenario not in PLANOS:
            raise ParametrosInvalidos(f"Cenário desconhecido: {cenario}")
        p = validar_parametros(self.config, parametros, cenario)
        comandos, limpeza = PLANOS[cenario](self.config, p)
        with self._lock:
            if self.estado == EstadoExperimento.EXECUTANDO:
                raise ParametrosInvalidos("Já existe um experimento em execução.")
            self._ctx = Contexto(self.config, self.agentes, self.telemetria, p)
            self.estado, self.cenario, self.parametros, self.erro = EstadoExperimento.EXECUTANDO, cenario, p, None
            self.historico = [{"descricao": c.descricao, "status": "pendente", "detalhe": None,
                               "inicio": None, "fim": None} for c in comandos]
            self.iniciado_em, self.finalizado_em = time.time(), None
        self._publicar()
        threading.Thread(target=self._executar, args=(self._ctx, comandos, limpeza), daemon=True).start()
        return self.instantaneo()

    def cancelar(self):
        with self._lock:
            if self._ctx and self.estado == EstadoExperimento.EXECUTANDO:
                self._ctx.cancelado.set()
        return self.instantaneo()

    def _executar(self, ctx, comandos, limpeza):
        final, erro = EstadoExperimento.CONCLUIDO, None
        for i, cmd in enumerate(comandos):
            passo = self.historico[i]
            passo.update(status="executando", inicio=time.time())
            self._publicar()
            try:
                passo["detalhe"] = cmd.executar(ctx)
                passo["status"] = "ok"
            except Cancelado:
                passo["status"], final = "cancelado", EstadoExperimento.CANCELADO
            except Exception as e:  # FalhaComando ou erro inesperado: registra e interrompe
                passo["status"], passo["detalhe"] = "falhou", str(e)
                final, erro = EstadoExperimento.FALHOU, str(e)
            passo["fim"] = time.time()
            self._publicar()
            if passo["status"] != "ok":
                break
        if final != EstadoExperimento.CONCLUIDO:
            for cmd in limpeza:  # não deixa processos órfãos nas máquinas
                try:
                    cmd.executar(ctx)
                except Exception:
                    pass
        with self._lock:
            self.estado, self.erro, self.finalizado_em = final, erro, time.time()
        self._publicar()

    def instantaneo(self):
        return {"estado": self.estado.value, "cenario": self.cenario, "parametros": self.parametros,
                "historico": self.historico, "erro": self.erro, "multiaddr": (self._ctx.dados.get("multiaddr")
                if self._ctx else None), "iniciado_em": self.iniciado_em, "finalizado_em": self.finalizado_em}

    def _publicar(self):
        self.barramento.publicar("experimento", self.instantaneo())
