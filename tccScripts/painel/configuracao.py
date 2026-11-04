"""Leitura e validação de config/maquinas.json (inventário de máquinas do experimento)."""

import json
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from bancada import caminhos

PAPEIS_VALIDOS = {"telemetria", "servidor", "cliente", "centralizado"}


class ConfiguracaoInvalida(Exception):
    pass


@dataclass(frozen=True)
class Maquina:
    nome: str
    agente: str
    interface: str = "tailscale0"
    papeis: tuple = ()
    num_blocks: Optional[int] = None
    tdp: Optional[float] = None
    sem_rapl: bool = False

    @property
    def host(self):
        return urlparse(self.agente).hostname

    def args_specs(self):
        """Argumentos de specs repassados a qualquer processo desta máquina."""
        return {"interface": self.interface, "tdp": self.tdp, "sem_rapl": self.sem_rapl}


@dataclass(frozen=True)
class Configuracao:
    modelo: str
    camadas: int
    telemetria_maquina: str
    telemetria_porta: int
    maquinas: tuple = field(default_factory=tuple)

    def maquina(self, nome) -> Maquina:
        for m in self.maquinas:
            if m.nome == nome:
                return m
        raise ConfiguracaoInvalida(f"Máquina inexistente: {nome}")

    def com_papel(self, papel):
        return [m for m in self.maquinas if papel in m.papeis]

    @property
    def telemetria_url(self):
        return f"http://{self.maquina(self.telemetria_maquina).host}:{self.telemetria_porta}"


def carregar(caminho=caminhos.MAQUINAS) -> Configuracao:
    if not caminho.exists():
        raise ConfiguracaoInvalida(f"Arquivo não encontrado: {caminho} (crie o inventário conforme o README)")
    d = json.loads(caminho.read_text(encoding="utf-8"))
    maquinas = []
    for item in d.get("maquinas", []):
        papeis = tuple(item.get("papeis", ()))
        invalidos = set(papeis) - PAPEIS_VALIDOS
        if invalidos:
            raise ConfiguracaoInvalida(f"{item.get('nome')}: papéis inválidos {sorted(invalidos)}")
        if not str(item.get("agente", "")).startswith("http"):
            raise ConfiguracaoInvalida(f"{item.get('nome')}: 'agente' deve ser uma URL http://IP:PORTA")
        maquinas.append(Maquina(
            nome=item["nome"], agente=item["agente"].rstrip("/"), interface=item.get("interface", "tailscale0"),
            papeis=papeis, num_blocks=item.get("num_blocks"), tdp=item.get("tdp"),
            sem_rapl=bool(item.get("sem_rapl", False)),
        ))
    nomes = [m.nome for m in maquinas]
    if len(nomes) != len(set(nomes)):
        raise ConfiguracaoInvalida("Nomes de máquina repetidos.")
    tel = d.get("telemetria", {})
    cfg = Configuracao(
        modelo=d.get("modelo", {}).get("nome", "Qwen/Qwen2.5-7B-Instruct"),
        camadas=int(d.get("modelo", {}).get("camadas", 28)),
        telemetria_maquina=tel.get("maquina"),
        telemetria_porta=int(tel.get("porta", 5000)),
        maquinas=tuple(maquinas),
    )
    if "telemetria" not in cfg.maquina(cfg.telemetria_maquina).papeis:
        raise ConfiguracaoInvalida(f"A máquina '{cfg.telemetria_maquina}' precisa do papel 'telemetria'.")
    for m in cfg.com_papel("servidor"):
        if not m.num_blocks:
            raise ConfiguracaoInvalida(f"{m.nome}: servidores precisam de 'num_blocks'.")
    return cfg
