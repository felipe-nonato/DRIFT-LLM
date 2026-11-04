"""Caminhos únicos do projeto (configuração versionada × dados gerados)."""

from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DIR_CONFIG = RAIZ / "config"
DIR_DADOS = RAIZ / "dados"
DIR_LOGS = DIR_DADOS / "logs"
DIR_RESULTADOS = DIR_DADOS / "resultados"
DIR_IDENTIDADES = DIR_DADOS / "identidades"

CONFIG_TCC = DIR_CONFIG / "config_tcc.json"
TABELA_TDP = DIR_CONFIG / "tdp_cpus.json"
PROMPTS = DIR_CONFIG / "prompts.json"
MAQUINAS = DIR_CONFIG / "maquinas.json"
BANCO = DIR_DADOS / "experimentos.db"


def garantir_diretorios():
    for d in (DIR_DADOS, DIR_LOGS, DIR_RESULTADOS, DIR_IDENTIDADES):
        d.mkdir(parents=True, exist_ok=True)
