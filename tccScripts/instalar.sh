#!/usr/bin/env bash
# Prepara ESTA máquina para participar do experimento (rodar uma vez em cada PC).
#
#   DRIFT_FONTE=<origem do Drift> ./instalar.sh
#
# DRIFT_FONTE aceita o mesmo que o pip: URL git, pasta local ou arquivo .whl, por exemplo
#   DRIFT_FONTE="git+https://github.com/felipe-nonato/DRIFT-LLM"     (padrão)
#   DRIFT_FONTE="$HOME/DRIFT-LLM"                                    (cópia local do código)
#   DRIFT_FONTE="/media/pendrive/drift_llm-*.whl"                    (pacote copiado de outro nó)
# Use DRIFT_FONTE=nenhum para não instalar o Drift (a máquina só poderá ser 'telemetria'/'centralizado').
set -euo pipefail
cd "$(dirname "$0")"

PY_VERSAO="${PY_VERSAO:-3.11}"   # as máquinas do experimento usam 3.11; o Drift exige >= 3.10

achar_python() {
  if command -v "python${PY_VERSAO}" >/dev/null 2>&1; then command -v "python${PY_VERSAO}"; return; fi
  if command -v mise >/dev/null 2>&1; then
    mise install "python@${PY_VERSAO}" >&2
    echo "$(mise where "python@${PY_VERSAO}")/bin/python3"; return
  fi
  if command -v uv >/dev/null 2>&1; then
    uv python install "${PY_VERSAO}" >&2
    uv python find "${PY_VERSAO}"; return
  fi
  echo "ERRO: Python ${PY_VERSAO} não encontrado. Instale python${PY_VERSAO}, mise ou uv." >&2
  exit 1
}

if [ ! -x .venv/bin/python ]; then
  PY="$(achar_python)"
  echo "==> Criando .venv com $("$PY" --version)"
  "$PY" -m venv .venv
fi
V=.venv/bin/python
"$V" -m pip install -q --upgrade pip

echo "==> PyTorch (somente CPU)"
# versão exigida pelo Drift (torch>=2.6,<2.7), da build CPU — evita baixar a build CUDA (~2,5 GB)
"$V" -m pip install -q "torch>=2.6,<2.7" --index-url https://download.pytorch.org/whl/cpu
echo "==> Dependências do projeto"
"$V" -m pip install -q -r requirements.txt

DRIFT_FONTE="${DRIFT_FONTE:-git+https://github.com/felipe-nonato/DRIFT-LLM}"
if [ "${DRIFT_FONTE}" != "nenhum" ]; then
  echo "==> Drift LLM a partir de: ${DRIFT_FONTE}"
  # shellcheck disable=SC2086
  "$V" -m pip install -q ${DRIFT_FONTE}
else
  echo "!!  Drift NÃO instalado (DRIFT_FONTE=nenhum): papéis servidor/cliente indisponíveis."
fi

echo "==> Verificação"
"$V" - <<'EOF'
import importlib.util, platform
for m in ("psutil", "requests", "torch", "transformers", "drift"):
    print(f"  {m:<13} {'ok' if importlib.util.find_spec(m) else 'AUSENTE'}")
print(f"  python        {platform.python_version()}")
EOF
echo
echo "==> Especificações desta máquina"
"$V" -m bancada.specs --interface "${INTERFACE:-$(ip -o route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<NF;i++) if($i=="dev") print $(i+1)}')}" || {
  echo "!!  Corrija os itens acima (ex.: liberar RAPL) antes de iniciar o agente."; }
echo
echo "Pronto. Inicie o agente com:  BANCADA_TOKEN=<segredo> ./iniciar_agente.sh"
