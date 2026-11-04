#!/usr/bin/env bash
# Inicia o agente desta máquina usando o ambiente criado por ./instalar.sh.
#
#   BANCADA_TOKEN=<segredo> ./iniciar_agente.sh            # IP da interface de rede padrão, porta 7000
#   BANCADA_TOKEN=<segredo> INTERFACE=tailscale0 ./iniciar_agente.sh
set -euo pipefail
cd "$(dirname "$0")"

[ -x .venv/bin/python ] || { echo "ERRO: rode ./instalar.sh primeiro."; exit 1; }
[ -n "${BANCADA_TOKEN:-}" ] || { echo "ERRO: defina BANCADA_TOKEN (o mesmo segredo em todas as máquinas)."; exit 1; }

INTERFACE="${INTERFACE:-$(ip -o route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<NF;i++) if($i=="dev") print $(i+1)}')}"
export BANCADA_TOKEN
exec .venv/bin/python -m painel.agente --interface "${INTERFACE}" --porta "${PORTA:-7000}"
