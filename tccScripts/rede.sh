#!/usr/bin/env bash
# Página da rede P2P — o mesmo comando em TODOS os PCs:
#   - no PC central: "Criar a rede aqui" e copie o ID;
#   - nos outros PCs: "Entrar numa rede" e cole o ID.
#
#   ./rede.sh                 # http://127.0.0.1:8090
#   PORTA=8091 ./rede.sh
set -euo pipefail
cd "$(dirname "$0")"

[ -x .venv/bin/python ] || { echo "==> Primeira vez neste PC: instalando o ambiente"; ./instalar.sh; }

# Checagens (só avisam; nada é alterado no sistema por este script)
RAPL=$(ls /sys/class/powercap/intel-rapl:*/energy_uj 2>/dev/null | head -1 || true)
if [ -n "${RAPL}" ] && [ ! -r "${RAPL}" ]; then
  echo "!!  RAPL sem permissão de leitura (vale até o próximo boot). Rode:"
  echo "      sudo chmod o+r /sys/class/powercap/intel-rapl:*/energy_uj /sys/class/powercap/intel-rapl:*:*/energy_uj"
  echo "    ou marque 'sem RAPL' em Avançado na página."
fi
if command -v ufw >/dev/null 2>&1 && systemctl is-active --quiet ufw 2>/dev/null; then
  echo "i   Firewall ufw ativo: as portas 5000/tcp (telemetria) e 31337/tcp (P2P) precisam estar liberadas:"
  echo "      sudo ufw allow 5000/tcp && sudo ufw allow 31337/tcp"
fi

PORTA="${PORTA:-8090}"
URL="http://127.0.0.1:${PORTA}"
( sleep 1.5; command -v xdg-open >/dev/null 2>&1 && xdg-open "${URL}" >/dev/null 2>&1 || true ) &
exec .venv/bin/python -m painel.rede --porta "${PORTA}"
