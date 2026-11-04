#!/usr/bin/env bash
# Página da rede P2P — o mesmo comando em TODOS os PCs:
#   - no PC central: "Criar a rede aqui" e copie o ID;
#   - nos outros PCs: "Entrar numa rede" e cole o ID.
#
#   ./rede.sh                 # http://127.0.0.1:8090
#   PORTA=8091 ./rede.sh
#   CACHE=/mnt/windows/drift-cache ./rede.sh   # pesos do modelo num disco de verdade (live USB)
#   REDE=lan ./rede.sh        # aceita rede local (padrão: só Tailscale, em todos os PCs)
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
if [ "${REDE:-tailscale}" = "lan" ] && command -v ufw >/dev/null 2>&1 && systemctl is-active --quiet ufw 2>/dev/null; then
  echo "i   Firewall ufw ativo: as portas 5000/tcp (telemetria) e 31337/tcp (P2P) precisam estar liberadas:"
  echo "      sudo ufw allow 5000/tcp && sudo ufw allow 31337/tcp"
fi

# Onde ficam os pesos do modelo (~15 GiB em bfloat16). Num live USB a "home" fica num overlay
# em RAM: baixar os pesos lá consome a própria RAM dos blocos e o espaço acaba no meio do download.
if [ -n "${CACHE:-}" ]; then
  mkdir -p "${CACHE}/drift" "${CACHE}/huggingface"
  export DRIFT_CACHE="${CACHE}/drift" HF_HOME="${CACHE}/huggingface"
  echo "i   Cache do modelo em ${CACHE} (DRIFT_CACHE e HF_HOME)"
fi
DIR_CACHE="${DRIFT_CACHE:-${HOME}/.cache/drift}"
mkdir -p "${DIR_CACHE}"
FS_CACHE=$(df --output=fstype "${DIR_CACHE}" | tail -1 | tr -d ' ')
LIVRE_GIB=$(( $(df --output=avail -k "${DIR_CACHE}" | tail -1) / 1024 / 1024 ))
if [ "${FS_CACHE}" = "overlay" ] || [ "${FS_CACHE}" = "tmpfs" ]; then
  echo "!!  O cache do modelo (${DIR_CACHE}) está em ${FS_CACHE}, ou seja, na RAM (sistema live?)."
  echo "    Os pesos ocupariam a RAM que os blocos precisam e o espaço acaba no download."
  echo "    Monte um disco (partição do Windows, HD externo) e rode:  CACHE=/ponto/de/montagem/drift-cache ./rede.sh"
fi
if [ "${LIVRE_GIB}" -lt 16 ]; then
  echo "!!  Só ${LIVRE_GIB} GiB livres em ${DIR_CACHE}; o modelo completo precisa de ~16 GiB."
fi

# Tailscale: todos os PCs precisam dele, na mesma tailnet. Na rede local, um PC atrás de outro
# roteador (outro Wi-Fi) consegue sair até o nó central, mas ninguém consegue chegar até ele.
REDE="${REDE:-tailscale}"
if [ "${REDE}" = "tailscale" ]; then
  if ! command -v tailscale >/dev/null 2>&1; then
    echo "xx  Tailscale não instalado (num live USB, instale a cada boot). Rode:"
    echo "      curl -fsSL https://tailscale.com/install.sh | sh && sudo tailscale up"
    echo "    e abra o link de login com a MESMA conta dos outros PCs. (Para rede local: REDE=lan ./rede.sh)"
    exit 1
  fi
  IP_TS=$(tailscale ip -4 2>/dev/null | head -1 || true)
  if [ -z "${IP_TS}" ]; then
    echo "xx  Tailscale instalado, mas desconectado. Rode:  sudo tailscale up"
    echo "    e abra o link de login com a MESMA conta dos outros PCs."
    exit 1
  fi
  echo "i   Tailscale conectado: ${IP_TS} ($(hostname))"
  if command -v ufw >/dev/null 2>&1 && systemctl is-active --quiet ufw 2>/dev/null; then
    echo "i   ufw ativo: libere o tráfego do Tailscale (uma vez):  sudo ufw allow in on tailscale0"
  fi
  ARG_REDE="tailscale"
else
  ARG_REDE="qualquer"
fi

PORTA="${PORTA:-8090}"
URL="http://127.0.0.1:${PORTA}"
( sleep 1.5; command -v xdg-open >/dev/null 2>&1 && xdg-open "${URL}" >/dev/null 2>&1 || true ) &
exec .venv/bin/python -m painel.rede --porta "${PORTA}" --rede "${ARG_REDE}"
