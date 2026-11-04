"""
Nó servidor do enxame P2P (Drift LLM) com medição contínua.

Substitui o antigo medirEnergia.py. Fluxo:
  1. coleta e valida as specs obrigatórias (aborta se faltar algo);
  2. confere se os blocos cabem na RAM (evita swap);
  3. registra a máquina na API, mede a linha de base OCIOSA;
  4. inicia o servidor Drift e envia amostras a cada poucos segundos até o fim.

Exemplos:
  # nó principal (bootstrap da DHT), na máquina que também roda a API:
  python -m bancada.no_p2p --api http://100.78.50.45:5000 --experimento exp01 --no_id no1 \
      --bootstrap --num_blocks 10 --torch_dtype bfloat16

  # demais nós:
  python -m bancada.no_p2p --api http://100.78.50.45:5000 --experimento exp01 --no_id no2 \
      --initial_peers /ip4/100.78.50.45/tcp/31337/p2p/<PEER_ID> --num_blocks 10 --torch_dtype bfloat16

Ao detectar o próprio PEER_ID na saída do Drift, o nó publica seu multiaddr na API
(POST /sessoes/<id>/anunciar) e imprime a linha "[MULTIADDR] ...". Essa é a fonte
única do endereço de conexão usada pelo painel e pelos demais nós.

Argumentos depois de "--" são repassados ao drift.cli.run_server.
OBS: flags do run_server seguem o Petals (do qual o Drift é fork); conferir com
`python -m drift.cli.run_server --help` na versão instalada.
"""

import argparse
import json
import re
import signal
import subprocess
import sys
import threading

import requests

from . import blocos, caminhos, experimento
from . import specs as specs_mod
from .medidor import DIR_LOGS, SessaoMedida

# PEER_ID libp2p: Ed25519 ("12D3KooW...") ou RSA ("Qm..."), sempre após "/p2p/"
PADRAO_PEER_ID = re.compile(r"/p2p/(12D3KooW[1-9A-HJ-NP-Za-km-z]{40,50}|Qm[1-9A-HJ-NP-Za-km-z]{44})\b")


def parse_args():
    p = argparse.ArgumentParser(description="Nó P2P (Drift LLM) com medição energética")
    specs_mod.argumentos_specs(p)
    p.add_argument("--api", required=True, help="URL da API de telemetria, ex.: http://100.78.50.45:5000")
    p.add_argument("--experimento", required=True, help="Rótulo do experimento (igual em todos os nós e no cliente)")
    p.add_argument("--no_id", required=True, help="Identificador ÚNICO deste nó (ex.: no1, no2)")
    p.add_argument("--modelo", default="Qwen/Qwen2.5-7B-Instruct")
    p.add_argument("--torch_dtype", required=True, choices=list(experimento.DTYPES),
                   help="Precisão dos pesos — DEVE ser a mesma do cenário centralizado")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--num_blocks", type=int, help="Quantidade de blocos Transformer servidos")
    g.add_argument("--block_indices", help="Intervalo explícito de blocos, ex.: 0:10")
    r = p.add_mutually_exclusive_group(required=True)
    r.add_argument("--bootstrap", action="store_true", help="Este nó cria o enxame (nó principal da DHT)")
    r.add_argument("--initial_peers", help="Multiaddr do nó principal: /ip4/IP/tcp/PORTA/p2p/PEER_ID")
    p.add_argument("--porta", type=int, default=31337, help="Porta P2P deste nó (padrão: 31337)")
    p.add_argument("--identity_path", help="Arquivo de identidade libp2p (mantém o PEER_ID entre execuções)")
    p.add_argument("--session_timeout", type=float, default=7200.0)
    p.add_argument("--step_timeout", type=float, default=900.0)
    p.add_argument("--baseline_s", type=float, help="Sobrepõe a duração da linha de base da config")
    p.add_argument("--forcar_memoria", action="store_true", help="Prossegue mesmo se os pesos não couberem na RAM")
    p.add_argument("--plano", help="JSON do planejamento de blocos (gravado nos extras da sessão)")
    p.add_argument("--permitir_sobreposicao", action="store_true",
                   help="Aceita um intervalo de blocos que outro servidor da rede já serve (réplica)")
    p.add_argument("extra", nargs=argparse.REMAINDER, help="Após '--': argumentos extras do run_server")
    return p.parse_args()


def numero_blocos(args):
    if args.num_blocks:
        return args.num_blocks
    ini, fim = (int(x) for x in args.block_indices.split(":"))
    return fim - ini


def montar_comando(args, ip):
    identidade = args.identity_path or str(caminhos.DIR_IDENTIDADES / f"identidade_{args.no_id}.key")
    cmd = [
        sys.executable, "-m", "drift.cli.run_server", args.modelo,
        "--device", "cpu",
        "--torch_dtype", args.torch_dtype,
        "--host_maddrs", f"/ip4/0.0.0.0/tcp/{args.porta}",
        "--announce_maddrs", f"/ip4/{ip}/tcp/{args.porta}",
        "--identity_path", identidade,
        "--no_auto_relay",
        "--session_timeout", str(args.session_timeout),
        "--step_timeout", str(args.step_timeout),
    ]
    cmd += ["--num_blocks", str(args.num_blocks)] if args.num_blocks else ["--block_indices", args.block_indices]
    cmd += ["--new_swarm"] if args.bootstrap else ["--initial_peers", args.initial_peers]
    extra = [a for a in (args.extra or []) if a != "--"]
    return cmd + extra


def anunciar(sessao, multiaddr):
    print(f"[MULTIADDR] {multiaddr}", flush=True)
    try:
        r = requests.post(f"{sessao.api}/sessoes/{sessao.id}/anunciar", json={"multiaddr": multiaddr}, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"[{sessao.no_id}] ⚠️ não foi possível publicar o multiaddr na API: {e}", flush=True)


def espelhar_saida(proc, arquivo, sessao, ip, porta):
    """Copia a saída do Drift para o terminal e o log; publica o multiaddr ao achar o PEER_ID."""
    anunciado = False
    with open(arquivo, "a", encoding="utf-8") as log:
        for linha in proc.stdout:
            sys.stdout.write(linha)
            sys.stdout.flush()
            log.write(linha)
            log.flush()
            if not anunciado:
                m = PADRAO_PEER_ID.search(linha)
                if m:
                    anunciar(sessao, f"/ip4/{ip}/tcp/{porta}/p2p/{m.group(1)}")
                    anunciado = True


def checar_intervalo(args):
    """Recusa servir camadas que outro servidor aberto deste experimento já serve."""
    if not args.block_indices:
        print(f"[{args.no_id}] ⚠️  --num_blocks: o Drift escolhe QUAIS blocos servir e pode rebalancear durante "
              "o experimento, mudando a cadeia entre consultas. Prefira --block_indices.", flush=True)
        return
    try:
        ini, fim = blocos.ler_intervalo(args.block_indices)
    except ValueError as e:
        raise SystemExit(f"❌ {e}. Use INI:FIM, ex.: 0:8")
    try:
        r = requests.get(f"{args.api}/sessoes", params={"experimento": args.experimento}, timeout=10)
        r.raise_for_status()
        fixos, _ = blocos.ocupados(r.json().get("sessoes", []))
    except Exception as e:
        print(f"[{args.no_id}] ⚠️  não foi possível conferir os blocos dos outros servidores: {e}", flush=True)
        return
    outros = blocos.sobrepostos(ini, fim, [x for x in fixos if x[2] != args.no_id])
    if outros and not args.permitir_sobreposicao:
        raise SystemExit(f"❌ Os blocos {args.block_indices} já são servidos por {', '.join(outros)}. Réplicas criam "
                         "rotas alternativas e a cadeia muda entre consultas. Escolha outro intervalo ou use "
                         "--permitir_sobreposicao.")


def main():
    args = parse_args()
    specs = specs_mod.coletar_ou_sair(args)
    specs_mod.imprimir(specs)
    checar_intervalo(args)

    n_blocos = numero_blocos(args)
    mem_gb, total_camadas = experimento.estimar_memoria_gb(args.modelo, args.torch_dtype, n_blocos)
    experimento.checar_memoria(specs, mem_gb, args.forcar_memoria, f"{n_blocos}/{total_camadas} blocos")

    cmd = montar_comando(args, specs["rede_ip"])
    sessao = SessaoMedida(
        args.api, specs, args.experimento, "p2p", "servidor", args.no_id,
        extras={"modelo": args.modelo, "torch_dtype": args.torch_dtype, "num_blocos": n_blocos,
                "block_indices": args.block_indices, "total_camadas": total_camadas,
                "bootstrap": args.bootstrap, "comando": cmd,
                "plano_blocos": json.loads(args.plano) if args.plano else None},
    )

    proc = None
    motivo = "processo_drift_finalizado"

    def ao_sinal(signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, ao_sinal)
    try:
        sessao.iniciar(args.baseline_s)
        caminhos.garantir_diretorios()
        log = DIR_LOGS / f"drift_{args.no_id}_{sessao.id}.log"
        print(f"[{args.no_id}] Iniciando servidor Drift (log: {log}):\n  {' '.join(cmd)}")
        if args.bootstrap:
            print(f"[{args.no_id}] Procure o PEER_ID no log ('.../p2p/12D3Koo...') e passe aos outros nós.")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        threading.Thread(target=espelhar_saida, args=(proc, log, sessao, specs["rede_ip"], args.porta),
                         daemon=True).start()
        codigo = proc.wait()
        if codigo != 0:
            motivo = f"drift_saiu_com_codigo_{codigo}"
            print(f"[{args.no_id}] ❌ O servidor Drift terminou com código {codigo}. Veja {log}.")
    except KeyboardInterrupt:
        motivo = "interrompido_pelo_usuario"
        print(f"\n[{args.no_id}] Encerrando...")
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
    finally:
        sessao.encerrar(motivo)


if __name__ == "__main__":
    main()
