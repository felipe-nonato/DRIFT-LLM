"""
Coleta e validação OBRIGATÓRIA das especificações de hardware de um nó.

Nenhum experimento começa sem que todas as specs necessárias ao cálculo
energético estejam resolvidas e com fonte rastreável:

  - modelo de CPU, núcleos, RAM total, identificador da máquina;
  - TDP da CPU (informado, tabela curada `tdp_cpus.json` ou base do CodeCarbon);
  - método de medição da CPU: RAPL (medido) ou modelo TDP x utilização (estimado);
  - interface de rede usada pelo enxame (para contar bytes) e seu IP;
  - ambiente não virtualizado (WSL/VM invalidam RAPL e utilização de CPU).

Uso direto:
    python specs.py                      # mostra as specs e valida
    python specs.py --calibrar           # (com RAPL) mede potência ociosa e em carga
    python specs.py --tdp 15 --interface tailscale0
"""

import argparse
import csv
import hashlib
import json
import multiprocessing as mp
import os
import platform
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

import psutil

from .caminhos import TABELA_TDP as TABELA_TDP_LOCAL

POWERCAP = Path("/sys/class/powercap")

INSTRUCAO_RAPL = (
    "Os contadores RAPL existem mas não podem ser lidos por este usuário.\n"
    "  Libere a leitura (vale até o próximo boot):\n"
    "    sudo chmod o+r /sys/class/powercap/intel-rapl:*/energy_uj "
    "/sys/class/powercap/intel-rapl:*:*/energy_uj\n"
    "  Ou, conscientemente, use --sem_rapl para cair no modelo estimado (TDP x utilização)."
)


class SpecsIncompletas(Exception):
    def __init__(self, problemas):
        self.problemas = problemas
        super().__init__("\n".join(f"  - {p}" for p in problemas))


# ---------------------------------------------------------------------------
# Leitura básica
# ---------------------------------------------------------------------------

def _ler(caminho):
    try:
        return Path(caminho).read_text().strip()
    except Exception:
        return None


def _cmd(args, timeout=5):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def modelo_cpu():
    info = _ler("/proc/cpuinfo") or ""
    m = re.search(r"^model name\s*:\s*(.+)$", info, re.M)
    if m:
        return m.group(1).strip()
    saida = _cmd(["lscpu"]) or ""
    m = re.search(r"^Model name:\s*(.+)$", saida, re.M)
    if m:
        return m.group(1).strip()
    return platform.processor() or None


def flags_cpu():
    info = _ler("/proc/cpuinfo") or ""
    m = re.search(r"^flags\s*:\s*(.+)$", info, re.M)
    flags = set(m.group(1).split()) if m else set()
    relevantes = ["avx", "avx2", "fma", "avx512f", "avx512_bf16", "amx_bf16", "f16c"]
    return {f: (f in flags) for f in relevantes}


def maquina_id():
    mid = _ler("/etc/machine-id") or _ler("/var/lib/dbus/machine-id")
    if not mid:
        macs = sorted(
            a.address
            for addrs in psutil.net_if_addrs().values()
            for a in addrs
            if getattr(a, "family", None) == psutil.AF_LINK and a.address not in ("", "00:00:00:00:00:00")
        )
        mid = socket.gethostname() + "|" + ",".join(macs)
    return hashlib.sha256(mid.encode()).hexdigest()[:16]


def virtualizacao():
    osrelease = (_ler("/proc/sys/kernel/osrelease") or "").lower()
    if "microsoft" in osrelease or "wsl" in osrelease:
        return "wsl"
    v = _cmd(["systemd-detect-virt"])
    if v and v != "none":
        return v
    return "nenhuma"


CHASSIS_NOTEBOOK = {"8", "9", "10", "11", "14", "30", "31", "32"}
CHASSIS_DESKTOP = {"3", "4", "5", "6", "7", "13", "15", "16", "24", "35", "36"}


def chassis():
    c = _ler("/sys/class/dmi/id/chassis_type")
    if c in CHASSIS_NOTEBOOK:
        return "notebook"
    if c in CHASSIS_DESKTOP:
        return "desktop"
    return f"desconhecido({c})"


def bateria():
    try:
        b = psutil.sensors_battery()
    except Exception:
        b = None
    if b is None:
        return None
    return {"percentual": b.percent, "na_tomada": b.power_plugged}


def modulos_ram():
    """Tipo/quantidade de pentes via dmidecode (requer root ou sudo sem senha)."""
    base = ["dmidecode", "-t", "17"]
    saida = _cmd(base) if os.geteuid() == 0 else _cmd(["sudo", "-n"] + base)
    if not saida:
        return None
    modulos = []
    for bloco in saida.split("Memory Device")[1:]:
        tam = re.search(r"^\s*Size:\s*(.+)$", bloco, re.M)
        tipo = re.search(r"^\s*Type:\s*(.+)$", bloco, re.M)
        vel = re.search(r"^\s*Speed:\s*(.+)$", bloco, re.M)
        if tam and "No Module" not in tam.group(1):
            modulos.append({
                "tamanho": tam.group(1).strip(),
                "tipo": tipo.group(1).strip() if tipo else None,
                "velocidade": vel.group(1).strip() if vel else None,
            })
    return modulos or None


# ---------------------------------------------------------------------------
# RAPL
# ---------------------------------------------------------------------------

def dominios_rapl():
    """
    Retorna os domínios RAPL úteis:
      - 'package-N' (zonas de topo)  -> energia da CPU
      - 'dram'      (subzonas)       -> energia da RAM, quando o hardware expõe
    Ignora 'psys' (plataforma inteira, contaria em dobro) e zonas '-mmio' (duplicatas).
    """
    dominios = []
    if not POWERCAP.exists():
        return dominios
    for zona in sorted(POWERCAP.glob("intel-rapl:*")):
        if "mmio" in zona.name:
            continue
        nome = _ler(zona / "name") or ""
        if nome.startswith("package"):
            tipo = "pkg"
        elif nome == "dram":
            tipo = "dram"
        else:
            continue
        arq = zona / "energy_uj"
        legivel = os.access(arq, os.R_OK) and _ler(arq) is not None
        maximo = _ler(zona / "max_energy_range_uj")
        dominios.append({
            "zona": zona.name,
            "nome": nome,
            "tipo": tipo,
            "arquivo": str(arq),
            "legivel": legivel,
            "max_uj": int(maximo) if maximo and maximo.isdigit() else None,
        })
    return dominios


# ---------------------------------------------------------------------------
# TDP
# ---------------------------------------------------------------------------

def _normalizar(nome):
    s = nome.lower()
    s = re.sub(r"\((r|tm)\)", " ", s)
    s = re.sub(r"\b\d+(st|nd|rd|th) gen\b", " ", s)
    s = re.sub(r"@.*$", " ", s)
    s = re.sub(r",.*$", " ", s)
    for termo in ("with radeon graphics", "radeon r7", "radeon graphics", "processor", "cpu", "apu"):
        s = s.replace(termo, " ")
    s = re.sub(r"\b\d+-core\b", " ", s)
    return " ".join(s.split())


def _token_modelo(nome_norm):
    """Número do modelo: 7200u, a8-7650k, 5500u, 4600g, e5-2680 ..."""
    toks = re.findall(r"\b(?:[a-z]\d-)?\d{3,5}[a-z]{0,3}\b", nome_norm)
    return toks[-1] if toks else None


def _base_codecarbon():
    try:
        import importlib.util
        spec = importlib.util.find_spec("codecarbon")  # localiza sem importar (evita dependências pesadas)
        caminho = Path(spec.origin).parent / "data" / "hardware" / "cpu_power.csv"
        linhas = []
        with open(caminho, newline="", encoding="utf-8") as f:
            for linha in csv.DictReader(f):
                try:  # a base tem linhas corrompidas (ex.: "27.29.5"); são ignoradas
                    linhas.append((linha["Name"], float(linha["TDP"])))
                except (TypeError, ValueError):
                    continue
        return linhas, str(caminho)
    except Exception:
        return None, None


def resolver_tdp(modelo, tdp_informado=None):
    """Retorna (tdp_w, fonte). Levanta ValueError se não for possível resolver com segurança."""
    if tdp_informado:
        return float(tdp_informado), "informado pelo usuário (--tdp)"

    alvo = _normalizar(modelo)

    if TABELA_TDP_LOCAL.exists():
        tabela = json.loads(TABELA_TDP_LOCAL.read_text())
        for item in tabela.get("cpus", []):
            if _normalizar(item["modelo"]) == alvo:
                return float(item["tdp_w"]), f"tdp_cpus.json: {item['fonte']}"

    linhas, caminho = _base_codecarbon()
    if linhas:
        exatos = {tdp for nome, tdp in linhas if _normalizar(nome) == alvo}
        if len(exatos) == 1:
            return exatos.pop(), f"base de TDP do CodeCarbon (correspondência exata) — {caminho}"
        token = _token_modelo(alvo)
        marca = alvo.split()[0] if alvo else ""
        if token:
            candidatos = {
                tdp for nome, tdp in linhas
                if _token_modelo(_normalizar(nome)) == token and _normalizar(nome).startswith(marca)
            }
            if len(candidatos) == 1:
                return candidatos.pop(), f"base de TDP do CodeCarbon (modelo '{token}') — {caminho}"
            if len(candidatos) > 1:
                raise ValueError(
                    f"TDP ambíguo para '{modelo}' na base do CodeCarbon: {sorted(candidatos)} W. "
                    "Informe com --tdp ou adicione em tdp_cpus.json."
                )

    raise ValueError(
        f"TDP não encontrado para '{modelo}'. Consulte a ficha do fabricante e informe com --tdp "
        "ou adicione uma entrada (com fonte) em tdp_cpus.json."
    )


# ---------------------------------------------------------------------------
# Rede
# ---------------------------------------------------------------------------

def interface_rede(preferida):
    enderecos = psutil.net_if_addrs()
    if preferida not in enderecos:
        return None, None, sorted(enderecos)
    ipv4 = next((a.address for a in enderecos[preferida] if a.family == socket.AF_INET), None)
    return preferida, ipv4, sorted(enderecos)


# ---------------------------------------------------------------------------
# Coleta + validação
# ---------------------------------------------------------------------------

def coletar(tdp=None, interface="tailscale0", sem_rapl=False, permitir_virtualizado=False):
    """Coleta todas as specs. Levanta SpecsIncompletas se faltar algo obrigatório."""
    problemas, avisos = [], []

    cpu = modelo_cpu()
    if not cpu:
        problemas.append("Modelo da CPU não identificado (/proc/cpuinfo e lscpu falharam).")

    logicos = psutil.cpu_count(logical=True)
    fisicos = psutil.cpu_count(logical=False)
    if not logicos:
        problemas.append("Número de núcleos lógicos não identificado.")

    ram_gb = psutil.virtual_memory().total / 1024**3
    if ram_gb <= 0:
        problemas.append("RAM total não identificada.")

    tdp_w, tdp_fonte = None, None
    if cpu:
        try:
            tdp_w, tdp_fonte = resolver_tdp(cpu, tdp)
        except ValueError as e:
            problemas.append(str(e))

    virt = virtualizacao()
    if virt != "nenhuma" and not permitir_virtualizado:
        problemas.append(
            f"Ambiente virtualizado detectado ({virt}). RAPL e utilização de CPU não refletem o hardware "
            "físico. Rode nativamente ou use --permitir_virtualizado (registrado como limitação)."
        )

    rapl = dominios_rapl()
    rapl_pkg = [d for d in rapl if d["tipo"] == "pkg"]
    rapl_dram = [d for d in rapl if d["tipo"] == "dram"]
    if sem_rapl:
        metodo_cpu = "modelo_tdp"
        avisos.append("RAPL desativado por opção (--sem_rapl): energia da CPU será ESTIMADA.")
    elif rapl_pkg and all(d["legivel"] for d in rapl_pkg):
        metodo_cpu = "rapl"
    elif rapl_pkg:
        metodo_cpu = None
        problemas.append(INSTRUCAO_RAPL)
    else:
        metodo_cpu = "modelo_tdp"
        avisos.append("CPU sem RAPL: energia da CPU será ESTIMADA (TDP x utilização).")

    usar_dram = metodo_cpu == "rapl" and rapl_dram and all(d["legivel"] for d in rapl_dram)
    metodo_ram = "rapl_dram" if usar_dram else "estimado_por_gb"

    iface, ip, disponiveis = interface_rede(interface)
    if not iface:
        problemas.append(
            f"Interface de rede '{interface}' não existe. Disponíveis: {', '.join(disponiveis)}. "
            "Use --interface."
        )
    elif not ip:
        problemas.append(f"Interface '{interface}' sem endereço IPv4.")

    bat = bateria()
    if bat and not bat["na_tomada"]:
        avisos.append("Notebook na BATERIA: o governador de energia pode reduzir o desempenho. Ligue na tomada.")

    ram_mod = modulos_ram()
    if ram_mod is None:
        avisos.append("Tipo/quantidade de pentes de RAM indisponível (dmidecode exige root). Apenas informativo.")

    governador = _ler("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    freq = psutil.cpu_freq()

    specs = {
        "coletado_em": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "hostname": socket.gethostname(),
        "maquina_id": maquina_id(),
        "so": platform.platform(),
        "python": platform.python_version(),
        "virtualizacao": virt,
        "chassis": chassis(),
        "bateria": bat,
        "cpu_modelo": cpu,
        "cpu_nucleos_fisicos": fisicos,
        "cpu_nucleos_logicos": logicos,
        "cpu_freq_max_mhz": freq.max if freq else None,
        "cpu_governador": governador,
        "cpu_flags": flags_cpu(),
        "tdp_w": tdp_w,
        "tdp_fonte": tdp_fonte,
        "ram_total_gb": round(ram_gb, 3),
        "ram_modulos": ram_mod,
        "metodo_cpu": metodo_cpu,
        "metodo_ram": metodo_ram,
        "rapl_dominios": [d for d in rapl if (d["tipo"] == "pkg" or (d["tipo"] == "dram" and usar_dram))]
        if metodo_cpu == "rapl" else [],
        "rede_interface": iface,
        "rede_ip": ip,
        "avisos": avisos,
    }
    if problemas:
        raise SpecsIncompletas(problemas)
    return specs


def imprimir(specs):
    print("=" * 64)
    print(f" ESPECIFICAÇÕES DO NÓ — {specs['hostname']} ({specs['maquina_id']})")
    print("=" * 64)
    print(f" CPU            : {specs['cpu_modelo']}")
    print(f" Núcleos        : {specs['cpu_nucleos_fisicos']} físicos / {specs['cpu_nucleos_logicos']} lógicos")
    print(f" TDP            : {specs['tdp_w']} W  [{specs['tdp_fonte']}]")
    print(f" RAM            : {specs['ram_total_gb']:.2f} GB")
    print(f" Medição CPU    : {specs['metodo_cpu']}")
    print(f" Medição RAM    : {specs['metodo_ram']}")
    print(f" Rede           : {specs['rede_interface']} ({specs['rede_ip']})")
    print(f" Virtualização  : {specs['virtualizacao']} | chassis: {specs['chassis']}")
    flags = ", ".join(f for f, v in specs["cpu_flags"].items() if v) or "nenhuma"
    print(f" Flags SIMD     : {flags}")
    for a in specs["avisos"]:
        print(f" ⚠️  {a}")
    print("=" * 64)


# ---------------------------------------------------------------------------
# Calibração (só com RAPL)
# ---------------------------------------------------------------------------

def _ler_pkg_uj(dominios):
    return [int(Path(d["arquivo"]).read_text()) for d in dominios]


def _potencia_rapl(dominios, segundos):
    ini, t0 = _ler_pkg_uj(dominios), time.time()
    time.sleep(segundos)
    fim, t1 = _ler_pkg_uj(dominios), time.time()
    total = 0
    for a, b, d in zip(ini, fim, dominios):
        delta = b - a
        if delta < 0 and d["max_uj"]:
            delta += d["max_uj"]
        total += delta
    return total / 1e6 / (t1 - t0)


def _queimar(fim):
    x = 0
    while time.time() < fim:
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF


def calibrar(specs, segundos=20):
    dominios = [d for d in specs["rapl_dominios"] if d["tipo"] == "pkg"]
    if specs["metodo_cpu"] != "rapl" or not dominios:
        raise SystemExit("Calibração exige RAPL legível neste nó.")
    print(f"Medindo potência OCIOSA por {segundos}s (feche outros programas)...")
    p_ocioso = _potencia_rapl(dominios, segundos)
    print(f"Medindo potência em CARGA TOTAL ({specs['cpu_nucleos_logicos']} processos) por {segundos}s...")
    fim = time.time() + segundos + 2
    procs = [mp.Process(target=_queimar, args=(fim,)) for _ in range(specs["cpu_nucleos_logicos"])]
    for p in procs:
        p.start()
    time.sleep(1)
    p_carga = _potencia_rapl(dominios, segundos)
    for p in procs:
        p.join()
    resultado = {
        "p_ociosa_w": round(p_ocioso, 3),
        "p_carga_total_w": round(p_carga, 3),
        "fracao_ociosa": round(p_ocioso / p_carga, 4) if p_carga else None,
        "razao_carga_tdp": round(p_carga / specs["tdp_w"], 4) if specs["tdp_w"] else None,
        "duracao_s": segundos,
        "em": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    print(json.dumps(resultado, indent=2))
    print("→ 'fracao_ociosa' pode alimentar o parâmetro fracao_ociosa_cpu (nós sem RAPL), citando este nó.")
    return resultado


def argumentos_specs(parser):
    parser.add_argument("--tdp", type=float, help="TDP da CPU em W (sobrepõe as tabelas)")
    parser.add_argument("--interface", default="tailscale0", help="Interface de rede do enxame (padrão: tailscale0)")
    parser.add_argument("--sem_rapl", action="store_true", help="Força o modelo estimado mesmo havendo RAPL")
    parser.add_argument("--permitir_virtualizado", action="store_true", help="Aceita WSL/VM (registrado como limitação)")


def coletar_ou_sair(args):
    try:
        specs = coletar(args.tdp, args.interface, args.sem_rapl, args.permitir_virtualizado)
    except SpecsIncompletas as e:
        print("❌ Especificações obrigatórias incompletas — o experimento NÃO será iniciado:\n" + str(e))
        sys.exit(2)
    if args.permitir_virtualizado and specs["virtualizacao"] != "nenhuma":
        specs["avisos"].append(f"Executado em ambiente virtualizado ({specs['virtualizacao']}) por opção explícita.")
    return specs


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Specs obrigatórias do nó (TCC P2P x centralizado)")
    argumentos_specs(p)
    p.add_argument("--calibrar", action="store_true", help="Mede potência ociosa/carga via RAPL")
    p.add_argument("--salvar", help="Salva as specs em JSON neste caminho")
    a = p.parse_args()
    s = coletar_ou_sair(a)
    imprimir(s)
    if a.calibrar:
        s["calibracao"] = calibrar(s)
    if a.salvar:
        Path(a.salvar).write_text(json.dumps(s, indent=2, ensure_ascii=False))
        print(f"Specs salvas em {a.salvar}")
