"""
Cálculo ambiental — o ÚNICO lugar onde parâmetros são aplicados.

Para cada consulta (janela [t0, t1] no relógio da API):

  1. Energia de TI por máquina (CPU + RAM), integrando os intervalos que
     cruzam a janela (sobreposição proporcional):
        CPU: RAPL medido, ou  P = TDP · (f + (1 − f) · u)        (estimado)
        RAM: RAPL dram,  ou  P = ram_w_por_gb · RAM_total         (estimado)
  2. Linha de base ociosa da máquina (fase 'baseline') -> energia marginal.
  3. E_TI = Σ máquinas (bruta ou marginal, conforme config).
  4. E_total = E_TI · PUE + E_rede,  E_rede = GB_enviados · k_rede  (se o cenário contar rede)
  5. CO2 = E_total · CIF;  Água = E_TI · PUE · WUE  (+ E_total · EWIF, se habilitado)
  6. Normalização por token gerado.
"""

import json
import statistics
from pathlib import Path

from .caminhos import CONFIG_TCC as CONFIG_PADRAO


class ParametroInvalido(Exception):
    pass


def carregar_config(caminho=CONFIG_PADRAO):
    cfg = json.loads(Path(caminho).read_text(encoding="utf-8"))
    for nome, p in cfg["parametros"].items():
        if "valor" not in p or "fonte" not in p:
            raise ParametroInvalido(f"Parâmetro '{nome}' sem 'valor' ou 'fonte'.")
    for cen, c in cfg["cenarios"].items():
        for nome in ("pue", "wue_l_por_kwh"):
            if nome not in c or "fonte" not in c[nome]:
                raise ParametroInvalido(f"Cenário '{cen}': '{nome}' sem valor/fonte.")
    if cfg["contabilizacao"]["base_energia"] not in ("bruta", "marginal"):
        raise ParametroInvalido("contabilizacao.base_energia deve ser 'bruta' ou 'marginal'.")
    return cfg


def _usar(param, nome, usados):
    if "PREENCHER" in param["fonte"]:
        raise ParametroInvalido(f"Parâmetro '{nome}' está em uso mas sem fonte (PREENCHER).")
    usados[nome] = {"valor": param["valor"], "fonte": param["fonte"]}
    return param["valor"]


def energia_intervalo(iv, specs, params, usados):
    """Energia (J) de CPU e RAM de um intervalo inteiro."""
    dt = iv["t_fim"] - iv["t_ini"]
    if iv.get("e_pkg_j") is not None:
        e_cpu = iv["e_pkg_j"]
    else:
        f = _usar(params["fracao_ociosa_cpu"], "fracao_ociosa_cpu", usados)
        e_cpu = specs["tdp_w"] * (f + (1 - f) * iv["cpu_util"]) * dt
    if iv.get("e_dram_j") is not None:
        e_ram = iv["e_dram_j"]
    else:
        w_gb = _usar(params["ram_w_por_gb"], "ram_w_por_gb", usados)
        e_ram = w_gb * specs["ram_total_gb"] * dt
    return e_cpu, e_ram


def integrar(intervalos, t0, t1, specs, params, usados):
    e_cpu = e_ram = tx = rx = cobertura = 0.0
    for iv in intervalos:
        dur = iv["t_fim"] - iv["t_ini"]
        sobre = min(iv["t_fim"], t1) - max(iv["t_ini"], t0)
        if dur <= 0 or sobre <= 0:
            continue
        frac = sobre / dur
        c, r = energia_intervalo(iv, specs, params, usados)
        e_cpu += c * frac
        e_ram += r * frac
        tx += iv["bytes_tx"] * frac
        rx += iv["bytes_rx"] * frac
        cobertura += sobre
    return {"e_cpu_j": e_cpu, "e_ram_j": e_ram, "bytes_tx": tx, "bytes_rx": rx, "cobertura_s": cobertura}


def potencia_base(intervalos_base, specs, params, usados):
    total_j = total_s = 0.0
    for iv in intervalos_base:
        c, r = energia_intervalo(iv, specs, params, usados)
        total_j += c + r
        total_s += iv["t_fim"] - iv["t_ini"]
    return total_j / total_s if total_s > 0 else None


def deduplicar_maquinas(sessoes):
    """Uma máquina física conta uma vez só (ex.: cliente rodando no mesmo PC de um nó).
    Fica a sessão iniciada primeiro (cuja linha de base foi medida sem carga)."""
    por_maquina = {}
    for s in sorted(sessoes, key=lambda s: s["inicio"]):
        por_maquina.setdefault(s["specs"]["maquina_id"], s)
    descartadas = [s for s in sessoes if por_maquina[s["specs"]["maquina_id"]] is not s]
    return list(por_maquina.values()), descartadas


def relatorio_consulta(consulta, sessoes, cfg):
    """
    consulta: dict com t_inicio, t_fim, tokens_saida, cenario ...
    sessoes:  sessões do mesmo experimento/cenário que cruzam a janela, cada uma com
              'intervalos' (da janela) e 'intervalos_base' (fase baseline).
    """
    usados = {}
    params = cfg["parametros"]
    cen = cfg["cenarios"][consulta["cenario"]]
    t0, t1 = consulta["t_inicio"], consulta["t_fim"]
    janela = t1 - t0
    minimo = cfg["amostragem"]["cobertura_minima"]
    base_escolhida = cfg["contabilizacao"]["base_energia"]
    problemas = []

    ativas, duplicadas = deduplicar_maquinas(sessoes)
    if not ativas:
        problemas.append("Nenhuma máquina medida cobre a janela desta consulta.")

    maquinas = []
    ti_bruta = ti_marg = bytes_tx = 0.0
    for s in ativas:
        r = integrar(s["intervalos"], t0, t1, s["specs"], params, usados)
        p_base = potencia_base(s["intervalos_base"], s["specs"], params, usados)
        bruta = (r["e_cpu_j"] + r["e_ram_j"]) / 3600
        cobertura = r["cobertura_s"] / janela if janela > 0 else 0
        if cobertura < minimo:
            problemas.append(f"Máquina {s['no_id']} cobriu só {cobertura:.0%} da janela.")
        if p_base is None:
            problemas.append(f"Máquina {s['no_id']} sem linha de base; energia marginal = bruta.")
            e_base = 0.0
        else:
            e_base = p_base * janela / 3600
        marg = max(0.0, bruta - e_base)
        ti_bruta += bruta
        ti_marg += marg
        bytes_tx += r["bytes_tx"]
        maquinas.append({
            "no_id": s["no_id"],
            "papel": s["papel"],
            "hostname": s["specs"]["hostname"],
            "cpu": s["specs"]["cpu_modelo"],
            "metodo_cpu": s["specs"]["metodo_cpu"],
            "metodo_ram": s["specs"]["metodo_ram"],
            "cobertura": round(cobertura, 4),
            "e_cpu_wh": r["e_cpu_j"] / 3600,
            "e_ram_wh": r["e_ram_j"] / 3600,
            "e_bruta_wh": bruta,
            "p_base_w": p_base,
            "e_base_wh": e_base,
            "e_marginal_wh": marg,
            "bytes_tx": r["bytes_tx"],
            "bytes_rx": r["bytes_rx"],
        })

    e_ti = ti_bruta if base_escolhida == "bruta" else ti_marg
    pue = _usar(cen["pue"], f"pue_{consulta['cenario']}", usados)
    wue = _usar(cen["wue_l_por_kwh"], f"wue_{consulta['cenario']}", usados)
    cif = _usar(params["cif_g_por_wh"], "cif_g_por_wh", usados)

    e_rede = 0.0
    if cen.get("contar_rede"):
        k = _usar(params["k_rede_kwh_por_gb"], "k_rede_kwh_por_gb", usados)
        e_rede = bytes_tx / 1e9 * k * 1000  # kWh -> Wh

    e_instalacao = e_ti * pue
    e_total = e_instalacao + e_rede
    carbono_g = e_total * cif
    agua_direta = e_instalacao / 1000 * wue * 1000  # L -> mL
    agua_indireta = 0.0
    if params["ewif_l_por_kwh"].get("usar"):
        ewif = _usar(params["ewif_l_por_kwh"], "ewif_l_por_kwh", usados)
        agua_indireta = e_total / 1000 * ewif * 1000
    agua_total = agua_direta + agua_indireta

    tokens = consulta.get("tokens_saida") or 0
    por_token = None
    if tokens > 0:
        por_token = {
            "wh": e_total / tokens,
            "mg_co2": carbono_g / tokens * 1000,
            "ml_agua": agua_total / tokens,
        }

    return {
        "consulta_id": consulta.get("id"),
        "experimento": consulta.get("experimento"),
        "cenario": consulta["cenario"],
        "prompt_id": consulta.get("prompt_id"),
        "repeticao": consulta.get("repeticao"),
        "duracao_s": janela,
        "tokens_entrada": consulta.get("tokens_entrada"),
        "tokens_saida": tokens,
        "tokens_por_s": tokens / janela if janela > 0 else None,
        "valido": not any("cobriu só" in p or "Nenhuma máquina" in p for p in problemas),
        "problemas": problemas,
        "maquinas": maquinas,
        "maquinas_deduplicadas": [s["no_id"] for s in duplicadas],
        "energia": {
            "base_escolhida": base_escolhida,
            "ti_bruta_wh": ti_bruta,
            "ti_marginal_wh": ti_marg,
            "ti_wh": e_ti,
            "pue": pue,
            "instalacao_wh": e_instalacao,
            "rede_wh": e_rede,
            "rede_bytes_tx": bytes_tx,
            "total_wh": e_total,
        },
        "carbono_g": carbono_g,
        "agua_direta_ml": agua_direta,
        "agua_indireta_ml": agua_indireta,
        "agua_total_ml": agua_total,
        "por_token": por_token,
        "parametros_usados": usados,
    }


METRICAS_AGREGADAS = {
    "total_wh": lambda r: r["energia"]["total_wh"],
    "ti_bruta_wh": lambda r: r["energia"]["ti_bruta_wh"],
    "ti_marginal_wh": lambda r: r["energia"]["ti_marginal_wh"],
    "rede_wh": lambda r: r["energia"]["rede_wh"],
    "carbono_g": lambda r: r["carbono_g"],
    "agua_total_ml": lambda r: r["agua_total_ml"],
    "wh_por_token": lambda r: r["por_token"]["wh"] if r["por_token"] else None,
    "mg_co2_por_token": lambda r: r["por_token"]["mg_co2"] if r["por_token"] else None,
    "duracao_s": lambda r: r["duracao_s"],
    "tokens_saida": lambda r: r["tokens_saida"],
    "tokens_por_s": lambda r: r["tokens_por_s"],
}


def agregar(relatorios):
    por_cenario = {}
    for r in relatorios:
        por_cenario.setdefault(r["cenario"], []).append(r)
    saida = {}
    for cen, lista in por_cenario.items():
        validos = [r for r in lista if r["valido"]]
        metricas = {}
        for nome, f in METRICAS_AGREGADAS.items():
            vals = [v for v in (f(r) for r in validos) if v is not None]
            if vals:
                metricas[nome] = {
                    "media": statistics.fmean(vals),
                    "desvio": statistics.stdev(vals) if len(vals) > 1 else 0.0,
                    "n": len(vals),
                }
        saida[cen] = {"consultas": len(lista), "validas": len(validos), "metricas": metricas}
    return saida
