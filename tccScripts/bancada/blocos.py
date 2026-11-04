"""
Planejamento dos blocos de cada servidor P2P.

Dois problemas que este módulo evita:
  1. Calcular a capacidade pela RAM TOTAL. Os pesos são mapeados do arquivo (mmap) e
     aparecem como "cache", não como "usada": se a RAM disponível (descontados sistema,
     navegador, cliente…) não comporta os blocos, o kernel descarta páginas dos pesos e as
     relê do disco a cada token, inflando tempo e energia sem que o monitor acuse falta de RAM.
  2. Deixar o Drift escolher QUAIS blocos cada nó serve (--num_blocks). Ele rebalanceia o
     enxame durante a execução e a cadeia de servidores pode mudar entre consultas. Com
     --block_indices fixo, a rota é a mesma do início ao fim do experimento.

Capacidade de um PC (GiB; mesma unidade de specs.ram_total_gb):

    limite  = min(0,85 × RAM_total, RAM_disponível − folga) − reserva_cliente
    blocos  = ⌊ limite / (tamanho_do_bloco + cache_de_atenção_por_bloco) ⌋

Distribuição: o nó central serve 0:n; cada PC que entra recebe o primeiro intervalo de
camadas ainda não coberto, limitado à sua capacidade e, se informado o número de PCs
previstos, à cota balanceada ⌈camadas restantes / PCs restantes⌉.

Modo manual: o operador informa quantos blocos o PC serve. Ele recebe o primeiro trecho
livre onde esse número cabe inteiro (sem sobrepor outro servidor), ignorando a cota.
"""

import math

FRACAO_RAM = 0.85               # teto já adotado pela bancada (sem swap)
FOLGA_GIB = 1.0                 # sistema, bancada e picos de memória
CACHE_POR_BLOCO_GIB = 0.04      # cache de atenção alocado pelo Drift (observado: 0,31 GiB / 10 blocos)
SOBRECARGA_CLIENTE_GIB = 0.3    # runtime do PyTorch, tokenizador e ativações do cliente


def memoria_ram():
    """(total, disponível) em GiB. 'Disponível' = MemAvailable: inclui cache recuperável."""
    import psutil
    vm = psutil.virtual_memory()
    return vm.total / 1024**3, vm.available / 1024**3


def memoria_cliente_gib(modelo, torch_dtype):
    """Embeddings + lm_head (o cliente P2P carrega as duas pontas do modelo) + sobrecarga."""
    from transformers import AutoConfig

    from .experimento import DTYPES
    cfg = AutoConfig.from_pretrained(modelo)
    matrizes = 1 if getattr(cfg, "tie_word_embeddings", False) else 2
    return matrizes * cfg.vocab_size * cfg.hidden_size * DTYPES[torch_dtype] / 1024**3 + SOBRECARGA_CLIENTE_GIB


def capacidade(gib_bloco, ram_total, ram_disponivel, reserva_gib=0.0):
    teto_total = ram_total * FRACAO_RAM
    teto_livre = ram_disponivel - FOLGA_GIB
    limite = min(teto_total, teto_livre) - reserva_gib
    n = math.floor(limite / (gib_bloco + CACHE_POR_BLOCO_GIB)) if limite > 0 else 0
    return {
        "blocos": max(0, n),
        "limite_gib": round(limite, 2),
        "ram_total_gib": round(ram_total, 2),
        "ram_disponivel_gib": round(ram_disponivel, 2),
        "reserva_cliente_gib": round(reserva_gib, 2),
        "gib_por_bloco": round(gib_bloco, 3),
        "restricao": "RAM disponível" if teto_livre < teto_total else f"{FRACAO_RAM:.0%} da RAM total",
    }


def descrever(cap):
    return (f"{cap['blocos']} blocos: limite {cap['limite_gib']:.1f} GiB "
            f"(RAM {cap['ram_total_gib']:.1f} GiB, disponível {cap['ram_disponivel_gib']:.1f} GiB, "
            f"folga {FOLGA_GIB:.1f} GiB, reserva do cliente {cap['reserva_cliente_gib']:.1f} GiB; "
            f"restrição: {cap['restricao']}) ÷ {cap['gib_por_bloco'] + CACHE_POR_BLOCO_GIB:.2f} GiB por bloco")


def ler_intervalo(texto):
    ini, fim = (int(x) for x in str(texto).split(":"))
    if not 0 <= ini < fim:
        raise ValueError(f"Intervalo inválido: {texto}")
    return ini, fim


def ocupados(sessoes):
    """Servidores abertos: ([(ini, fim, no_id)] com intervalo fixo, [no_id] sem intervalo fixo)."""
    fixos, sem_intervalo = [], []
    for s in sessoes:
        if s.get("papel") != "servidor" or s.get("fim"):
            continue
        bi = (s.get("extras") or {}).get("block_indices")
        if bi:
            fixos.append((*ler_intervalo(bi), s.get("no_id")))
        else:
            sem_intervalo.append(s.get("no_id"))
    return fixos, sem_intervalo


def lacunas(camadas, fixos):
    cobertas = set()
    for ini, fim, _ in fixos:
        cobertas.update(range(ini, fim))
    saida, ini = [], None
    for c in range(camadas + 1):
        livre = c < camadas and c not in cobertas
        if livre and ini is None:
            ini = c
        elif not livre and ini is not None:
            saida.append((ini, c))
            ini = None
    return saida


def sobrepostos(ini, fim, fixos):
    return [no for a, b, no in fixos if a < fim and ini < b]


def alvo_balanceado(camadas, fixos, previstas):
    """Cota deste PC se o número total de PCs servidores for conhecido; senão None."""
    if not previstas:
        return None
    restantes = sum(b - a for a, b in lacunas(camadas, fixos))
    pcs_restantes = max(1, previstas - len(fixos))
    return math.ceil(restantes / pcs_restantes)


def proximo_intervalo(camadas, fixos, blocos_max, previstas=None):
    """Primeiro trecho não coberto, limitado à capacidade e à cota. None se já está tudo coberto."""
    livres = lacunas(camadas, fixos)
    if not livres or blocos_max < 1:
        return None
    ini, fim = livres[0]
    alvo = alvo_balanceado(camadas, fixos, previstas)
    n = min(blocos_max, fim - ini, alvo or camadas)
    return ini, ini + n


def intervalo_manual(camadas, fixos, n):
    """Primeiro trecho livre com pelo menos n camadas: (ini, ini+n). None se nenhum comporta n."""
    for ini, fim in lacunas(camadas, fixos):
        if fim - ini >= n:
            return ini, ini + n
    return None
