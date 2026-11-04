"""
Partes comuns aos dois cenários: prompts, checagem de memória e o laço de
consultas que registra cada janela [t_inicio, t_fim] na API.

O mesmo laço é usado pelo cliente P2P e pelo cenário centralizado, garantindo
que ambos passem pelo mesmo pipeline (mesmos prompts, mesma decodificação gulosa,
mesmo limite de tokens, mesma pausa entre consultas).
"""

import json
import time
from pathlib import Path

import requests

DTYPES = {"float32": 4, "bfloat16": 2, "float16": 2}


def carregar_prompts(caminho):
    dados = json.loads(Path(caminho).read_text(encoding="utf-8"))
    prompts = dados["prompts"] if isinstance(dados, dict) else dados
    ids = [p["id"] for p in prompts]
    if len(ids) != len(set(ids)):
        raise SystemExit("prompts.json: ids repetidos.")
    return prompts


def estimar_memoria_gb(modelo, torch_dtype, num_blocos=None):
    """
    Memória dos pesos (GB) — do modelo inteiro ou de N blocos Transformer —
    instanciando a arquitetura no dispositivo 'meta' (não baixa nem aloca pesos).
    """
    try:
        import torch
        from transformers import AutoConfig, AutoModelForCausalLM
    except ImportError as e:
        raise SystemExit(f"❌ Dependência ausente: {e.name}. Instale com 'pip install -r requirements.txt' "
                         f"no ambiente Python usado ({__import__('sys').executable}).")

    cfg = AutoConfig.from_pretrained(modelo)
    with torch.device("meta"):
        m = AutoModelForCausalLM.from_config(cfg)
    total_camadas = cfg.num_hidden_layers
    if num_blocos is None:
        n = sum(p.numel() for p in m.parameters())
    else:
        por_bloco = sum(p.numel() for p in m.model.layers[0].parameters())
        n = por_bloco * num_blocos
    return n * DTYPES[torch_dtype] / 1024**3, total_camadas


def checar_memoria(specs, necessario_gb, forcar, rotulo):
    from .blocos import FOLGA_GIB, FRACAO_RAM, memoria_ram

    total = specs["ram_total_gb"]
    _, livre = memoria_ram()
    print(f"[memória] {rotulo}: ~{necessario_gb:.2f} GiB de pesos | RAM total {total:.2f} GiB, "
          f"disponível agora {livre:.2f} GiB")
    problemas = []
    if necessario_gb > total * FRACAO_RAM:
        problemas.append(f"Os pesos ({necessario_gb:.2f} GiB) excedem {FRACAO_RAM:.0%} da RAM ({total:.2f} GiB).")
    if necessario_gb > livre - FOLGA_GIB:
        # pesos mapeados do arquivo contam como cache: sem RAM livre, são relidos do disco a cada token
        problemas.append(f"Os pesos ({necessario_gb:.2f} GiB) não cabem na RAM disponível agora "
                         f"({livre:.2f} GiB − folga de {FOLGA_GIB:.1f} GiB). Feche navegador/IDE ou reduza os blocos.")
    for msg in problemas:
        msg += " Haverá paginação (swap/releitura do disco), o que distorce tempo e energia."
        if not forcar:
            raise SystemExit("❌ " + msg + " Ou use --forcar_memoria (registrado como limitação).")
        print("⚠️  " + msg)
        specs["avisos"].append(msg)


class ContadorTokens:
    """Streamer do transformers que conta os tokens gerados e imprime o progresso.

    Interface de transformers.generation.streamers.BaseStreamer: put() recebe primeiro o
    prompt e depois cada token novo; end() ao terminar. Imprime no máximo a cada `intervalo_s`
    (e no último token): uma linha a cada ~20 s no P2P em CPU, custo desprezível.
    Formato lido pelo painel (painel/processos.py) e por analise/progresso.py:
      [P2P] gerando 37/128 tokens em 812s · consulta 1/15 prompt=p1_colonialismo rep=1
    """

    def __init__(self, rotulo, max_new_tokens, consulta, total, prompt_id, rep, intervalo_s=2.0):
        self.rotulo, self.max, self.consulta, self.total = rotulo, max_new_tokens, consulta, total
        self.prompt_id, self.rep, self.intervalo = prompt_id, rep, intervalo_s
        self.n, self.prompt_visto, self.t0, self.ultimo = 0, False, time.monotonic(), 0.0

    def _imprimir(self):
        self.ultimo = time.monotonic()
        print(f"[{self.rotulo}] gerando {self.n}/{self.max} tokens em {self.ultimo - self.t0:.0f}s · "
              f"consulta {self.consulta}/{self.total} prompt={self.prompt_id} rep={self.rep}", flush=True)

    def put(self, valor):
        if not self.prompt_visto:  # a primeira chamada traz os tokens de entrada
            self.prompt_visto = True
            self._imprimir()
            return
        self.n += int(valor.numel())
        if self.n >= self.max or time.monotonic() - self.ultimo >= self.intervalo:
            self._imprimir()

    def end(self):
        self._imprimir()


def executar_consultas(sessao, tokenizer, gerar, prompts, repeticoes, max_new_tokens, pausa_s,
                       saida_dir, rotulo):
    import torch

    api = sessao.api
    DIR_RES = Path(saida_dir)
    DIR_RES.mkdir(exist_ok=True)

    def preparar(texto):
        ids = tokenizer.apply_chat_template(
            [{"role": "user", "content": texto}], add_generation_prompt=True, return_tensors="pt"
        )
        if not isinstance(ids, torch.Tensor):  # versões que devolvem BatchEncoding
            ids = ids["input_ids"]
        return ids

    print(f"[{rotulo}] Aquecimento (não contabilizado)...")
    gerar(preparar(prompts[0]["texto"]), 8)

    total = repeticoes * len(prompts)
    feitas = 0
    for rep in range(1, repeticoes + 1):
        for p in prompts:
            time.sleep(pausa_s)  # repouso entre janelas: separa consultas nas amostras
            ids = preparar(p["texto"])
            contador = ContadorTokens(rotulo, max_new_tokens, feitas + 1, total, p["id"], rep)
            t0 = sessao.relogio.agora()
            saida = gerar(ids, max_new_tokens, contador)
            t1 = sessao.relogio.agora()
            n_out = int(saida.shape[1] - ids.shape[1])
            resposta = tokenizer.decode(saida[0, ids.shape[1]:], skip_special_tokens=True)
            r = requests.post(f"{api}/consultas", json={
                "experimento": sessao.experimento,
                "cenario": sessao.cenario,
                "prompt_id": p["id"],
                "repeticao": rep,
                "t_inicio": t0,
                "t_fim": t1,
                "tokens_entrada": int(ids.shape[1]),
                "tokens_saida": n_out,
                "resposta": resposta,
                "extras": {"max_new_tokens": max_new_tokens, "sessao_cliente": sessao.id},
            }, timeout=10)
            r.raise_for_status()
            feitas += 1
            print(f"[{rotulo}] {feitas}/{total} prompt={p['id']} rep={rep}: {n_out} tokens em {t1 - t0:.1f}s "
                  f"({n_out / (t1 - t0):.2f} tok/s) → consulta #{r.json()['consulta_id']}")

    espera = sessao.cfg["lote_envio_s"] * 2 + 2
    print(f"[{rotulo}] Aguardando {espera:.0f}s para as últimas amostras chegarem à API...")
    time.sleep(espera)

    rel = requests.get(f"{api}/relatorio", params={"experimento": sessao.experimento}, timeout=120).json()
    csv_txt = requests.get(f"{api}/relatorio.csv", params={"experimento": sessao.experimento}, timeout=120).text
    carimbo = time.strftime("%Y%m%d-%H%M%S")
    base = DIR_RES / f"{sessao.experimento}_{sessao.cenario}_{carimbo}"
    Path(f"{base}.json").write_text(json.dumps(rel, indent=2, ensure_ascii=False), encoding="utf-8")
    Path(f"{base}.csv").write_text(csv_txt, encoding="utf-8")
    print(f"[{rotulo}] Relatório salvo em {base}.json e {base}.csv")
    imprimir_agregado(rel)
    return rel


def imprimir_agregado(rel):
    print("\n" + "=" * 64)
    print(f" RESULTADO — experimento '{rel['experimento']}'")
    print("=" * 64)
    for cen, a in rel["agregado"].items():
        print(f" [{cen}] consultas válidas: {a['validas']}/{a['consultas']}")
        for nome in ("total_wh", "wh_por_token", "carbono_g", "mg_co2_por_token", "agua_total_ml",
                     "rede_wh", "tokens_por_s"):
            m = a["metricas"].get(nome)
            if m:
                print(f"   {nome:<18}: {m['media']:.6g} ± {m['desvio']:.3g} (n={m['n']})")
    invalidas = [c for c in rel["consultas"] if not c["valido"]]
    for c in invalidas:
        print(f" ⚠️  consulta #{c['consulta_id']} inválida: {' | '.join(c['problemas'])}")
    print("=" * 64)
