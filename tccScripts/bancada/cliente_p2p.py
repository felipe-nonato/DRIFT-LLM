"""
Cliente do cenário P2P: envia a carga de prompts ao enxame Drift e registra a
janela de cada consulta na API. Substitui o antigo teste_llm.py.

A máquina do cliente também é medida (embeddings, lm_head e tokenização rodam
nela). Se o cliente rodar no mesmo PC de um nó, a API conta a máquina uma vez só.

Exemplo:
  python cliente_p2p.py --api http://100.78.50.45:5000 --experimento exp01 \
      --initial_peers /ip4/100.78.50.45/tcp/31337/p2p/<PEER_ID> --torch_dtype bfloat16
"""

import argparse

from . import caminhos, experimento
from . import specs as specs_mod
from .medidor import SessaoMedida


def parse_args():
    p = argparse.ArgumentParser(description="Cliente P2P (Drift LLM) com medição")
    specs_mod.argumentos_specs(p)
    p.add_argument("--api", required=True)
    p.add_argument("--experimento", required=True)
    p.add_argument("--no_id", default="cliente")
    p.add_argument("--modelo", default="Qwen/Qwen2.5-7B-Instruct")
    p.add_argument("--initial_peers", required=True, help="Multiaddr do nó principal")
    p.add_argument("--torch_dtype", required=True, choices=list(experimento.DTYPES))
    p.add_argument("--prompts", default=str(caminhos.PROMPTS))
    p.add_argument("--repeticoes", type=int, default=3)
    p.add_argument("--max_new_tokens", type=int, default=128)
    p.add_argument("--pausa_s", type=float, default=15.0, help="Repouso antes de cada consulta")
    p.add_argument("--baseline_s", type=float)
    p.add_argument("--saida", default=str(caminhos.DIR_RESULTADOS))
    return p.parse_args()


def main():
    args = parse_args()
    specs = specs_mod.coletar_ou_sair(args)
    specs_mod.imprimir(specs)
    prompts = experimento.carregar_prompts(args.prompts)

    sessao = SessaoMedida(
        args.api, specs, args.experimento, "p2p", "cliente", args.no_id,
        extras={"modelo": args.modelo, "torch_dtype": args.torch_dtype, "max_new_tokens": args.max_new_tokens,
                "repeticoes": args.repeticoes, "prompts": [p["id"] for p in prompts]},
    )
    motivo = "concluido"
    try:
        sessao.iniciar(args.baseline_s)

        import torch
        from drift import AutoDistributedModelForCausalLM
        from transformers import AutoTokenizer

        tokenizer = AutoTokenizer.from_pretrained(args.modelo)
        print("Conectando ao enxame P2P...")
        modelo = AutoDistributedModelForCausalLM.from_pretrained(
            args.modelo, initial_peers=[args.initial_peers], torch_dtype=getattr(torch, args.torch_dtype),
        )

        def gerar(ids, max_new_tokens):
            with torch.inference_mode():
                return modelo.generate(
                    input_ids=ids, attention_mask=torch.ones_like(ids),
                    max_new_tokens=max_new_tokens, do_sample=False,
                )

        experimento.executar_consultas(sessao, tokenizer, gerar, prompts, args.repeticoes,
                                       args.max_new_tokens, args.pausa_s, args.saida, "P2P")
    except KeyboardInterrupt:
        motivo = "interrompido_pelo_usuario"
    except Exception as e:
        motivo = f"erro: {type(e).__name__}: {e}"
        raise
    finally:
        sessao.encerrar(motivo)


if __name__ == "__main__":
    main()
