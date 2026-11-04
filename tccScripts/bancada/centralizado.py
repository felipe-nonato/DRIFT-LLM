"""
Cenário CENTRALIZADO simulado: o modelo inteiro em uma única máquina (CPU),
medido pelo mesmo pipeline do P2P. Na API, o PUE e o WUE de data center
(config_tcc.json → cenarios.centralizado) são aplicados sobre a energia de TI.

Exemplo:
  python centralizado.py --api http://100.78.50.45:5000 --experimento exp01 --torch_dtype bfloat16

A API pode rodar na própria máquina (--api http://127.0.0.1:5000).
"""

import argparse

from . import caminhos, experimento
from . import specs as specs_mod
from .medidor import SessaoMedida


def parse_args():
    p = argparse.ArgumentParser(description="Cenário centralizado simulado (CPU) com medição")
    specs_mod.argumentos_specs(p)
    p.add_argument("--api", required=True)
    p.add_argument("--experimento", required=True)
    p.add_argument("--no_id", default="central")
    p.add_argument("--modelo", default="Qwen/Qwen2.5-7B-Instruct")
    p.add_argument("--torch_dtype", required=True, choices=list(experimento.DTYPES),
                   help="DEVE ser a mesma precisão usada no P2P")
    p.add_argument("--threads", type=int, help="Threads do PyTorch (padrão: todos os núcleos lógicos)")
    p.add_argument("--prompts", default=str(caminhos.PROMPTS))
    p.add_argument("--repeticoes", type=int, default=3)
    p.add_argument("--max_new_tokens", type=int, default=128)
    p.add_argument("--pausa_s", type=float, default=15.0)
    p.add_argument("--baseline_s", type=float)
    p.add_argument("--saida", default=str(caminhos.DIR_RESULTADOS))
    p.add_argument("--forcar_memoria", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    # No centralizado não há tráfego entre nós; a interface só precisa existir.
    specs = specs_mod.coletar_ou_sair(args)
    specs_mod.imprimir(specs)
    prompts = experimento.carregar_prompts(args.prompts)

    mem_gb, camadas = experimento.estimar_memoria_gb(args.modelo, args.torch_dtype)
    experimento.checar_memoria(specs, mem_gb, args.forcar_memoria, f"modelo completo ({camadas} camadas)")

    import torch
    threads = args.threads or specs["cpu_nucleos_logicos"]
    torch.set_num_threads(threads)

    sessao = SessaoMedida(
        args.api, specs, args.experimento, "centralizado", "centralizado", args.no_id,
        extras={"modelo": args.modelo, "torch_dtype": args.torch_dtype, "threads": threads,
                "max_new_tokens": args.max_new_tokens, "repeticoes": args.repeticoes,
                "prompts": [p["id"] for p in prompts]},
    )
    motivo = "concluido"
    try:
        sessao.iniciar(args.baseline_s)

        from transformers import AutoModelForCausalLM, AutoTokenizer

        tokenizer = AutoTokenizer.from_pretrained(args.modelo)
        print(f"Carregando {args.modelo} ({args.torch_dtype}, {threads} threads) na CPU...")
        # transformers 5.x (exigido pelo Drift) usa `dtype`; o carregamento já é de baixa memória
        modelo = AutoModelForCausalLM.from_pretrained(args.modelo, dtype=getattr(torch, args.torch_dtype))
        modelo.eval()

        def gerar(ids, max_new_tokens):
            with torch.inference_mode():
                return modelo.generate(
                    input_ids=ids, attention_mask=torch.ones_like(ids),
                    max_new_tokens=max_new_tokens, do_sample=False,
                )

        experimento.executar_consultas(sessao, tokenizer, gerar, prompts, args.repeticoes,
                                       args.max_new_tokens, args.pausa_s, args.saida, "CENTRAL")
    except KeyboardInterrupt:
        motivo = "interrompido_pelo_usuario"
    except Exception as e:
        motivo = f"erro: {type(e).__name__}: {e}"
        raise
    finally:
        sessao.encerrar(motivo)


if __name__ == "__main__":
    main()
