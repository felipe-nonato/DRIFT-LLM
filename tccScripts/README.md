# Bancada de medição — P2P (Drift LLM) × centralizado, somente CPU

Mede energia, carbono e água por consulta na inferência do Qwen2.5-7B-Instruct em dois
cenários e controla o experimento distribuído por um painel web.

## Estrutura

```
bancada/            medição e cálculo (roda em cada máquina)
  specs.py          coleta/validação OBRIGATÓRIA das specs (CPU, TDP, RAM, RAPL, rede)
  medidor.py        amostragem contínua (RAPL ou utilização), relógio sincronizado com a API
  telemetria.py     API coletora (SQLite) — inclui POST /sessoes/<id>/anunciar (multiaddr)
  calculo.py        único lugar onde parâmetros são aplicados (PUE, WUE, CIF, k_rede…)
  experimento.py    laço de consultas comum aos dois cenários
  no_p2p.py         nó servidor do Drift; publica o próprio multiaddr na API
  cliente_p2p.py    carga de prompts no enxame
  centralizado.py   modelo completo em uma máquina
  caminhos.py       caminhos do projeto (config/ versionado, dados/ gerado)
painel/             plano de controle
  agente.py         em CADA máquina: inicia/para os processos locais (lista fechada de papéis)
  servidor.py       interface web + orquestração do protocolo
  processos.py      ciclo de vida dos processos (State) e papéis (Strategy)
  comandos.py       passos do protocolo (Command)
  orquestrador.py   montagem (Builder) e execução dos planos P2P e centralizado
  adaptadores.py    clientes HTTP do agente e da telemetria (Adapter)
  eventos.py        barramento de eventos → navegador via SSE (Observer)
  monitor.py        acompanha agentes/relatórios e publica mudanças (Observer)
  http_base.py      servidor HTTP com roteamento, token e SSE (Template Method)
  static/           index.html, estilo.css, app.js
config/
  config_tcc.json   parâmetros metodológicos, cada um com fonte
  maquinas.json     inventário: máquinas, papéis, blocos, interface
  tdp_cpus.json     TDP curado das CPUs
  prompts.json      carga de trabalho padrão
dados/              gerado (banco, logs, resultados, identidades libp2p) — fora do versionamento
```

## Instalação (todas as máquinas)

```bash
pip install -r requirements.txt          # + Drift LLM nas máquinas que rodam o modelo
sudo chmod o+r /sys/class/powercap/intel-rapl:*/energy_uj /sys/class/powercap/intel-rapl:*:*/energy_uj
python -m bancada.specs                  # deve terminar sem erro (rodar nativamente, não em WSL)
```

## Montar o laboratório (2 a 10 PCs)

Em **cada** PC (mesma rede local ou Tailscale):

```bash
# 1. copiar esta pasta para o PC e preparar o ambiente (Python 3.11 + torch CPU + Drift)
./instalar.sh          # instala o Drift de github.com/felipe-nonato/DRIFT-LLM
# 2. liberar o RAPL (a cada boot) — o instalador avisa se faltar
sudo chmod o+r /sys/class/powercap/intel-rapl:*/energy_uj
# 3. iniciar o agente com o MESMO segredo em todos os PCs
BANCADA_TOKEN=<segredo> ./iniciar_agente.sh          # usa o IP da interface padrão (ex.: enp3s0)
```

No PC que abrirá o painel, liste as máquinas em `config/maquinas.json` (uma entrada por PC,
com o IP que o agente mostrou ao iniciar, a `interface` e os `papeis`). Os servidores precisam
somar ao menos 28 blocos e cada um deve caber em 85% da RAM (bfloat16 ≈ 0,43 GiB por bloco):

| PCs servidores | Exemplo de divisão | RAM mínima por PC |
|---|---|---|
| 2 | 14 + 14 | 8 GB |
| 3 | 10 + 10 + 8 | 6 GB |
| 4 | 7 + 7 + 7 + 7 | 4 GB |

O PC do **cliente** carrega ainda as camadas de embedding e de saída (≈ 2 GiB em bfloat16): se ele
também for servidor, reduza os blocos dele. O cenário centralizado precisa de um PC com ≥ 16 GB (modelo inteiro ≈ 14,2 GiB em bfloat16).
Libere no firewall as portas 7000 (agente), 5000 (telemetria) e 31337 (P2P) entre os PCs.

## Rede P2P em 2 cliques (recomendado)

O **mesmo comando** em todos os PCs (na primeira vez ele roda o `./instalar.sh`):

```bash
./rede.sh                    # abre http://127.0.0.1:8090
```

**No PC central:** clique em **Criar a rede aqui** e escolha a rede (local ou Tailscale) e o nome
do experimento. A página liga a telemetria e o nó bootstrap. Depois da linha de base (~1 min),
o ID aparece com o botão **Copiar ID**.

**Em cada outro PC:** clique em **Entrar numa rede** e cole o ID. A conexão começa sozinha: a
página acha a telemetria do central, o experimento, o modelo, a precisão, a interface de rede e
quantos blocos cabem em 85% da RAM.

Tudo aparece na página do central:
- **PCs na rede:** quem entrou, a CPU, o método de medição e a soma de blocos (a rede precisa cobrir as 28 camadas);
- **Rodar consultas:** envia os prompts de `config/prompts.json` pelo enxame;
- **Consumo:** energia, CO₂ e água por consulta (média ± desvio), a tabela das consultas e o **Baixar CSV**;
- **Log** de cada processo deste PC.

**Desligar este PC** (ou Ctrl+C no terminal) para os processos na ordem certa. Desligar o central derruba a rede.
Se o `rede.sh` avisar, libere o RAPL e as portas 5000/31337 no firewall (ele mostra os comandos).

## Uso com o painel

1. Defina o mesmo segredo em todas as máquinas: `export BANCADA_TOKEN=<segredo>`.
2. Ajuste `config/maquinas.json` (IPs do Tailscale, papéis, `num_blocks` somando ≥ 28).
   Na página da rede (`./rede.sh`) os blocos são planejados sozinhos: cada PC serve um intervalo fixo
   (`--block_indices`) calculado pela RAM **disponível** no momento (ver `bancada/blocos.py`). Feche
   navegador e IDE antes de criar a rede ou entrar nela. Prefira como nó central o PC com mais RAM,
   pois o cliente (embeddings + lm_head, ~2,3 GiB em bf16 e ~4,4 GiB em fp32) roda nele.
3. Em **cada** máquina, na raiz do projeto: `python -m painel.agente`
   (escuta no IP do `tailscale0`, porta 7000; recusa iniciar exposto sem token).
4. Em uma máquina: `python -m painel.servidor` e abra `http://127.0.0.1:8080`.
   Para expor: `--host 0.0.0.0` e acesse uma vez `http://IP:8080/login?token=<segredo>`.
5. No painel: **Validar specs** → **Executar P2P** → **Executar centralizado** (mesmo rótulo).

O protocolo executado pelo botão **Executar P2P**:
validar specs → garantir telemetria → iniciar bootstrap → aguardar o nó publicar o multiaddr →
iniciar demais servidores com esse endereço → aguardar linha de base → aguardar carregamento dos
blocos → iniciar cliente → aguardar a carga → parar servidores. Em falha ou cancelamento, os
processos iniciados são parados (sem órfãos).

## Uso manual (sem painel)

```bash
python -m bancada.telemetria
python -m bancada.no_p2p --api http://IP:5000 --experimento exp01 --no_id no1 --bootstrap --block_indices 0:8 --torch_dtype bfloat16
python -m bancada.no_p2p --api http://IP:5000 --experimento exp01 --no_id no2 --initial_peers <MULTIADDR> --block_indices 8:14 --torch_dtype bfloat16
python -m bancada.cliente_p2p --api http://IP:5000 --experimento exp01 --initial_peers <MULTIADDR> --torch_dtype bfloat16
python -m bancada.centralizado --api http://IP:5000 --experimento exp01 --torch_dtype bfloat16
curl "http://IP:5000/relatorio.csv?experimento=exp01"
```

O `<MULTIADDR>` aparece no log do bootstrap como `[MULTIADDR] /ip4/.../p2p/12D3KooW...` e em
`GET /sessoes?experimento=exp01`.

## Teste de carga (1 a N clientes simultâneos)

Com o enxame no ar (bootstrap publicado na API com o mesmo `--experimento`):

```bash
.venv/bin/python -m bancada.teste_carga --api http://127.0.0.1:5000 --experimento carga-qwen05b \
    --modelo Qwen/Qwen2.5-0.5B-Instruct --torch_dtype float32 \
    --clientes 1-10 --requisicoes_por_cliente 2 --tokens 16,32,64,128,256 --interface enp3s0
.venv/bin/python analise/carga_pandas.py            # resumo da última execução
.venv/bin/python analise/carga_pandas.py --csv dados/csv
```

- Cada nível n roda n clientes em paralelo (threads com sessões de inferência independentes no
  enxame, compartilhando um modelo cliente); cada cliente faz R requisições em sequência.
- Orçamentos de tokens e prompts são sorteados de forma reprodutível (`--semente`); por padrão a
  saída tem exatamente `max_new_tokens` tokens (`--saida_livre` permite parar no EOS).
- Banco `dados/carga.db`: tabelas `execucoes`, `niveis` (vazão, latência, energia/CO₂/água do nível
  calculados pela API), `requisicoes`, `amostras` (séries de 1 s) e as views `v_requisicoes`
  (com `wh_atribuido` proporcional aos tokens) e `v_niveis`.

```python
import sqlite3, pandas as pd
con = sqlite3.connect("dados/carga.db")
niveis = pd.read_sql("SELECT * FROM v_niveis", con)
req = pd.read_sql("SELECT * FROM v_requisicoes", con)
```

## Servidor centralizado convencional × P2P (teste de carga)

`bancada/servidor_central.py` é um servidor de inferência HTTP com o modelo inteiro em uma máquina
(`POST /v1/gerar`), com lote dinâmico (`--modo lote`, padrão) ou fila FIFO (`--modo serial`), medido
pelo mesmo pipeline (cenário `centralizado`, PUE/WUE de data center). `bancada/carga_central.py` aplica
a MESMA carga do `teste_carga` P2P (mesmo plano, semente, níveis e orçamentos) por clientes HTTP
simultâneos e grava no mesmo `dados/carga.db`.

Com a máquina livre (sem nó P2P rodando — a medição é da máquina inteira):

```bash
.venv/bin/python -m bancada.carga_central --api http://127.0.0.1:5000 --experimento central-carga-qwen05b \
    --iniciar_servidor --modelo Qwen/Qwen2.5-0.5B-Instruct --torch_dtype float32 \
    --clientes 1-10 --requisicoes_por_cliente 2 --tokens 16,32,64,128,256 --interface enp3s0
.venv/bin/python analise/comparar_carga.py --api http://127.0.0.1:5000   # P2P × centralizado por nível
```

`--iniciar_servidor` sobe a telemetria (se não houver) e o servidor, mede a linha de base, aplica a
carga e encerra tudo. A tabela `requisicoes` ganha `t_fila_s`, `t_geracao_s` e `lote` no centralizado.

## Regras do método

- Energia medida **por janela de consulta**, somando todas as máquinas do cenário, no relógio da API.
- Medição da **máquina** (CPU + RAM), não do processo; máquina repetida conta uma vez.
- A API devolve energia **bruta** e **marginal**; `contabilizacao.base_energia` escolhe a principal.
- Fronteira: CPU (package) + RAM. Fonte, disco, placa-mãe e tela ficam fora — declarar como limitação.
- Consulta com cobertura < 95% em alguma máquina é inválida e sai dos agregados.

## Segurança

- Agente e painel exigem `BANCADA_TOKEN` quando não escutam só em `127.0.0.1`.
- O agente só executa os papéis de `painel/processos.py`, com argumentos de lista fechada.
- A API de telemetria não tem autenticação: mantenha-a acessível apenas pela rede do Tailscale.
- Encerre o agente com Ctrl+C, SIGTERM ou fechando o terminal: ele para os processos filhos antes de sair.
  Se o agente for morto com `kill -9`, os processos filhos continuam rodando e precisam ser encerrados à mão.
