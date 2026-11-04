"""Bancada de medição energética: P2P (Drift LLM) × centralizado, somente CPU.

Módulos executáveis (a partir da raiz do projeto):
    python -m bancada.specs          valida as especificações da máquina
    python -m bancada.telemetria     API coletora
    python -m bancada.no_p2p         nó servidor do enxame
    python -m bancada.cliente_p2p    cliente do enxame (carga de prompts)
    python -m bancada.centralizado   cenário centralizado simulado
"""
