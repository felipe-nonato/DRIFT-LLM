"""Painel de controle da bancada (plano de controle + agentes).

    python -m painel.agente     em CADA máquina: controla os processos locais
    python -m painel.servidor   em uma máquina: interface web e orquestração

Camadas e padrões:
    http_base      Template Method — servidor HTTP com roteamento, JSON, SSE e autenticação
    processos      State + Strategy — ciclo de vida de cada processo; papéis como estratégias
    agente         Facade — expõe os processos locais por HTTP
    adaptadores    Adapter — clientes HTTP do agente e da API de telemetria
    eventos        Observer — barramento de eventos publicado ao navegador via SSE
    comandos       Command — passos do protocolo experimental, executáveis e auditáveis
    orquestrador   State + Builder — monta e executa os planos de experimento
    monitor        Observer — acompanha agentes e relatórios e publica mudanças
    servidor       Facade — interface web do plano de controle
"""
