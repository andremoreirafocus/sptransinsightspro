
## Objetivo deste projeto
Viabilizar a análise exploratória de dados com mínima infraestrutura

## O que este projeto faz
Disponibiliza um ambiente de notebook usando Jupyter com DuckDB pré-instalado para possibilitar a análise exploratória de dados na camada trusted implementada usando object storage.
Os notebooks são criados na pasta notebooks

## Pré-requisitos
Cada subprojeto tem seus pré-requisitos específicos descritos na subpasta correspondente.

## Configurações
Conceder permissão de escrita na subpasta notebooks utilizada para armazenar notebooks
    sudo chown -R 1000:100 ./jupyter/notebooks
    chmod -R 777 ./jupyter/notebooks

## Instruções para execução
O Jupyter é iniciado via docker compose ao iniciar todos os serviços através do comando:
    docker compose up -d

Caso se deseje iniciar apenas este serviço, basta executar:
    docker compose up -d jupyter

## Instruções para acessar o jupyter:
Basta acessar o link http://localhost:8888

O notebook  ![positions_for_line_and_vehicle_and_day.ipynb](./notebooks/positions_for_line_and_vehicle_and_day.ipynb) contem funções exemplo para acesso ao DuckDB para análise exploratória.




