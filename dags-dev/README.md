## Objetivo deste projeto
Proporcionar o ambiente de desenvolvimento e o código fonte das DAGs utilizadas pelo AIrflow em produção.

## O que este projeto faz
Cada DAG possui uma subpasta com o código fonte das funções cujo conteúdo, quando replicado para a pasta airflow, viabiliza a implementação em produção.
As configurações de cada subprojeto são carregadas via o módulo config.py, que identifica o ambiente de execução, seja na produção, via Airflow, ou desenvolvimento, localmente na subpasta dentro da pasta dags-dev.
A implementação em produção é feita via DAGs do Airflow de produção existentes na pasta airflow, com a criação de um arquivo contendo a DAG e todas as configurações.
A execução no ambiente de desenvolvimento é efetuada através de um script python que realiza todas as tarefas das tasks na mesma sequência que na DAG corerspondente do ambiente de produção no Airflow.


## Pré-requisitos
Cada subprojeto tem seus pré-requisitos específicos descritos na subpasta correspondente.

## Configurações
Cada subprojeto tem suas configurações específicas descritas no README na subpasta correspondente.

## Instruções para instalação
Os pacotes python utilizados são gerenciados de forma unificada da mesma maneira que no Airflow, com um só arquivo requirements.txt.

Para instalar os requisitos de todos os subprojetos:
- cd dags-dev
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

## Instruções para execução dos scripts em desenvolvimento
Os scripts se encontram na pasta dags-dev da mesma forma que na pasta dags do Airflow e devem ser executados diretamenet da pasta, desta maneira:

python <nome_da_dag-versao-da-dag.py>

Se o arquivo .env não existir na raiz do projeto, crie-o com as variáveis enumeradas conforme definido no README de cada subprojeto.

