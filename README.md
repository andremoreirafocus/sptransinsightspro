Este projeto proporciona aos seus usuários visualizações sobre as posições atuais de todos os ônibus rastreados pela SPTrans e apresenta informações sobre as viagens concluídas nas últimas horas.

Para isto, o Sptransinsights, em intervalos regulares, extrai as posições de todos os ônibus em circulação em cada momento, armazenando estes dados para gerar informações sobre as viagens de cada veículo de cada linha e, assim, proporcionar insights aos seus usuários, permitindo que identifiquem os melhores momentos para fazerem suas viagens.

A pipeline mais crítica do projeto (`transformlivedata`) aplica um framework completo de qualidade de dados, com validações orientadas a configuração (JSON Schema e Great Expectations), quarentena de registros inválidos e geração de relatório de qualidade com resumo e detalhes do processamento proporcionando informações de observabilidade.  
Observação: os resumos de qualidade das pipelines `transformlivedata`, `gtfs`, `refinedfinishedtrips` e do microserviço `extractloadlivedata` são enviados via webhook para o microserviço `alertservice`, responsável por emitir notificações e alertas por e-mail com alertas imediatos para falhas e alertas cumulativos para warnings.

A solução adota o conceito de monorepo e é composta por alguns subprojetos. Cada um deles possui um README com informações sobre o seu papel e os requisitos para o seu funcionamento.

## Arquitetura
![Diagrama da solução](./diagrama_solucao.png)

Para implementar a solução foram adotados os componentes:
- Airflow: para orquestração de processos recorrentes do pipeline através de diversas DAGs utilizando o Python Operator. O ambiente de produção para este módulo se encontra na pasta airflow. ![Para mais informações:](./airflow/README.md). 
O ambiente de desenvolvimento se encontra na pasta dags-dev. ![Para mais informações:](./dags-dev/README.md)
Detalhes sobre as DAGS:
    - DAG gtfs: processo composto de 3 etapas principais.   
        - extração e carga de arquivos: que extrai os dados GTFS da SPTRANS, valida os arquivos e salva na camada raw (ou quarentena em caso de falha).
        - transformação: executa a **TRANSFORMATION STAGE** com validação de qualidade (Great Expectations quando aplicável), staging de artefatos (tabelas) em formato Parquet, quarentena em caso de falha e promoção para caminho final em caso de sucesso.
        - enrichment: cria a tabela `trip_details` em staging, valida com Great Expectations e só então promove para caminho final (ou quarentena em caso de falha).
        - qualidade: gera um relatório consolidado único por execução, persistido em bucket de metadados, incluindo diagnósticos por fase e linhagem de colunas de `trip_details` com detecção de drift.
        - Esta DAG emite um sinal (dataset do Airflow) ao ser finalizada com sucesso, permitindo que a DAG de sincronização dos detalhes de viagens para a camada refined seja iniciada automaticamente.
        ![Para mais informações:](./dags-dev/gtfs/README.md)
    - DAG transformlivedata: processo de transformação dos dados brutos de posição da camada raw em dados enriquecidos e confiáveis na camada trusted. 
        - Validação do JSON bruto via JSON Schema (configuração externa)
        - Validação pós-transformação via Great Expectations (suite externa)
        - Quarentena de registros inválidos
        - Relatório de qualidade em JSON com `summary` e `details`, incluindo métricas, issues e informações parciais em caso de falha
        ![Para mais informações:](./dags-dev/transformlivedata/README.md)
    - DAG orchestratetransform: processo de identificação de dados de posição dos ônibus pendentes de processamento e que dispara a DAG de transformação.  ![Para mais informações:](./dags-dev/orchestratetransform/README.md)
    - DAG refinedfinishedtrips: processo de transformação para criação das informações de viagens na camada refined a partir dos dados da camada enriquecidos da camada trusted, incluindo checagem de qualidade com as seguintes etapas:
        - Extração de viagens baseada principalmente em geolocalização 
        - Sanitização de amostras anômalas de posição
        - Maior precisão de `trip_start_time`, `trip_end_time` e `duration`
        - Verificação de qualidade das posições (freshness e gaps de extração); falha interrompe o pipeline com relatório e notificação imediata
        - Verificação de qualidade da extração de viagens (zero trips e low trip count) baseada na janela efetiva de extração
        - Resultado da persistência com contagem de viagens adicionadas e de viagens que já haviam sido salvas anteriormente
        - Relatório de qualidade consolidado com status das três fases ao final de cada execução, persistido no bucket de metadata e enviado ao `alertservice` via webhook
        ![Para mais informações:](./dags-dev/refinedfinishedtrips/README.md)
    - DAG refinedsynctripdetails: processo de carga dos detalhes de viagens canônicos da camada trusted para a camada refined, com adaptação leve para consumo da camada de visualização, especialmente em linhas circulares. Esta DAG é iniciada assim que a DAG gtfs é finalizada com sucesso através do uso do mecanismo datasets do Airflow. ![Para mais informações:](./dags-dev/refinedsynctripdetails/README.md)
    - DAG updatelatestpositions: processo de transformação para criação dos dados de última posição de cada ônibus na camada refined a partir dos dados da camada trusted. ![Para mais informações:](./dags-dev/updatelatestpositions/README.md)

- extractloadlivedata: microserviço que extrai os dados da API da SPTRANS a intervalos regulares, inicialmente a cada 2 minutos, mas possibilitando que este intervalo seja reduzido, o que não seria viável usando um job no Airflow, uma vez que atrasos na execução impactariam a precisão dos intervalos entre execuções da extração de dados, e salvando em um volume local e em seguida na camada raw, implementada usando o Minio. ![Para mais informações:](./extractloadlivedata/README.md)
- alertservice: microserviço que recebe resumos de qualidade via webhook de `transformlivedata`, `gtfs` e `extractloadlivedata`, e envia notificações por e-mail com alertas imediatos para falhas e alertas cumulativos para warnings baseados em limiares configuráveis por pipeline. ![Para mais informações:](./alertservice/README.md)
- Minio: utilizado para implementar as camadas raw, para armazenamento de dados brutos extraídos da API SPTrans e dados GTFS da SPTrans, e para os dados da camada trusted
- DuckDB: utilizado nos processos de transformação para fazer queries SQL diretamente nas tabelas armazenadas em formato Parquet na camada trusted, implementada através do Minio, com excelente performance, e sem requerer a implementação de motores SQL como o Presto, assim reduzindo a complexidade da infraestrutura. Utilizado também para análise exploratória de dados com intermédio do Jupyter
- Jupyter: usado para criar notebooks com a finalidade de viabilizar a exploração de dados na camada trusted armazenada no object storage. ![Para mais informações:](./jupyter/README.md)
- PostgreSQL: utilizado para armazenar a camada refined, porporcionando consultas com baixa latência na camada de visualização.
- PowerBI: utilizado para implementar a camada de visualização devido a sua flexibilidade, poder e larga adoção, consumindo dados diretamente da camada refined através do recurso de direct query ao PostgreSQL. ![Para mais informações:](./powerbi/README.md)

## Configuração
Um template de configuração está disponível em `.env.example` na raiz do projeto. Este arquivo contém todas as variáveis de ambiente necessárias para o funcionamento da infraestrutura (MinIO, Airflow, alertservice, extractloadlivedata).

## Para executar o Sptransinsights
Ao iniciar o projeto seguindo as instruções abaixo, deve-se em seguida, executar alguns comandos de inicialização que estão discriminados em cada subprojeto, especialmente:
- ![Airflow](./airflow/README.md)
- ![gtfs](./dags-dev/gtfs/README.md)
- ![transformlivedata](./dags-dev/transformlivedata/README.md)
- ![refinedfinishedtrips](./dags-dev/refinedfinishedtrips/README.md)
- ![updatelatestpositions](./dags-dev/updatelatestpositions/README.md)
- ![extractloadlivedata](./extractloadlivedata/README.md)
- ![alertservice](./alertservice/README.md)

Para iniciar o projeto:
Crie `.env` na raiz do projeto com base em `.env.example` preenchendo todos os campos:

```bash
cp .env.example .env
# Edite .env e preencha as credenciais necessárias
```
 
 Execute:
  docker compose up -d 

 Caso deseje iniciar serviços específicos:
 ```shell
  docker compose up -d minio
  docker compose up -d postgres
  docker compose up -d postgres_airflow airflow_webserver airflow_scheduler
  docker compose up -d extractloadlivedata
  docker compose up -d alertservice
  docker compose up -d jupyter
```


## Para monitorar os serviços ou efetuar configurações:

 Minio:
 http://localhost:9001/login

 Airflow:
 http://localhost:8080/

 Jupyter:
 http://localhost:8888/

## Configuração unificada de pipelines
Para padronizar a configuração entre pipelines e ambientes, o projeto utiliza o módulo `pipeline_configurator` (em `dags-dev/pipeline_configurator` e promovido para `airflow/dags/pipeline_configurator`).  
Ele fornece um único ponto de entrada para carregar configurações e conexões, com validação estruturada via schemas Pydantic específicos para cada pipeline.

Padrão de saída (contrato canônico):
- `general`: parâmetros funcionais da pipeline (ex.: buckets, tabelas, pastas, janelas de análise)
- `connections`: credenciais e endpoints (ex.: `object_storage`, `database`, `http`)
- `raw_data_json_schema`: schema de validação do formato dos dados JSON provenientes da ingestão de dados da API, quando aplicável 
- `data_expectations`: suite de expectations para checagem de qualidade do pipeline, quando aplicável 

O módulo resolve automaticamente o ambiente de execução:
- **Local/dev**: carrega arquivos JSON de configuração e `.env` locais
- **Airflow**: usa Variables e Connections do Airflow

Esse padrão reduz acoplamento entre serviços e garante consistência entre todos os pipelines.
![Para mais informações:](./dags-dev/pipeline_configurator/README.md)


## Ciclo de Desenvolvimento e Deployment

Para garantir a estabilidade do ambiente de produção, o projeto adota um fluxo de Promotion-based Development. No caso dos pipelines, todo o código é desenvolvido e testado no diretório `dags-dev` e, após validado, é "promovido" para o diretório de produção do Airflow (`airflow/dags`).

### Promoção de Pipelines (DAGs)
Para promover uma pipeline (ex: `transformlivedata`), utilize o script de gateway que realiza automaticamente a verificação de sintaxe (Linting), SAST e testes unitários antes de sincronizar os arquivos com a produção:

```shell
# Sintaxe: python scripts/promote_pipeline.py <nome_da_pipeline>
python scripts/promote_pipeline.py transformlivedata
```

Este script realiza os seguintes passos:
1. Análise estática: executa o `ruff` no subdiretório da pipeline.
2. SAST: executa o `bandit` (alta severidade) no subdiretório da pipeline.
3. Testes de unidade: executa o `pytest` na pasta `tests/` da pipeline (se existir).
4. Sincronização do código: sincroniza o subdiretório da pipeline para produção.
5. Sincronização de infraestrutura compartilhada: sincroniza as pastas `infra`, `quality` e `pipeline_configurator` para produção.

O número total de passos exibido é ajustado automaticamente com base na presença de testes.

As suítes de testes do projeto priorizam injeção explícita de dependências, uso de fakes reutilizáveis e cobertura de serviços e fluxos de orquestração, evitando `monkeypatch` como estratégia padrão.

### Deployment de Microserviços
Para atualizar e reiniciar um microserviço (ex: `extractloadlivedata`), utilize o script de deployment:

```shell
# Sintaxe: python scripts/deploy_service.py <nome_do_servico> <diretorio_do_servico>
python scripts/deploy_service.py extractloadlivedata extractloadlivedata
```

Este script realiza os seguintes passos:
1. Análise estática: executa o `ruff` no diretório do serviço.
2. SAST: executa o `bandit` (alta severidade) no diretório do serviço.
3. Testes de unidade: executa o `pytest` na pasta `tests/` do serviço (se existir).
4. Build da imagem Docker via Docker Compose.
5. Reinício do container via Docker Compose.

### Scripts auxiliares
Os scripts de deployment compartilham dois módulos auxiliares em `scripts/`:

| Módulo | Responsabilidade |
|---|---|
| `os_command_helper.py` | `run_command(command, error_msg)` — executa subprocessos com `shell=False` e reporta o exit code em caso de falha |
| `deploy_helpers.py` | `run_code_validations(folder, label, step_offset)` — executa linting, SAST e testes, retornando o número de passos consumidos para alinhamento do contador de steps |

![Para mais informações:](./scripts/README.md)

## Decisões arquiteturais (ADRs)
As principais decisões de design do projeto — tecnologias escolhidas, alternativas descartadas e tradeoffs aceitos — estão documentadas como Architecture Decision Records em `docs/adr/`. Os oito ADRs cobrem desde a escolha da arquitetura Medallion e do DuckDB como motor de transformação até o design da fila durável com PostgreSQL, o framework de qualidade de dados multi-camada, o design do alertservice e o workflow de promoção de pipelines.

![Índice de ADRs](./docs/adr/README.md)
