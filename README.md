Este projeto proporciona aos seus usuários visualizações sobre as posições atuais de todos os ônibus rastreados pela SPTrans e apresenta informações sobre as viagens concluídas nas últimas horas.

Para isto, o Sptransinsights, em intervalos regulares, extrai as posições de todos os ônibus em circulação em cada momento, armazenando estes dados para gerar informações sobre as viagens de cada veículo de cada linha e, assim, proporcionar insights aos seus usuários, permitindo que identifiquem os melhores momentos para fazerem suas viagens.

Um framework completo de qualidade de dados, com validações orientadas a configuração (JSON Schema e Great Expectations), quarentena de registros inválidos e geração de relatório de qualidade com resumo e detalhes do processamento proporcionando informações de observabilidade é aplicado nos pipelines mais críticos.

Resumos de qualidade dos pipelines principais e do microserviço de ingest são enviados via webhook para um microserviço de geração de alertas, responsável por emitir notificações e alertas por e-mail com alertas imediatos para falhas e alertas cumulativos para warnings.

A solução adota o conceito de monorepo e é composta por alguns subprojetos. Cada um deles possui um README com informações sobre o seu papel e os requisitos para o seu funcionamento.

## Arquitetura
As principais decisões de design do projeto — tecnologias escolhidas, alternativas descartadas e tradeoffs aceitos — estão documentadas como Architecture Decision Records em `docs/adr/`. Os ADRs cobrem desde a escolha da arquitetura Medallion e do DuckDB como motor de transformação até o design da fila durável com PostgreSQL, o framework de qualidade de dados multi-camada, o design do alertservice e o workflow de promoção de pipelines.

[Índice de ADRs](./docs/adr/README.md)
![Diagrama da solução](./diagrama_solucao.png)

Para implementar a solução foram adotados os componentes:
- Docker e Docker Compose: utilizados para empacotar e executar os componentes da solução em containers, além de orquestrar a subida do ambiente local com serviços como Airflow, PostgreSQL, MinIO, Jupyter, extractloadlivedata e alertservice, reduzindo o esforço de configuração manual e aumentando a reprodutibilidade do ambiente.
- [extractloadlivedata](./extractloadlivedata/README.md): microserviço que extrai os dados da API da SPTRANS a intervalos regulares, inicialmente a cada 2 minutos, mas possibilitando que este intervalo seja reduzido, o que não seria viável usando um job no Airflow, uma vez que atrasos na execução impactariam a precisão dos intervalos entre execuções da extração de dados, e salvando em um volume local e em seguida na camada raw, implementada usando o Minio.
- [alertservice](./alertservice/README.md): microserviço que recebe resumos de qualidade via webhook do microserviço de ingest e dos pipelines, e envia notificações por e-mail com alertas imediatos para falhas e alertas cumulativos para warnings baseados em limiares configuráveis por pipeline.
- Minio: utilizado para implementar a camada raw, para armazenamento de dados brutos extraídos da API SPTrans e dados GTFS da SPTrans, e para a camada trusted, com dados trasnformados e com qualidade checada
- DuckDB: utilizado nos processos de transformação para fazer queries SQL diretamente nas tabelas armazenadas em formato Parquet na camada trusted, implementada através do Minio, com excelente performance, e sem requerer a implementação de motores SQL como o Presto, assim reduzindo a complexidade da infraestrutura. Utilizado também para análise exploratória de dados com intermédio do Jupyter
- [Jupyter](./jupyter/README.md): usado para criar notebooks com a finalidade de viabilizar a exploração de dados na camada trusted armazenada no object storage.
- PostgreSQL: utilizado para armazenar a camada refined, porporcionando consultas com baixa latência na camada de visualização.
- [PowerBI](./powerbi/README.md): utilizado para implementar a camada de visualização devido a sua flexibilidade, poder e larga adoção, consumindo dados diretamente da camada refined através do recurso de direct query ao PostgreSQL.
- [Airflow](./airflow/README.md): para orquestração de processos recorrentes do pipeline através de diversas DAGs utilizando o Python Operator. O ambiente de produção para este módulo se encontra na pasta airflow. 
O ambiente de desenvolvimento se encontra na pasta [dags-dev](./dags-dev/README.md)
Detalhes sobre as DAGS:
    - [DAG gtfs](./dags-dev/gtfs/README.md): processo composto de 3 etapas principais, com checagem de qualidade ao longo do fluxo, geração de um relatório consolidado por execução e publicação de um Airflow Dataset ao final de uma execução bem sucedida para disparar a sincronização downstream para a camada refined.   
        - extração e carga de arquivos: extrai os dados GTFS da SPTrans, valida os arquivos e salva na camada raw, gerando diagnóstico consolidado em caso de falha.
        - transformação: converte as tabelas base do GTFS para Parquet, aplica checagem de qualidade (Great Expectations quando configurado), usa staging antes da promoção para o caminho final e quarentena em caso de falha.
        - enrichment: cria a tabela `trip_details` em staging, valida sua qualidade e promove ou quarentena o artefato conforme o resultado.
    - [DAG transformlivedata](./dags-dev/transformlivedata/README.md): processo de transformação dos dados brutos de posição da camada raw em dados enriquecidos e confiáveis na camada trusted, com quarentena de registros inválidos e geração de relatório de qualidade consolidado.
        - Objetos configuráveis principais:
        - JSON Schema para validação estrutural do dado bruto
        - suite Great Expectations para validação pós-transformação
    - [DAG orchestratetransform](./dags-dev/orchestratetransform/README.md): processo de identificação de dados de posição dos ônibus pendentes de processamento e que dispara a DAG de transformação.
    - [DAG refinedfinishedtrips](./dags-dev/refinedfinishedtrips/README.md): processo de transformação para criação das viagens finalizadas na camada refined a partir dos dados enriquecidos da camada trusted, com checagens de qualidade sobre posições, extração e persistência, além de geração de relatório consolidado e notificação via webhook.
        - A partir da versão 6 desta pipeline, a DAG no Airflow deixa de depender de agendamento por cron e passa a ser disparada por um Airflow Dataset emitido pelo pipeline `transformlivedata`, o que maximiza o freshness das viagens finalizadas calculadas na camada refined, que passam a ser atualizadas logo após a publicação bem sucedida dos dados transformados, e simplifica a manutenção ao remover o acoplamento entre cron schedules upstream e downstream.
    - [DAG refinedsynctripdetails](./dags-dev/refinedsynctripdetails/README.md): processo de carga dos detalhes de viagens canônicos da camada trusted para a camada refined, com adaptação leve para consumo da camada de visualização, especialmente em linhas circulares. Esta DAG é iniciada assim que a DAG gtfs é finalizada com sucesso através do uso do mecanismo datasets do Airflow.
    - [DAG updatelatestpositions](./dags-dev/updatelatestpositions/README.md): processo de transformação para criação dos dados de última posição de cada ônibus na camada refined a partir dos dados da camada trusted. A partir da versão 4 deste pipeline, a DAG no Airflow deixa de depender de agendamento por cron e passa a ser disparada por um Airflow Dataset emitido pelo pipeline `transformlivedata`, o que maximiza o freshness da tabela `refined.latest_positions`, que passa a ser atualizada logo após a publicação bem sucedida dos dados transformados, e simplifica a manutenção ao remover o acoplamento entre cron schedules upstream e downstream.

### Orquestração por eventos no Airflow
O diagrama abaixo complementa a descrição das DAGs mostrando a orquestração orientada a eventos atualmente implementada no Airflow por meio de Datasets.

![Diagrama de eventos do Airflow](./diagrama_eventos_airflow.png)

- `gtfs` publica o Dataset `gtfs://trip_details_ready`
- `refinedsynctripdetails` é disparada por esse Dataset, ou seja, é executado automaticamente após a conclusão com sucesso do pipeline gtfs
- `transformlivedata` publica o Dataset `sptrans://trusted/transformed_positions_ready`
- `refinedfinishedtrips` e `updatelatestpositions` são disparadas por esse Dataset , ou seja, são executados automaticamente após a conclusão com sucesso do pipeline transformlivedata


## Configuração
Um template de configuração está disponível em `.env.example` na raiz do projeto. Este arquivo contém todas as variáveis de ambiente necessárias para o funcionamento da infraestrutura (MinIO, Airflow, alertservice, extractloadlivedata).

## Para executar o Sptransinsights
Ao iniciar o projeto seguindo as instruções abaixo, deve-se em seguida, executar alguns comandos de inicialização que estão discriminados em cada subprojeto, especialmente:
- [Airflow](./airflow/README.md)
- [gtfs](./dags-dev/gtfs/README.md)
- [transformlivedata](./dags-dev/transformlivedata/README.md)
- [refinedfinishedtrips](./dags-dev/refinedfinishedtrips/README.md)
- [updatelatestpositions](./dags-dev/updatelatestpositions/README.md)
- [extractloadlivedata](./extractloadlivedata/README.md)
- [alertservice](./alertservice/README.md)

Para iniciar o projeto:
Crie `.env` na raiz do projeto com base em `.env.example` preenchendo todos os campos:

```bash
cp .env.example .env
# Edite .env e preencha as credenciais necessárias
```
 
O caminho recomendado para iniciar a plataforma sem falhas prematuras de serviços dependentes de banco é:

```bash
cd automation
./platform_bootstrap_and_start.sh
```

Esse script:
- sobe primeiro `airflow_postgres`, `postgres` e `minio`
- executa o bootstrap do MinIO
- executa o bootstrap dos artefatos obrigatórios de banco
- sobe a camada de aplicação do Airflow e executa seu bootstrap
- sobe o restante da plataforma somente após a conclusão do bootstrap

Caso deseje subir a plataforma manualmente, o comando base continua sendo:

```bash
docker compose up -d
```

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
[Mais informações sobre o pipeline configurator](./dags-dev/pipeline_configurator/README.md)


## Ciclo de Desenvolvimento e Deployment

Para garantir a estabilidade do ambiente de produção, o projeto adota um fluxo de Promotion-based Development. No caso dos pipelines, todo o código é desenvolvido e testado no diretório `dags-dev` e, após validado, é "promovido" para o diretório de produção do Airflow (`airflow/dags`).

### Promoção de Pipelines (DAGs)
Para promover uma pipeline (ex: `transformlivedata`), utilize o script de gateway que realiza automaticamente a verificação de sintaxe (Linting), SAST e testes unitários antes de sincronizar os arquivos com a produção:

```shell
# Sintaxe: python automation/promote_pipeline.py <nome_da_pipeline>
python automation/promote_pipeline.py transformlivedata
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
# Sintaxe: python automation/deploy_service.py <nome_do_servico> <diretorio_do_servico>
python automation/deploy_service.py extractloadlivedata extractloadlivedata
```

Este script realiza os seguintes passos:
1. Análise estática: executa o `ruff` no diretório do serviço.
2. SAST: executa o `bandit` (alta severidade) no diretório do serviço.
3. Testes de unidade: executa o `pytest` na pasta `tests/` do serviço (se existir).
4. Build da imagem Docker via Docker Compose.
5. Reinício do container via Docker Compose.

### Scripts auxiliares
Os scripts de deployment compartilham dois módulos auxiliares em `automation/`:

| Módulo | Responsabilidade |
|---|---|
| `os_command_helper.py` | `run_command(command, error_msg)` — executa subprocessos com `shell=False` e reporta o exit code em caso de falha |
| `deploy_helpers.py` | `run_code_validations(folder, label, step_offset)` — executa linting, SAST e testes, retornando o número de passos consumidos para alinhamento do contador de steps |

[Mais informações sobre os scripts](./automation/README.md)
