## Objetivo
O Airflow é o orquestrador de fluxos de trabalho utilizado neste projeto para gerenciar a execução dos pipelines por meio de schedules pré-definidos ou de eventos.

As configurações de todos os pipelines são armazenadas como Variables do Airflow, que são extraídas através de chamadas de API `Variable.get` executadas pelo módulo [pipeline configurator](./dags/pipeline_configurator/README.md), conforme pode ser visto abaixo:

![Variables no Airflow](./airflow_variables.png)

As credenciais usadas por todos os pipelines são armazenadas como Connections do Airflow, que são extraídas através de chamadas de API `BaseHook.get_connection` executadas pelo módulo [pipeline configurator](./dags/pipeline_configurator/README.md).

![Connections no Airflow](./airflow_connections.png)

[Mais informações sobre o papel do Airflow na arquitetura do projeto e as DAGS implementadas](../README.md)

Esta abordagem facilita e centraliza a administração de configurações e credenciais usadas pelos pipelines já que alterações e importações podem ser feitas por usuários do Airflow com poderes administrativos.

## Inicializando o ambiente
Para inicializar os serviços específicos do Airflow, utilize:

```shell
docker compose up -d airflow_postgres airflow_webserver airflow_scheduler
```

O caminho operacional recomendado para bootstrap do Airflow é usar:

```shell
./automation/bootstrap_airflow_app.sh
```

Esse script:
- aguarda o CLI do Airflow ficar utilizável
- garante a criação do usuário admin definido no `.env`
- importa as variables de bootstrap
- importa as connections de bootstrap

## Comandos manuais de fallback
Se você precisar executar manualmente o bootstrap do Airflow, os comandos abaixo continuam disponíveis.

### Criar o usuário para login no Airflow
Após inicializar os serviços, crie um usuário admin para acessar a interface do Airflow:

```shell
docker compose exec airflow_webserver airflow users create \
    --username admin \
    --firstname Firstname \
    --lastname Lastname \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### Importar connections e variables
Para importar as connections e variables usadas pelas DAGs:

O bootstrap é dividido em:
- `connections.json`: template versionado com a estrutura base das Connections do Airflow
- `variables.json`: variables consolidadas de pipelines que não exigem arquivos dedicados adicionais neste bootstrap
- arquivos JSON dedicados por pipeline: usados quando a configuração é mantida separadamente

```shell
docker compose exec airflow_webserver bash
python /opt/airflow/dags/../variables_and_connections/render_airflow_connections.py \
  /opt/airflow/variables_and_connections/connections.json \
  /opt/airflow/variables_and_connections/generated_connections.json
airflow connections import variables_and_connections/generated_connections.json
airflow variables import variables_and_connections/variables.json

# transformlivedata
airflow variables import variables_and_connections/transformlivedata_general.json
airflow variables import variables_and_connections/transformlivedata_data_expectations.json
airflow variables import variables_and_connections/transformlivedata_raw_data_json_schema.json

# gtfs
airflow variables import variables_and_connections/gtfs_general.json
airflow variables import variables_and_connections/gtfs_data_expectations_stops.json
airflow variables import variables_and_connections/gtfs_data_expectations_stop_times.json
airflow variables import variables_and_connections/gtfs_data_expectations_trip_details.json

# refinedfinishedtrips
airflow variables import variables_and_connections/refinedfinishedtrips_general.json
```

O arquivo [variables.json](/home/andrem/projetos/sptransinsightspro/airflow/variables_and_connections/variables.json) já contém:
- `orchestratetransform_general`
- `updatelatestpositions_general`
- `refinedsynctripdetails_general`

Ou seja, essas três configurações já são carregadas no import consolidado e não exigem arquivos adicionais separados nesta etapa.

## Integração do Airflow com o serviço de ingest 
O ingest de dados da API é efetuado pelo serviço de ingest [extractloadlivedata](../extractloadlivedata/README.md)
Este serviço se integra da seguinte forma: 
- para cada arquivo gerado pelo serviço e salvo na camada raw, um request de processamento é salvo na tabela `to_be_processed.raw` do banco de dados do Airflow
- a DAG `orchestratetransform` identifica requests pendentes e dispara a DAG de transformação
- as DAGs que consomem os dados transformados funcionam orientadas a evento usando Airflow Datasets

Com isto a resiliência implementada pelo fluxo de ingest e de transformação consegue se recuperar de falhas tanto no Airflow quanto no object storage.

Alternativamente pode-se configurar o serviço de ingest para efetuar o disparo direto da DAG de transformação via API do Airflow, embora este não seja o caminho adotado para a produção.

## Configuração opcional de trigger via API
Para possibilitar que o `extractloadlivedata` dispare a execução de DAGs diretamente via API do Airflow:

```shell
airflow roles create API_Trigger
airflow roles add-perms API_Trigger -a can_read -r "DAGs"
airflow roles add-perms API_Trigger -a can_read -r "DAG Runs"
airflow roles add-perms API_Trigger -a can_create -r "DAG Runs"
airflow roles add-perms API_Trigger -a can_read -r "DAG:transformlivedata-v10"
airflow roles add-perms API_Trigger -a can_edit -r "DAGs"
airflow users create \
    --username ingest_service \
    --firstname Ingest \
    --lastname Service \
    --role API_Trigger \
    --email ingest@example.com \
    --password ingest_password
```

Feito isso, para finalizar a integração via disparo por API, é necessário configurar o serviço de ingest para utilizar as credenciais criadas, além do nome da DAG a ser disparada pelo Airflow.

## Acessar o Airflow
Interface web:

http://localhost:8080/
