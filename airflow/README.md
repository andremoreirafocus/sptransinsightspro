Neste projeto estão contemplados os seguintes fluxos de acordo com o nome das pastas das DAGs e respectivos scripts:
- gtfs: importa arquivos do GTFS, transforma arquivos para tabelas correspondentes no Postgres no schema trusted e cria a tabela trip_details no schema trusted
- transformdatatotrusted: transforma conjunto de posicoes de onibus correspondente ao endpoint GET /posicoes, gerando, para cada veiculo, um registro na tabela trusted.positions com sua posicao e metadados enriquecidos com o conteúdo da tabela trusted.trip_details
- refinedfinishedtrips: identifica viagens dos veiculos nas ultimas horas baseado na sequencia temporal de posicoes de cada veiculo armazenada em trusted.positions
- updatelatestpositions: captura a última posição de cada veículo a partir do timestamp de extração do dado e salva na tabela refined.latest_positions
- refinedsynctripdetails: processo de sincronização dos detalhes de viagens da camada trusted para a camada refined para utilização pela camada de visualização. Esta DAG é iniciada assim que a DAG gtfs é finalizada com sucesso.
- maintainfinishedtrips: processo de limpeza automática das partições antigas da tabela de viagens finalizadas, executada por intermédio do SQLExecuteQueryOperator do Airflow


## Inicializndo o Ambiente

Para inicializar os serviços específicos do Airflow, utilize o comando:

```shell
docker compose up -d postgres_airflow webserver scheduler
```

## Criar o Usuário para Fazer Login no Airflow

Após inicializar o serviço, crie um usuário admin para acessar a interface do Airflow:

- Executar o comando abaixo no terminal

```shell
docker compose exec webserver airflow users create \
    --username admin \
    --firstname Firstname \
    --lastname Lastname \
    --role Admin \
    --email admin@example.com \
    --password admin
```

Para importar conexões e variáveis de ambiente usadas pelas DAGS
```shell
docker exec -it webserver bash
airflow connections import connections.json 
airflow variables import variables.json 
```

Para criar usuário do extractloadlivedata que invoca DAGs
```shell
airflow roles create API_Trigger
airflow roles add-perms API_Trigger -a can_read -r "DAG Runs"
airflow roles add-perms API_Trigger -a can_create -r "DAG Runs"
airflow users create \
    --username ingest_service \
    --firstname Ingest \
    --lastname Service \
    --role API_Trigger \
    --email ingest@example.com \
    --password ingest_password
```

 ## Para acessar o Airflow:
 http://localhost:8080/




