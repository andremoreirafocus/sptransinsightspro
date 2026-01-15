Este projeto faz uso de um monorepo com diferentes subprojetos que compõe o SPTransInsights

Olhe o README de cada subprojeto para mais informações

Requisitos para o funcionamento do projeto:
 docker compose up -d kafka-broker akhq
 docker compose up -d minio
 docker compose up -d postgres
 docker compose up -d postgres_airflow webserver scheduler
docker compose up -d metabase

 AKHQ (Kafka): 
 http://localhost:28080/ui/

 Minio:
 http://localhost:9001/login

 Airflow:
 http://localhost:8080/

 Metabase:
 http://localhost:3000/


# #############################################################
TO DO:

Em gtfstransform:
    criar a tabela trusted.terminais com os campos e computar para consulta pelo livedatatransform:
        trip_id
        trip_distance
        tp_stop_id
        tp_lat
        tp_lon
        ts_stop_id
        ts_lat
        ts_lon


Em livedatatransform:
    Na trusted.positions incluir os campos e computar os campos
        trip_id 
        tp_distance
        ts_distance


Em um novo processo refinelivedata:
Criar as tabelas refined.viagens-concluidas e viagens-em-andamento
    trip_id
    vehicle_id
    trip_start_time
    trip_end_time
    duration
    average_speed

Quando a distancia ao tp for superior ao limite de tolerancia (exemplo 30 metros) a viagem começa
Quando a distancia ao ts for inferior ao limite de tolerancia (exemplo 30 metros) a viagem termina

Adicionar docker-compose.yaml e config de serviços
Dockerizar algum processo que esteja sendo em Python sem airflow



