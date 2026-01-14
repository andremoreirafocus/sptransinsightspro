Este projeto faz uso de um monorepo com diferentes subprojetos que compõe o SPTransInsights

Olhe o README de cada subprojeto para mais informações

Requisitos para o funcionamento do projeto:
 docker compose up -d kafka-broker akhq
 docker compose up -d minio
 docker compose up -d postgres
 docker compose up -d postgres_airflow webserver scheduler
 
