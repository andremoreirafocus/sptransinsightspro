Este projeto proporciona aos seus usuários visualizações sobre as posições atuais de todos os ônibus rastreados pela SPTrans e traz informações sobre as viagens concluídas nas últimas horas.

Para isto, o sptransinsights, em intervalos regulares, extrai as posições de todos os ônibus em circulação em cada momento, armazenando estes dados para gerar informações sobre as viagens de cada veículo de cada linha e assim proporcionar insights aos seus usuários, permitindo que identifiquem os melhores momentos para fazerem suas viagens.

Este projeto faz uso de um monorepo com diferentes subprojetos que compõe o SPTransInsights.

Cada subprojeto possui um README com informações sobre o seu papel e os requisitos para o seu funcionamento.

## Arquitetura
![Diagrama da solução](./diagramasolucao.png)

Para implementar a solução foram adotados os componentes:
- Airflow: para orquestração de processos recorrentes do pipeline
- Kafka: para desacoplar o processo de ingestão dos dados das camadas de storage e transformação, permitindo que falhas nestas camadas 
- Minio: utilizado como camada de storage de dados brutos extraídos da API SPTrans e dados GTFS da SPTrans
- PostgreSQL: utilizado para armazenar as camadas trusted e refined, simplificando a arquitetura e viabilizando consultas performáticas para a camada de visualização
- PowerBI: utilizado para implementar a camada de visualização devido a sua flexibilidade, poder e larga adoção, consumindo dados diretamente da camada refined
- extractlivedata: microserviço que extrai os dados da API da SPTRANS a intervalos regulares, inicialmente a cada 2 minutos, mas possibilitando que este intervalo seja reduzido, o que não seria viável usando um job no Airflow, uma vez que atrasos na exeução impactariam a precisão dos intervalos entre execuções da extração de dados, publicando o dado bruto em um tópico do Kafka
- loadlivedata: microserviço que consome os dados brutos extraídos da API pelo ectractlivedata e salva na camada de storage, implementada usando o Minio
- gtfsextractload: processo que extrai os dados GTFS da SPTRANS e salva na camada de storage, implementada usando o Minio
- gtfstransform: processo que, a partir dos dados GTFS da SPTRANS, cria tabelas na camada trusted, implementada no Postgresql devido ao baixo volume dos dados e à facilidade de implementação, reduzindo a complexidade da arquitetura. Além disso, este processo cria uma tabela de dados de viagens utilizada para enriquecer os dados de posição extraídos da API SPTrans.
- transformlivedata: processo de transformação dos dados brutos de posição da camada raw em dados enriquecidos na camada trusted
- refinelivedata: processo de transformação para criação das informações de viagens na camada refined

## Para executar o Sptransinsights
Ao iniciar o projeto seguindo as instruções abaixo, deve-se em seguida, executar alguns comandos de inicialização que estão discriminados em cada subprojeto, especialmente:
- airflow
- gtfsextract
- transformlivedata
- refinelivedata

Para iniciar o projeto:
 docker compose up -d 

Caso deseje iniciar serviços específicos:
 docker compose up -d kafka-broker akhq
 docker compose up -d minio
 docker compose up -d postgres
 docker compose up -d postgres_airflow webserver scheduler
 docker compose up -d extraclivedata loadlivedata

 AKHQ (Kafka): 
 http://localhost:28080/ui/

 Minio:
 http://localhost:9001/login

 Airflow:
 http://localhost:8080/




