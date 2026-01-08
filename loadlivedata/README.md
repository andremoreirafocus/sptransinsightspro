Este projeto consome dados de um tópico Kafka e salva em um bucket no Minio 

Configurações necessárias no arquivo .env do projeto:
INTERVALO = 120  # 2 minutos em segundos
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "bus_positions"
FOLDER = "raw"

Para instalar os requisitos:
pip install -r requirements.txt

Kafka:
    Foi necessário mudar a porta do akhq para 28080 no docker-compose-yaml para parar de conflitar com outros componentes
    docker compose up -d kafka-broker zookeeper akhq

    Para criar o tópico:
    docker exec -it kafka-broker /bin/bash
    kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic sptrans-positions;


Para executar: 
python ./main.py


