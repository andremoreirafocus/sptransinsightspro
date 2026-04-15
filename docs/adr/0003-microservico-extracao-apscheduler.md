# ADR-0003: Microserviço de extração com APScheduler

**Data:** 2026-04-15  
**Status:** Aceito

## Contexto

A extração de posições de ônibus da API SPTrans precisa ser executada em intervalos curtos e regulares (inicialmente 2 minutos) para capturar a movimentação com granularidade suficiente para análise de viagens. Dois requisitos são críticos:

1. **Precisão de intervalo**: desvios de segundos ou minutos na frequência de extração introduzem lacunas nos dados de posição, criando viagens com trechos ausentes ou medições de velocidade incorretas.
2. **Resiliência independente do Airflow**: se o scheduler do Airflow sofrer atraso (ex: filas cheias, restart) ou estiver indisponível, a extração não deve parar.

O Airflow scheduler opera com resolução mínima de 1 minuto (`schedule_interval`) e não garante precisão de segundo: um DAG agendado para cada minuto pode sofrer desvios de 10–30 segundos dependendo da carga do scheduler e do número de DAGs ativos. Além disso, a disponibilidade da extração ficaria acoplada à disponibilidade do Airflow.

## Decisão

Implementar a extração como um microserviço Docker independente (`extractloadlivedata`) usando **APScheduler** com `BlockingScheduler`.

O microserviço:
- Executa em seu próprio container Docker, completamente independente do Airflow.
- Usa APScheduler com `IntervalTrigger` para precisão de intervalo configurável via variável de ambiente (`EXTRACTION_INTERVAL_SECONDS`).
- Persiste os dados extraídos localmente no disco (`INGEST_BUFFER_PATH`) antes de tentar o upload ao MinIO — garantindo que nenhum dado seja perdido em caso de falha transitória do object storage.
- Registra solicitações de processamento no PostgreSQL (`to_be_processed.raw`) para que o Airflow (`orchestratetransform`) identifique quais arquivos precisam ser transformados.
- Em caso de indisponibilidade do PostgreSQL, persiste as solicitações em disco (`diskcache`) e as reenvia na próxima execução.

## Decisão complementar: PostgreSQL como fila de processamento

Além do mecanismo de agendamento, o microserviço precisa comunicar ao Airflow quais arquivos extraídos estão prontos para transformação. A decisão final é resultado de três abordagens avaliadas em sequência.

### Abordagem 1 descartada: DAG agendada a cada 2 minutos

A abordagem mais simples seria uma DAG `transformlivedata` com `schedule_interval='*/2 * * * *'`: a cada 2 minutos, a DAG buscaria o arquivo mais recente no MinIO e o transformaria.

O problema é que essa abordagem não tem memória de falhas: se o Airflow ou o MinIO ficarem indisponíveis durante um ciclo, aquela janela de 2 minutos simplesmente não é processada — e nenhum mecanismo nativo do Airflow garante que a execução perdida será reprocessada com o arquivo correto após a recuperação. O resultado é lacunas silenciosas na camada trusted que exigem reconciliação manual para identificar e reprocessar quais arquivos ficaram sem transformação.

### Abordagem 2 descartada: disparo direto via API do Airflow

Para resolver o problema de reconciliação, a segunda abordagem foi fazer o `extractloadlivedata` acionar a DAG `transformlivedata` diretamente via API REST do Airflow assim que o arquivo fosse salvo no MinIO (`NOTIFICATION_ENGINE=airflow`). Isso garante que cada arquivo salvo gera exatamente um trigger de transformação.

Porém, essa abordagem cria um acoplamento bidirecional frágil:

- **Risco de perda silenciosa nos dois sentidos**: se o disparo for confirmado pelo Airflow mas o microserviço falhar logo após (antes de registrar o sucesso localmente), ele tentará disparar novamente na próxima execução — podendo gerar processamento duplicado. O inverso também é possível: o microserviço registra o disparo como bem-sucedido, mas o Airflow rejeita a execução por atingir o limite de DAG runs concorrentes.
- **Acoplamento de disponibilidade**: se o Airflow estiver indisponível, o microserviço fica retentando o disparo indefinidamente sem certeza de que o arquivo será processado. Quando o Airflow voltar, não há garantia de que todos os disparos pendentes serão reprocessados na ordem correta.
- **Acoplamento ao nome da DAG**: o microserviço precisaria ter o nome da DAG de transformação configurado (`transformlivedata-v8`). Uma nova versão da DAG exigiria atualização da configuração do `extractloadlivedata` e reinício do container — acoplando o ciclo de release de dois serviços independentes. Com a fila, apenas a DAG `orchestratetransform` (gerenciada inteiramente no ambiente Airflow) conhece o nome da DAG alvo; o microserviço de ingestão é completamente agnóstico à versão da transformação.
- **Sem garantia de ordenação**: múltiplos disparos concorrentes sem controle de fila podem causar condições de corrida na camada trusted.

### Decisão: tabela PostgreSQL como fila durável

O `extractloadlivedata` registra cada arquivo extraído como uma linha na tabela `to_be_processed.raw` com `processed = false`. O Airflow (`orchestratetransform`) faz polling dessa tabela periodicamente, aciona `transformlivedata` para cada arquivo pendente em ordem de criação e, somente após a transformação concluída com sucesso, marca o registro como `processed = true`.

Essa arquitetura preserva todas as garantias ACID do PostgreSQL:

- **Disponibilidade do Airflow**: como o PostgreSQL é o backend do próprio Airflow, se o banco estiver indisponível o Airflow também estará — não há janela em que o Airflow está disponível mas o banco não. Quando ambos voltam, o `orchestratetransform` retoma o polling naturalmente e processa todos os arquivos pendentes acumulados durante a indisponibilidade, sem nenhuma intervenção manual.
- **Disponibilidade do `extractloadlivedata`**: se o banco estiver indisponível no momento da extração, as solicitações são persistidas em disco (`diskcache`) e reinseridas na tabela quando o banco voltar — sem perda de registros.
- **Garantia de processamento exatamente uma vez**: cada arquivo só sai da fila quando a transformação confirma sucesso. Falhas em qualquer estágio do `transformlivedata` deixam o registro como `processed = false`, garantindo reprocessamento automático sem duplicação.
- **Ordenação natural**: o `orchestratetransform` processa os registros em ordem de criação, evitando condições de corrida.

O suporte a disparo direto via API do Airflow é mantido como mecanismo alternativo configurável (`NOTIFICATION_ENGINE=airflow`) para cenários de desenvolvimento ou diagnóstico, mas não é o caminho de produção.

---

## Alternativas consideradas para o agendamento

**DAG Airflow com sensor de intervalo:** A abordagem mais simples — um DAG com `schedule_interval='*/2 * * * *'`. Porém, a precisão de agendamento do Airflow depende da carga do scheduler, e a disponibilidade da extração ficaria acoplada à disponibilidade do Airflow. Uma reinicialização do scheduler para operações como upgrades interromperia a extração por minutos.

**Cron job no sistema operacional:** Precisão confiável sem dependência do Airflow. Porém, requereria acesso ao crontab do host (fora do Docker Compose), tornando a solução menos portável e mais difícil de configurar em novos ambientes.

**Kubernetes CronJob:** Solução robusta para produção em nuvem, com controle de concorrência e retry policies. Porém, introduz dependência de um cluster Kubernetes — incompatível com a proposta de execução local via Docker Compose.

## Consequências

**Positivo:**
- Precisão de intervalo de extração independente do estado do Airflow.
- Nenhum arquivo extraído é perdido: a fila PostgreSQL garante que todo arquivo pendente será processado exatamente uma vez, mesmo após indisponibilidade prolongada de qualquer serviço.
- Resiliência de quatro camadas: API (retry com backoff), MinIO (buffer local no disco), PostgreSQL (cache em disco via diskcache), fila durável (tabela `to_be_processed.raw`).
- Intervalo configurável sem redeployment (`EXTRACTION_INTERVAL_SECONDS`).
- Deploy e rollback independentes: o microserviço pode ser atualizado sem tocar no Airflow.

**Negativo / Tradeoffs:**
- Serviço adicional para monitorar, fazer deploy e manter (Dockerfile, variáveis de ambiente, volume de disco).
- Divide o monitoramento: falhas de extração ficam nos logs do container, não no Airflow UI.
- A tabela `to_be_processed.raw` é um ponto central de coordenação — corrupção ou crescimento descontrolado dessa tabela (ex: falha persistente no `transformlivedata` sem limpeza) pode acumular registros indefinidamente, exigindo monitoramento de backlog.
