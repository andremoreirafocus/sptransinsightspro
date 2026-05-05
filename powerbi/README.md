# 📊 Painel de Insights SPTrans (Power BI)

Este diretório contém o relatório do Power BI utilizado para visualizar os dados processados no pipeline. 
O dashboard faz consultas às tabelas geradas pelas funções de refinamento de dados, através do mecanismo de Direct Query.

---
### 🎯 Conteúdo e Dados do Dashboard

O dashboard consome os dados da camada refined, e apresenta métricas e KPIs gerados pelos processos desenvolvidos no subprojeto `refinelivedata`e apresentados a seguir:

1.  **Monitoramento de Viagens**:
    * Visualização do horário de início, horário de final e duração de cada uma das viagens realizadas nas últimas horas
    * Visualização da duração média das viagens nas últimas horas por período do dia
    * Filtragem por viagem, caracterizada por linha e sentido (0 ou 1) e, se desejado, por veiculo
    * Construído pela [DAG refiniedfinishedtrips no Airflow](../airflow/refinedfinishedtrips-v2.py) a partir de consultas à tabela de viagens finalizadas na camada refinada

2.  **Rastreamento em Tempo Real**:
    * **Mapa de Posição**: Identificação de posição dos ônibus no momento da consulta
      * Filtragem por viagem, caracterizada por linha e sentido (0 ou 1) e, se desejado, por veiculo
      * Construído pela [DAG refiniedfinishedtrips no Airflow](../airflow/updatetlatestpositions-v1.py) a partir de consultas à tabela de posições mais recentes na camada refinada

### 🖼️ Visualização do Dashboard
![Screenshot do Dashboard](./dashboardview.png)

### 🛠️ Como Utilizar
1.  Certifique-se de que as tabelas no banco de dados PostgreSQL foram criadas e populadas pelo processo `refinelivedata`.
2.  Abra o arquivo [sptrans-dashboard.pbix](./sptrans-dashboard.pbix) no Power BI Desktop.
3.  Atualize as credenciais de conexão para apontar para o seu container PostgreSQL local.
