# 📊 Painel de Insights SPTrans (Power BI)

Este diretório contém o relatório do Power BI que visualiza os dados processados pelo pipeline de engenharia. O dashboard utiliza diretamente as tabelas geradas pelas funções de refinamento de dados.

---
### 🎯 Conteúdo e Dados do Dashboard

O dashboard consome os dados gerados no diretório `refinelivedata`, especificamente os resultados de:

1.  **Monitoramento de Viagens (`extract_trips_for_all_Lines_and_vehicles`)**:
    * Visualização de linhas e duração das viagens mais recentes
    * Apresentação da duração média das viagens
    * Os dados são apresentados de acordo os filtros utilizados

2.  **Rastreamento em Tempo Real (`update_latest_positions`)**:
    * **Mapa de Posição**: Identificação de posição dos ônibus no momento da consulta

---

### 🖼️ Visualização do Dashboard
![Screenshot do Dashboard](./dashboardview.png)

### 🛠️ Como Utilizar
1.  Certifique-se de que as tabelas no banco de dados PostgreSQL foram criadas e populadas pelo serviço `refinelivedata`.
2.  Abra o arquivo `.pbix` no Power BI Desktop.
3.  Atualize as credenciais de conexão para apontar para o seu container PostgreSQL local.

---
*Desenvolvido como parte do projeto [SPTrans Insights](https://github.com/andremoreirafocus/sptransinsights).*