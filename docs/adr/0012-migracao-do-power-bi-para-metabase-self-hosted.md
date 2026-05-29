# ADR-0012: Migração do Power BI para Metabase self-hosted

**Data:** 2026-05-29  
**Status:** Proposto

## Contexto

A camada de visualização analítica do projeto hoje depende do Power BI, enquanto os dados já são produzidos e disponibilizados no ambiente containerizado local.

A implementação atual cria limitações arquiteturais e operacionais:
- O custo recorrente de licenciamento se mantém para consumo e evolução de dashboards.
- A evolução da camada analítica permanece dependente de uma plataforma proprietária externa e do seu roadmap.
- A autoria completa de relatórios fica restrita ao Power BI Desktop em ambiente Windows.
- Em uma futura execução em nuvem, há potencial aumento de custo com saída de dados (`data transfer out`) para consumo em ferramenta externa.

Por outro lado, o projeto já dispõe de:
- banco analítico PostgreSQL (`postgres`) com dados curados na camada refined;
- operação padronizada por Docker Compose;
- artefatos versionados de bootstrap e configuração.

## Decisão

Substituir o Power BI pelo Metabase self-hosted como plataforma padrão de visualização analítica.

A plataforma passa a operar com as seguintes fronteiras arquiteturais:

- O Metabase consome dados curados da camada refined no PostgreSQL analítico (`postgres`).
- O banco de metadados/orquestração do Airflow (`airflow_postgres`) permanece isolado da camada de BI.
- A adoção da plataforma de BI e a migração dos dashboards são tratadas como frentes separadas: este ADR cobre a decisão de plataforma; o plano de migração cobre a conversão funcional de painéis.

## Alternativas consideradas

**Manter Power BI como plataforma principal**

Foi descartado por manter custo recorrente de licenciamento, dependência de plataforma externa proprietária e limitação operacional de autoria completa em ambiente Windows.

**Estratégia híbrida permanente (Power BI + Metabase)**

Foi descartada como alvo arquitetural por manter duplicidade operacional e de governança de dashboards, reduzindo ganhos de simplificação e padronização.

**Adotar ferramenta proprietária de BI na nuvem como substituição direta**

Foi descartada neste momento por manter dependência de licenciamento externo e não atender ao objetivo de controle operacional self-hosted compatível com o estágio atual do projeto.

## Consequências

**Positivo:**
- Eliminação de custo de licenciamento de BI proprietário.
- Redução de custo potencial com saída de dados em futura operação cloud ao manter consumo analítico na mesma infraestrutura.
- Remoção de dependência de plataforma proprietária externa para evolução da camada de visualização.
- Acesso web para consumo e autoria, eliminando dependência de cliente desktop pesado e reduzindo fricção de suporte/onboarding em equipes Linux/macOS/Windows.
- Maior aderência ao stack atual por integração direta com o PostgreSQL analítico já existente.
- Maior controle operacional e de governança da plataforma de BI sob responsabilidade direta do próprio projeto.
- Reprodutibilidade operacional por meio de artefatos versionados de infraestrutura e setup.
- Melhor preparação para evolução futura em AWS com padrão de execução containerizado reaproveitável.

**Negativo / Tradeoffs:**
- A equipe passa a assumir responsabilidade direta por operação, atualização e disponibilidade da plataforma de BI.
- A migração exige esforço de conversão e validação de paridade dos dashboards existentes.
- Diferenças funcionais entre Power BI e Metabase podem exigir adaptação de modelagem e visualizações em parte dos painéis.
