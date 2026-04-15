# ADR-0001: Arquitetura Medallion (Raw → Trusted → Refined)

**Data:** 2026-04-15  
**Status:** Aceito

## Contexto

O projeto precisa ingerir dados brutos da API SPTrans a cada 2 minutos e disponibilizá-los para uma camada de visualização (PowerBI) com baixa latência. Os dados precisam ser enriquecidos com informações GTFS, validados segundo regras de qualidade e disponibilizados em formatos diferentes dependendo do consumidor:

- Dados históricos completos precisam de consultas analíticas eficientes sobre grandes volumes.
- A visualização em tempo real precisa de respostas com latência de milissegundos.
- Em caso de falha em qualquer etapa de transformação, deve ser possível reprocessar os dados originais sem perda.

Em uma arquitetura flat — armazenar apenas o estado final processado — um bug de transformação ou uma mudança de regra de negócio implica perda irrecuperável dos dados originais.

## Decisão

Adotar três camadas de dados com responsabilidades e tecnologias distintas:

| Camada | Armazenamento | Formato | Responsabilidade |
|--------|--------------|---------|-----------------|
| **Raw** | MinIO (`raw`) | JSON (comprimido com Zstandard) | Preservação fiel dos dados originais da API |
| **Trusted** | MinIO (`trusted`) | Parquet (particionado por ano/mês/dia/hora) | Dados validados, enriquecidos e prontos para análise |
| **Refined** | PostgreSQL | Tabelas particionadas por hora | Consultas de baixa latência para visualização e APIs |

Dados adicionais gerados internamente (relatórios de qualidade, registros em quarentena) são armazenados em buckets dedicados (`metadata`, `quarantined`) no mesmo MinIO.

## Alternativas consideradas

**Arquitetura flat (apenas camada final):** Simplicidade operacional, mas sem possibilidade de reprocessamento; qualquer correção de bug exige reextração da API, que pode não estar disponível.

**Duas camadas (raw + refined diretamente):** Elimina a complexidade do trusted, mas mistura responsabilidades: a mesma tabela precisaria ser otimizada tanto para reprocessamento quanto para consulta, o que leva a compromissos ruins nos dois sentidos.

**Data warehouse tradicional (ex: Redshift, BigQuery):** Camada única gerenciada, mas introduz dependência de serviços de nuvem pagos que aumentam o custo operacional e a complexidade para um projeto executado localmente.

## Consequências

**Positivo:**
- Reprocessamento total possível a partir da camada raw, sem depender da API estar disponível.
- Cada camada usa a tecnologia ideal para seu padrão de acesso: compressão máxima no raw, consultas analíticas no trusted via DuckDB, baixa latência no refined via PostgreSQL.
- Separação clara de propriedade: falhas de transformação ficam na camada trusted, sem contaminar o raw.
- Lineage natural: é sempre possível rastrear um registro refined até o arquivo JSON original que o originou.

**Negativo / Tradeoffs:**
- Triplicação do armazenamento (o mesmo dado existe em três formas).
- Maior complexidade operacional: três sistemas de armazenamento distintos (MinIO × 2 buckets funcionais + PostgreSQL) precisam estar disponíveis.
- Pipelines de promoção entre camadas introduzem latência: um dado ingerido na raw só chega à refined após execução completa de `transformlivedata` e `refinedfinishedtrips`.
