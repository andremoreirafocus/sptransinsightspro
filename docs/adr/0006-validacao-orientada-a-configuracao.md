# ADR-0006: Validação orientada a configuração (JSON Schema + Great Expectations)

**Data:** 2026-04-15  
**Status:** Aceito

## Contexto

O [ADR-0005](./0005-framework-de-qualidade-de-dados.md) estabelece que o `transformlivedata` deve tratar qualidade de dados como cidadã de primeira classe, com validação em múltiplos estágios: JSON Schema antes da transformação e Great Expectations após. Esta decisão pressupõe esse framework já adotado e trata de uma questão operacional específica: **onde as regras de validação devem viver**.

A abordagem mais simples — hardcodar as regras no código Python (`if "field" not in data: raise`, thresholds numéricos fixos) — funciona para regras estáticas, mas torna impossível atualizar as regras sem redeployment do pipeline, e embaralha a lógica de validação com a lógica de negócio. Em produção com Airflow, ajustar um único threshold exigiria Pull Request + merge + promoção.

## Decisão

Externalizar todas as regras de validação em arquivos de configuração carregados em runtime pelo `pipeline_configurator`:

- **JSON Schema (Draft 7)** para validação estrutural do payload raw: arquivo `transformlivedata_raw_data_json_schema.json`.
- **Great Expectations suite** para validação pós-transformação: arquivo `transformlivedata_data_expectations.json`.

As regras são carregadas como parte do contrato canônico de configuração (`config["raw_data_json_schema"]` e `config["data_expectations"]`) e interpretadas por bibliotecas especializadas (`jsonschema`, `great_expectations`), sem lógica de validação no código da pipeline.

Em Airflow (produção), as mesmas regras são carregadas de Airflow Variables — permitindo atualização sem redeployment.

## Alternativas consideradas

**Validação hardcodada em Python:** Simples e sem dependências. Porém, qualquer mudança nas regras exige modificação de código, lint, testes e redeployment. Em produção com Airflow, isso significa uma Pull Request + merge + promoção, mesmo para ajustar um threshold numérico.

**Validação apenas via Great Expectations (sem JSON Schema):** GX tem capacidade para validar estrutura de DataFrames após a transformação, mas não valida o payload JSON bruto antes da transformação. Uma falha estrutural na API causaria uma exceção genérica difícil de diagnosticar, em vez de uma mensagem de erro clara com o campo problemático identificado.

**Validação com Pydantic apenas:** Excelente para modelagem de objetos Python com validação de tipos, mas o overhead de criar e manter modelos Pydantic para schemas de API external é alto, e o framework não tem suporte nativo para expectativas estatísticas (ex: "pelo menos 95% dos registros devem ter trip_id não nulo").

## Consequências

**Positivo:**
- Regras de validação atualizadas em Airflow Variables sem redeployment — mudanças de threshold ou adição de campos não exigem ciclo de desenvolvimento.
- Separação clara entre lógica de negócio (código) e regras de qualidade (configuração).
- JSON Schema produz mensagens de erro precisas identificando exatamente qual campo violou qual regra.
- Great Expectations gera relatórios estruturados com métricas por expectativa, integrados ao relatório de qualidade do pipeline.
- O mesmo framework de validação pode ser reutilizado em outros pipelines, apenas trocando os arquivos de configuração.

**Negativo / Tradeoffs:**
- Dois sistemas de validação separados para manter (JSON Schema externo + suite GX), com curvas de aprendizado distintas.
- Arquivos de configuração precisam ser versionados junto com o código — uma mudança no schema de transformação precisa ser refletida nas expectations (e vice-versa); o sincronismo é responsabilidade do desenvolvedor, não há checagem automática.
- Great Expectations tem dependências pesadas (`great_expectations` adiciona ~200MB ao ambiente); para projetos com restrições de tamanho de imagem, seria necessário avaliar alternativas mais leves.
