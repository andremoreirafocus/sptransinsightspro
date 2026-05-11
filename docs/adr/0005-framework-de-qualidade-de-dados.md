# ADR-0005: Framework de qualidade de dados multi-camada

**Data:** 2026-04-15  
**Status:** Aceito

## Contexto

O pipeline `transformlivedata` processa dados de posição de ônibus extraídos de uma API externa (SPTrans) que está fora do controle do projeto. Características do dado:

- **Volume:** dezenas de milhares de registros por execução (posições de todos os ônibus em circulação, a cada 2 minutos).
- **Origem não confiável:** a API pode retornar registros com campos ausentes, valores fora de domínio (coordenadas inválidas, IDs de linha inexistentes no GTFS), ou estrutura ligeiramente alterada sem aviso.
- **Uso downstream crítico:** os dados transformados alimentam análises de viagens concluídas (`refinedfinishedtrips`) e a camada de visualização em tempo real. Dados corrompidos na camada trusted se propagam silenciosamente até o usuário final.
- **Necessidade de observabilidade:** sem visibilidade sobre o que acontece dentro do pipeline, é impossível distinguir "a API está com problema hoje" de "introduzi um bug na transformação" de "o dado sempre teve esse nível de qualidade".

A abordagem mais simples — executar a transformação e falhar toda a execução se qualquer erro ocorrer — descartaria execuções inteiras por causa de poucos registros inválidos, degradando artificialmente a disponibilidade do pipeline. O oposto — ignorar erros silenciosamente — mascararia problemas reais de qualidade de dados.

## Decisão

Implementar um **framework de qualidade de dados com quatro responsabilidades complementares**, executadas como estágios sequenciais dentro do `transformlivedata`:

### 1. Validação estrutural (pré-transformação)
JSON Schema valida o payload bruto recebido da API antes de qualquer transformação. Detecta mudanças no contrato da API (campos removidos, tipos alterados) antes que causem erros opacos na transformação. Se o payload não respeita o schema, o pipeline falha imediatamente com mensagem precisa — não há dado útil a salvar.

### 2. Transformação com coleta de issues
Durante a transformação (`transform_positions.py`), problemas semânticos são **coletados, não lançados**: registros com `trip_id` inexistente, coordenadas fora de domínio, ou distâncias incalculáveis são marcados com o motivo do problema. A transformação continua para os demais registros.

### 3. Quarentena de registros inválidos
Registros que falharam validações são isolados em um bucket dedicado (`quarantined`) em formato Parquet, com uma coluna adicional `quarantine_reason`. Isso garante:
- Os registros problemáticos não chegam à camada trusted.
- Os registros não são descartados — podem ser inspecionados, investigados e eventualmente reprocessados se o problema for corrigível.

### 4. Validação estatística pós-transformação (Great Expectations)
Após a transformação, expectativas estatísticas são verificadas sobre o DataFrame resultante (ex: taxa mínima de registros com `trip_id` válido, ausência de nulos em colunas críticas). Violações são registradas no relatório de qualidade; registros que falham nas expectativas são adicionalmente quarentenados.

### Relatório de qualidade
Cada execução gera um relatório JSON com duas seções:
- **`summary`**: status da execução (`PASS`/`WARN`/`FAIL`), taxa de aceitação, contagem de registros inválidos por categoria. Enviado via webhook ao `alertservice` para notificação.
- **`details`**: contagens por estágio do pipeline, métricas de transformação, resumo das expectativas GX, artefatos gerados (caminhos dos parquets, relatório de quarentena).

### Relatório em falha parcial
Se o pipeline falhar em qualquer estágio após o início do processamento (ex: falha ao salvar na trusted), o relatório de qualidade é gerado com os dados disponíveis até aquele ponto, identificando o estágio de falha. Isso permite diagnóstico preciso sem depender apenas de stack traces nos logs.

### Lineage de colunas
O módulo `lineage/lineage_functions.py` rastreia automaticamente a origem de cada coluna do DataFrame resultante (caminho JSON de origem, transformação aplicada, tipo resultante), adicionando rastreabilidade ao relatório de qualidade.

## Alternativas consideradas

**Fail-fast (falha toda a execução no primeiro erro):** Simples de implementar. Porém, um único registro com coordenada inválida derrubaria o processamento de dezenas de milhares de registros válidos. A disponibilidade percebida do pipeline seria artificialmente baixa.

**Best-effort silencioso (ignorar erros, salvar o que der):** Máxima disponibilidade, mas zero observabilidade. Dados corrompidos chegam silenciosamente à camada trusted e à visualização. Bugs de transformação levam horas ou dias para ser detectados, quando usuários reclamam de dados errados.

**Logging estruturado sem quarentena:** Registrar os erros em log sem isolar os registros inválidos. Melhor que silêncio, mas os dados ruins ainda chegam à camada trusted, e correlacionar logs com registros específicos exige cruzamento manual de informações.

**Framework externo de DQ (ex: dbt tests, Soda Core):** Ferramentas maduras para validação de dados em pipelines orientados a SQL. Porém, introduzem dependências externas pesadas, requerem configuração de conexões separadas, e não se integram naturalmente com o fluxo de transformação Python do Airflow operator. O framework interno tem integração nativa com o pipeline e o contrato de configuração do `pipeline_configurator`.

## Consequências

**Positivo:**
- Registros inválidos não contaminam a camada trusted, mas também não são descartados — preservados na quarentena para investigação e reprocessamento.
- O `alertservice` recebe notificação estruturada a cada execução, permitindo monitoramento proativo sem acesso direto aos logs do Airflow.
- Falhas parciais são diagnosticáveis com precisão: o relatório indica exatamente em qual estágio o pipeline falhou.
- A taxa de aceitação histórica (armazenada via `alertservice`) permite identificar degradação gradual na qualidade dos dados da API antes que se torne um problema crítico.
- Lineage de colunas documenta automaticamente a transformação aplicada a cada campo, facilitando auditoria e debugging.

**Negativo / Tradeoffs:**
- Complexidade de implementação e manutenção significativamente maior do que abordagens mais simples.
- O relatório de qualidade é gerado e salvo a cada execução — em alta frequência (a cada 2 minutos), acumula muitos artefatos no bucket `metadata` que precisam de política de retenção.
- O webhook para o `alertservice` introduz acoplamento de runtime entre o pipeline e o serviço de notificação: se o `alertservice` estiver indisponível, o envio do relatório falha (embora o pipeline em si continue).
- As ferramentas específicas de validação (JSON Schema, Great Expectations) e a decisão de externalizar suas regras para arquivos de configuração são tratadas no [ADR-0006](./0006-validacao-orientada-a-configuracao.md).
