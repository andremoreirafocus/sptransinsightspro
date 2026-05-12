# ADR-0011: Logging estruturado com contrato canônico e adapter Loki

**Data:** 2026-05-12  
**Status:** Proposto

## Contexto

Atualmente, os componentes da plataforma (microserviços e pipelines) utilizam o módulo `logging` do Python com mensagens textuais livres e handlers locais de arquivo. Esse desenho funciona para depuração local básica, mas cria limitações operacionais relevantes:

1. **Baixa padronização entre componentes**: cada serviço define formato e conteúdo de logs de forma própria, dificultando correlação.
2. **Consulta limitada**: logs textuais livres reduzem a capacidade de filtro por campos semânticos (`execution_id`, `correlation_id`, `event`, `status`).
3. **Acoplamento local**: dependência de arquivo local dificulta observabilidade em ambientes containerizados.
4. **Duplicação de setup**: configuração de logging é repetida em múltiplos pontos do monorepo.

Como o projeto seguirá evoluindo com execução containerizada e integração progressiva com serviços de nuvem, é necessário estabelecer uma base de logging operacional mais consistente, reutilizável e portável.

## Decisão

Adotar um modelo de logging estruturado, com contrato canônico único, implementado por biblioteca compartilhada na camada de infraestrutura e com integração inicial ao stack Grafana Loki.

### 1. Contrato canônico de logs

Todo log operacional deve seguir um contrato mínimo padronizado, serializado em JSON por linha.

Campos obrigatórios:
- `timestamp`
- `level`
- `service`
- `component`
- `event`
- `message`

Campos recomendados:
- `execution_id`
- `correlation_id`
- `status`
- `error_type`
- `error_message`
- `metadata` (objeto)

Convenções:
- timestamp em UTC
- níveis padronizados (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)
- nomenclatura estável para `event` (snake_case)

### 2. Biblioteca compartilhada em `infra`

Criar uma biblioteca de logging na camada de infraestrutura para:
- padronizar a emissão de logs estruturados
- evitar setup ad-hoc por componente
- garantir reuso entre microserviços e pipelines

A camada de serviços/pipelines não deve conhecer detalhes de transporte/armazenamento de logs.

### 3. Estratégia por adapters

A biblioteca deve isolar transporte de logs por adapters.

Adapters iniciais:
- `stdout` (padrão)
- integração com Loki no ambiente containerizado

Isso permite portabilidade: o contrato e API de logging permanecem estáveis, enquanto o backend de observabilidade pode variar por ambiente.

### 4. Stack de observabilidade containerizada inicial

Adotar Grafana + Loki + Promtail como solução centralizada de logs para o ambiente containerizado atual, com foco em simplicidade operacional e baixo custo de manutenção no contexto do projeto.

## Alternativas consideradas

**Manter logging textual em arquivo local**

Foi descartado por manter baixa padronização e baixa capacidade de correlação entre componentes.

**ELK/OpenSearch como solução inicial**

Foi descartado neste momento por maior complexidade operacional para o estágio atual do projeto.

**Acoplar logging diretamente a um backend específico**

Foi descartado porque reduziria portabilidade e dificultaria migração entre ambientes.

## Consequências

**Positivo:**
- Padronização de logs operacionais em todo o projeto.
- Melhor capacidade de busca, filtro e correlação entre execuções.
- Redução de duplicação de código de logging.
- Base arquitetural reutilizável para evoluções futuras de observabilidade.

**Negativo / Tradeoffs:**
- Esforço inicial de migração dos componentes existentes.
- Necessidade de disciplina de contrato para evitar drift de campos.
- Necessidade de governança de labels/campos para evitar cardinalidade excessiva.

