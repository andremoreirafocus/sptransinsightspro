# ADR-0003: Modelagem de dados e arquitetura para detecção de duração de viagens

**Data:** 2026-05-11  
**Status:** Aceito

## Contexto

O objetivo central do fluxo de posições é calcular a duração das viagens realizadas pelos ônibus ao longo do dia. Para isso, não basta armazenar posições históricas: é necessário identificar, de forma confiável, quando cada viagem começa e quando termina.

A definição operacional adotada para esse problema depende do comportamento do ônibus em relação aos terminais da sua linha. Em termos práticos, a detecção de início e fim de viagem exige identificar os momentos em que o veículo se aproxima do primeiro ponto e do último ponto do trajeto correspondente.

Como a posição desses terminais varia conforme a linha e o sentido operacional da viagem, o pipeline precisava de uma estrutura intermediária que relacionasse cada viagem operacional às coordenadas do primeiro e do último ponto. Essa informação precisava estar disponível no momento da transformação das posições, para que o cálculo de distância pudesse ser feito uma única vez e propagado para as etapas seguintes.

## Decisão

Criar uma tabela de detalhes de viagens derivada do GTFS especificamente para suportar a detecção de início e fim de viagens.

Essa tabela deve concentrar, para cada viagem operacional, os atributos necessários para enriquecer as posições de ônibus com contexto terminal:

- identificador da viagem (`trip_id`)
- posição do primeiro ponto da viagem
- posição do último ponto da viagem

Isto viabilizará que o pipeline de transformação use os detalhes de viagem para enriquecer cada registro de posição já com:

- coordenadas do primeiro ponto
- coordenadas do último ponto
- distância entre a posição atual do ônibus e o primeiro ponto
- distância entre a posição atual do ônibus e o último ponto

Como resultado, a tabela de posições da camada trusted deverá ser intencionalmente produzida como um conjunto enriquecido e desnormalizado, pronto para consumo pela etapa de detecção de viagens concluídas.

## Justificativa

Concentrar os detalhes de viagens apenas nos atributos necessários para contexto terminal é benéfico porque o problema a resolver não é reconstruir toda a semântica do GTFS durante a detecção de viagens, mas permitir que o pipeline downstream identifique com eficiência quando uma viagem começou e quando terminou.

Essa modelagem traz os seguintes benefícios:

- evita repetir, na etapa de detecção de viagens, joins e reconstruções de contexto GTFS que já podem ser resolvidos antes
- antecipa o custo do enriquecimento para a etapa de transformação e posições, onde ele é calculado uma única vez por posição
- permite que o pipeline de identificação de viagens concluídas se concentre apenas em analisar a variação temporal das distâncias aos terminais para detectar início, fim e intervalos entre viagens
- simplifica o raciocínio downstream: a etapa refined passa a operar sobre posições já contextualizadas, em vez de precisar combinar telemetria, GTFS e regras de detecção simultaneamente
- reduz o acoplamento da lógica de detecção de viagens à estrutura bruta das tabelas GTFS

Em outras palavras, a decisão não foi modelar o GTFS de forma genérica para depois decidir como usá-lo, mas preparar explicitamente os atributos que tornam a detecção de viagens viável e eficiente no restante da arquitetura.

## Consequências

**Positivo:**
- o pipeline de transformação de posições deverá produzir dados prontos para análise de evolução da viagem, sem exigir enriquecimento adicional no refined.
- o pipeline de identificação de viagens concluídas poderá se concentrar na detecção de início, fim e tempo entre viagens, sem repetir lógica de integração com GTFS.
- O custo computacional e cognitivo da detecção de viagens fica distribuído de forma mais clara entre as camadas: contexto terminal no trusted, cálculo de fatos de viagem no refined.

**Negativo / Tradeoffs:**
- A camada trusted passa a carregar um enriquecimento específico para o problema de detecção de viagens, e não apenas uma transformação estrutural neutra.
- A tabela de detalhes de viagem será propositalmente orientada ao problema de negócio e, portanto, não substitui outras necessidades analíticas que possam existir sobre os dados GTFS.
- Alterações futuras na forma de detectar início e fim de viagem podem exigir revisão dos atributos mantidos em detalhes de viagens.

## Alternativas consideradas

**Calcular distâncias aos terminais apenas no pipeline de identificação de viagens concluídas:** manteria a transformação mais simples, mas empurraria para a etapa de detecção tanto o custo computacional quanto a complexidade de reconstruir o contexto GTFS para cada posição.

**Usar diretamente tabelas GTFS mais amplas durante a detecção de viagens:** preservaria maior generalidade, mas aumentaria a complexidade da lógica downstream e misturaria enriquecimento estrutural com cálculo de fatos de viagem.

**Modelar a camada trusted sem desnormalização terminal:** reduziria a redundância por registro, mas tornaria a etapa refined mais cara e menos focada, ao exigir joins e cálculos adicionais exatamente no ponto em que o objetivo deveria ser apenas detectar viagens concluídas.
