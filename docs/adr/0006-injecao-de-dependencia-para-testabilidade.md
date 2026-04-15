# ADR-0006: Injeção de dependência como estratégia de testabilidade

**Data:** 2026-04-15  
**Status:** Aceito

## Contexto

Os serviços das pipelines dependem de colaboradores externos: conexões DuckDB, engines PostgreSQL, clientes MinIO, funções de leitura e escrita em disco. Testar esses serviços com dependências reais exigiria infraestrutura rodando (MinIO, PostgreSQL, DuckDB com S3 configurado), tornando os testes lentos, frágeis e impossíveis de executar em ambientes sem infraestrutura disponível.

A abordagem convencional em Python para desacoplar código de suas dependências nos testes é o uso de `unittest.mock.patch` ou `monkeypatch` do pytest: sobrescrever temporariamente atributos de módulos ou classes com objetos mock durante a execução do teste.

Essa abordagem tem limitações práticas:
- Patches são acoplados ao caminho de importação (`mock.patch("pipeline.services.service.client.method")`), quebrando silenciosamente com refatorações.
- Mocks que especificam `return_value` não testam se a função chamou os métodos corretos com os argumentos corretos — apenas se retornam o valor certo.
- Patches espalhados por testes dificultam a rastreabilidade: é difícil saber, ao ler um teste, quais dependências foram substituídas.

## Decisão

Adotar **injeção de dependência via parâmetros opcionais** em todos os serviços testáveis: colaboradores externos são aceitos como parâmetros com valores padrão que apontam para a implementação de produção.

```python
# Produção: usa o cliente real
def load_trip_details(config, duckdb_client=None):
    con = duckdb_client or get_duckdb_connection(config["connections"])
    ...

# Teste: injeta um fake
def test_load_trip_details_returns_dataframe():
    fake = FakeDuckDBConnection(df=expected_df)
    result = load_trip_details(config, duckdb_client=fake)
    assert result.equals(expected_df)
```

Fakes reutilizáveis entre pipelines ficam em `dags-dev/tests/fakes/`:
- `FakeDuckDBConnection` — simula conexão DuckDB com retorno configurável e suporte a simulação de exceções.
- `FakeDbEngine` + `make_fake_engine_factory` — simula engine SQLAlchemy com inspeção das statements executadas.
- `FakeObject` — simula objeto retornado por listagens do MinIO.

O `conftest.py` de cada pipeline adiciona `dags-dev/tests/` ao `sys.path`, tornando os fakes acessíveis como `from fakes.fake_duckdb_connection import FakeDuckDBConnection`.

## Alternativas consideradas

**`unittest.mock.patch` / `monkeypatch`:** Amplamente usado, sem necessidade de alterar o código de produção. Porém, acopla os testes ao caminho de importação, dificulta a leitura (o contexto do que foi substituído fica separado do assert) e não fornece fakes stateful (ex: um fake que acumula statements executadas para verificação posterior).

**Testes de integração com infraestrutura real (ex: pytest-docker):** Garante que o código funciona com o sistema real. Porém, aumenta dramaticamente o tempo de execução, requer Docker disponível no ambiente de CI, e torna cada teste dependente do estado da infraestrutura (dados residuais, timeouts de rede).

**Subclasses e interfaces formais (ABC):** Definir protocolos ou classes abstratas para cada colaborador. Seria mais formal e type-safe, mas adiciona overhead de manutenção para um projeto Python onde duck typing é suficiente e os contratos são simples.

## Consequências

**Positivo:**
- Testes rápidos e determinísticos: nenhuma chamada de rede, banco de dados ou disco real.
- Fakes expressos como classes simples em Python puro, legíveis e mantíveis sem conhecimento de bibliotecas de mock.
- Fakes stateful: `FakeDbEngine.executed_statements` permite verificar exatamente quais queries foram executadas, sem magic assertions sobre calls de mock.
- Refatorações de caminho de importação não quebram testes (a injeção é estrutural, não baseada em string de caminho).
- Fakes compartilhados em `tests/fakes/` garantem consistência do comportamento simulado entre todas as pipelines.

**Negativo / Tradeoffs:**
- As assinaturas das funções ficam ligeiramente mais longas quando vários colaboradores são injetados (`load_trip_details(config, duckdb_client=None, write_fn=None)`).
- Requer disciplina de manutenção: quando um colaborador muda sua interface real, o fake correspondente precisa ser atualizado manualmente — não há checagem automática de aderência.
- O padrão só cobre a camada de serviços. O DAG script em si (`transformlivedata-v8.py`) permanece sem cobertura, pois depende do Airflow context — esse é um gap conhecido e aceito.
