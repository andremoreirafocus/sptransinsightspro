# Fakes compartilhados

Este diretório contém objetos fake reutilizáveis entre os testes das pipelines. Eles seguem o padrão de **Injeção de Dependência (DI)** adotado no projeto: as funções de produção aceitam colaboradores como parâmetros opcionais, e os testes injetam fakes no lugar dos objetos reais, sem necessidade de monkeypatching.

## Fakes disponíveis

### `FakeDuckDBConnection` — `fake_duckdb_connection.py`

Simula uma conexão DuckDB. Use quando a função testada recebe um `duckdb_client` como parâmetro.

| Parâmetro | Tipo | Descrição |
|---|---|---|
| `df` | `pd.DataFrame` | DataFrame retornado por `.df()`. Padrão: `DataFrame` vazio |
| `raises` | `Exception` | Exceção lançada ao chamar `.execute()`. Padrão: `None` |

Métodos implementados: `execute(sql)`, `df()`, `close()`. O atributo `closed` é marcado como `True` após `close()`.

**Exemplo:**
```python
from fakes.fake_duckdb_connection import FakeDuckDBConnection

# Simula retorno de dados
fake = FakeDuckDBConnection(df=pd.DataFrame({"col": [1, 2]}))
result = my_service(config, duckdb_client=fake)

# Simula falha de conexão
fake = FakeDuckDBConnection(raises=RuntimeError("connection refused"))
with pytest.raises(RuntimeError):
    my_service(config, duckdb_client=fake)
```

---

### `FakeDbEngine` e `make_fake_engine_factory` — `fake_db_engine.py`

Simula um engine SQLAlchemy (PostgreSQL). Use quando a função testada recebe uma `engine_factory` como parâmetro.

`make_fake_engine_factory` é a forma recomendada: cria uma factory que retorna sempre o mesmo `FakeDbEngine` e expõe o engine via `.engine` para inspeção nos asserts.

| Parâmetro | Tipo | Descrição |
|---|---|---|
| `rowcount` | `int` | Valor de `rowcount` retornado pelo execute. Padrão: `0` |
| `raises` | `Exception` | Exceção lançada ao entrar no context manager `begin()`. Padrão: `None` |

O engine acumula todas as chamadas em `engine.executed_statements` como uma lista de `(str(stmt), params)`.

**Exemplo:**
```python
from fakes.fake_db_engine import make_fake_engine_factory

# Simula insert bem-sucedido
factory = make_fake_engine_factory(rowcount=1)
my_service(config, engine_factory=factory)
assert len(factory.engine.executed_statements) == 1

# Simula falha no banco
factory = make_fake_engine_factory(raises=Exception("DB unavailable"))
with pytest.raises(Exception, match="DB unavailable"):
    my_service(config, engine_factory=factory)
```

---

### `FakeObject` — `fake_object_storage.py`

Simula um objeto retornado pelo client de object storage (MinIO) em listagens. Possui apenas o atributo `object_name`.

**Exemplo:**
```python
from fakes.fake_object_storage import FakeObject

def fake_list_objects(bucket, prefix):
    return [FakeObject(object_name=f"{prefix}data.parquet")]

result = my_service(config, list_fn=fake_list_objects)
```

---

## Quando criar um novo fake

Crie um novo fake neste diretório quando o mesmo colaborador precisar ser substituído em **mais de uma pipeline**. Para colaboradores específicos de uma única pipeline, o fake pode ficar no próprio diretório `tests/` da pipeline.
