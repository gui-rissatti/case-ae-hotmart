# Exercício 2: ETL PySpark com Modelagem Histórica

## 📌 Contexto

Este exercício é **significativamente mais complexo** que o primeiro, pois trabalha com:

- **Modelo de Eventos (Event Sourcing)**: Todas as alterações são registradas
- **Chegada Assíncrona**: As 3 tabelas não são salvas simultaneamente
- **Rastreabilidade Temporal**: Necessidade de navegar entre períodos
- **Idempotência**: Reprocessar deve gerar o mesmo resultado
- **SCD Type 2**: Manter histórico completo de mudanças

---

## 🗂️ Modelo de Dados Fonte

### Tabela: `purchase` (Eventos)

Registra **todas as alterações** que ocorrem em uma compra.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `purchase_id` | BIGINT | ID da compra |
| `buyer_id` | BIGINT | ID do comprador |
| `purchase_relation_id` | BIGINT | FK para relacionar com itens |
| `order_date` | DATE | Data do pedido |
| `release_date` | DATE | Data de pagamento confirmado |
| `producer_id` | BIGINT | ID do produtor |
| `purchase_value` | DECIMAL(10,2) | Valor da compra |
| `transaction_date` | DATE | **Data em que o evento foi salvo no banco** |

**Características:**
- Mesma `purchase_id` pode aparecer múltiplas vezes (histórico de mudanças)
- `transaction_date` determina quando a mudança ocorreu
- Não é tabela corrente - é tabela de eventos (CDC)

### Tabela: `product_item` (Eventos)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `product_item_id` | BIGINT | ID do item |
| `purchase_relation_id` | BIGINT | FK para compra |
| `product_id` | BIGINT | ID do produto |
| `item_value` | DECIMAL(10,2) | Valor do item |
| `transaction_date` | DATE | Data do evento |

### Tabela: `purchase_extra_info` (Eventos)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `purchase_extra_info_id` | BIGINT | ID da informação extra |
| `purchase_relation_id` | BIGINT | FK para compra |
| `subsidiary` | VARCHAR(50) | NATIONAL ou INTERNATIONAL |
| `transaction_date` | DATE | Data do evento |

---

## ⚠️ Comportamento Assíncrono (CRÍTICO!)

### Exemplo Real do Teste:

```
Compra ID: 55
Order Date: 2023-01-20

┌─────────────────┬─────────────────────────────────────────────────┐
│ transaction_date│ Tabela que Recebeu Evento                      │
├─────────────────┼─────────────────────────────────────────────────┤
│ 2023-01-20      │ ✅ purchase (compra criada)                    │
│ 2023-01-20      │ ✅ product_item (item registrado)              │
│ 2023-01-20      │ ❌ purchase_extra_info (NÃO CHEGOU!)           │
├─────────────────┼─────────────────────────────────────────────────┤
│ 2023-01-23      │ ❌ purchase (sem mudanças)                     │
│ 2023-01-23      │ ❌ product_item (sem mudanças)                 │
│ 2023-01-23      │ ✅ purchase_extra_info (chegou 3 dias depois!) │
├─────────────────┼─────────────────────────────────────────────────┤
│ 2023-02-05      │ ✅ purchase (buyer_id alterado)                │
│ 2023-02-05      │ ❌ product_item (sem mudanças)                 │
│ 2023-02-05      │ ❌ purchase_extra_info (sem mudanças)          │
└─────────────────┴─────────────────────────────────────────────────┘
```

### Consequências:

1. **Full Outer Join obrigatório**: Evento pode chegar em qualquer tabela primeiro
2. **Forward Fill necessário**: Repetir valores anteriores quando não há atualização
3. **Complexidade na detecção de mudanças**: O que realmente mudou vs o que é repetição?

---

## 🎯 Requisitos do Exercício

| # | Requisito | Descrição | Status |
|---|-----------|-----------|--------|
| 1 | **Modelagem Histórica** | Tabela final deve manter rastreabilidade completa | ✅ |
| 2 | **Processamento D-1** | Processar apenas eventos de D-1 a cada execução | ✅ |
| 3 | **Particionamento** | Particionar por `transaction_date` | ✅ |
| 4 | **Idempotência** | Reprocessar gera sempre o mesmo resultado | ✅ |
| 5 | **Time Travel** | Permitir consultar GMV em qualquer ponto no tempo | ✅ |
| 6 | **Assincronismo** | Tratar chegada fora de ordem das 3 tabelas | ✅ |
| 7 | **Forward Fill** | Repetir dados quando tabela não atualiza | ✅ |
| 8 | **Dados Correntes** | Facilitar consulta do último estado | ✅ |
| 9 | **GMV Auditável** | Garantir que GMV não muda com reprocessamento | ✅ |

---

## 🏗️ Arquitetura da Solução

### Diagrama de Fluxo

```
┌───────────────────────────────────────────────────────────────┐
│                    CAMADA DE INGESTÃO                         │
│                                                               │
│  ┌─────────┐     ┌──────────────┐     ┌─────────────────┐   │
│  │purchase │     │product_item  │     │purchase_extra   │   │
│  │(events) │     │  (events)    │     │  _info (events) │   │
│  └────┬────┘     └──────┬───────┘     └────────┬────────┘   │
│       │                 │                       │            │
│       │  Filtrar por transaction_date = D-1     │            │
│       └─────────────────┴───────────────────────┘            │
└───────────────────────────┬───────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                    TRANSFORMAÇÃO (PySpark)                    │
│                                                               │
│  1️⃣ Leitura de Eventos D-1                                   │
│     - Ler purchase WHERE transaction_date = :process_date    │
│     - Ler product_item WHERE transaction_date = :process_date│
│     - Ler purchase_extra_info WHERE ...                      │
│                                                               │
│  2️⃣ Full Outer Join                                          │
│     - Join por purchase_relation_id                          │
│     - Manter registros mesmo se apenas 1 tabela atualizou    │
│                                                               │
│  3️⃣ Buscar Estado Anterior (D-2, D-3, ...)                   │
│     - Para cada purchase_id sem atualização em alguma tabela │
│     - Buscar último valor conhecido na tabela histórica      │
│     - Forward fill: repetir valor anterior                   │
│                                                               │
│  4️⃣ Detecção de Mudança Real                                 │
│     - Calcular hash MD5 do registro completo                 │
│     - Comparar com hash do registro anterior                 │
│     - Inserir nova linha apenas se houver mudança            │
│                                                               │
│  5️⃣ Aplicar SCD Type 2                                       │
│     - effective_date: transaction_date da mudança            │
│     - end_date: transaction_date da próxima mudança          │
│     - is_current: TRUE para versão mais recente              │
│                                                               │
│  6️⃣ Atualizar Flags                                          │
│     - Atualizar is_current das linhas antigas para FALSE     │
│     - Atualizar end_date das linhas antigas                  │
└───────────────────────────┬───────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────┐
│              CAMADA DE ARMAZENAMENTO                          │
│                                                               │
│           fact_purchase_history                               │
│  ┌─────────────────────────────────────────────┐             │
│  │ purchase_id | effective_date | end_date | ..│             │
│  ├─────────────────────────────────────────────┤             │
│  │     55      |  2023-01-20    |2023-01-23|.. │  <- v1      │
│  │     55      |  2023-01-23    |2023-02-05|.. │  <- v2      │
│  │     55      |  2023-02-05    |   NULL   |.. │  <- v3 (now)│
│  └─────────────────────────────────────────────┘             │
│                                                               │
│  PARTITIONED BY (transaction_date DATE)                       │
└───────────────────────────────────────────────────────────────┘
```

---

## 📊 Modelagem: `fact_purchase_history`

### DDL (Data Definition Language)

```sql
CREATE TABLE fact_purchase_history (
    -- 🔑 Grain: purchase_id + effective_date
    purchase_id BIGINT NOT NULL,
    effective_date DATE NOT NULL,
    end_date DATE,  -- NULL = registro corrente
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- 📦 Campos de purchase
    buyer_id BIGINT,
    purchase_relation_id BIGINT,
    order_date DATE,
    release_date DATE,
    producer_id BIGINT,
    purchase_value DECIMAL(10,2),
    
    -- 📦 Campos de product_item
    product_item_id BIGINT,
    product_id BIGINT,
    item_value DECIMAL(10,2),
    
    -- 📦 Campos de purchase_extra_info
    purchase_extra_info_id BIGINT,
    subsidiary VARCHAR(50),
    
    -- 🏷️ Metadados
    source_update VARCHAR(100),  -- Qual tabela originou esta versão
    record_hash VARCHAR(32),     -- MD5 para detecção de mudanças
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    
    -- 🔐 Constraints
    PRIMARY KEY (purchase_id, effective_date)
)
PARTITIONED BY (transaction_date DATE)
STORED AS PARQUET;

-- Índices para otimizar time travel queries
CREATE INDEX idx_current_records ON fact_purchase_history(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_time_range ON fact_purchase_history(effective_date, end_date);
```

### Características da Modelagem:

| Aspecto | Implementação | Justificativa |
|---------|---------------|---------------|
| **Grain** | `purchase_id` + `effective_date` | Permite múltiplas versões da mesma compra |
| **SCD Type** | Type 2 (versioning) | Mantém histórico completo, não sobrescreve |
| **Particionamento** | `transaction_date` | Facilita D-1 incremental e reprocessamento |
| **Flag Corrente** | `is_current = TRUE` | Queries rápidas do estado atual |
| **Time Travel** | `effective_date` + `end_date` | Posicionar em qualquer ponto no tempo |
| **Detecção de Mudança** | MD5 hash | Evita inserir linhas duplicadas |

---

## 🔄 Lógica de Forward Fill

### Cenário Real:

```python
# D-1: 2023-01-20
# Chegam eventos:
# - purchase(55): buyer_id=100, value=1000
# - product_item(55): product_id=200
# - purchase_extra_info(55): ❌ NÃO CHEGA

# O que fazer com subsidiary?
# Opção 1: Deixar NULL → ❌ Perde informação quando chegar
# Opção 2: Forward fill → ✅ Repetir último valor conhecido

# Resultado em 2023-01-20:
fact_purchase_history:
  purchase_id: 55
  effective_date: 2023-01-20
  buyer_id: 100
  purchase_value: 1000
  product_id: 200
  subsidiary: NULL  # Ainda não chegou


# D: 2023-01-23 (3 dias depois)
# Chega evento:
# - purchase_extra_info(55): subsidiary=NATIONAL

# Forward fill: Repetir campos que NÃO mudaram
fact_purchase_history:
  purchase_id: 55
  effective_date: 2023-01-23  # Nova versão!
  buyer_id: 100               # Repetido de 2023-01-20
  purchase_value: 1000        # Repetido de 2023-01-20
  product_id: 200             # Repetido de 2023-01-20
  subsidiary: NATIONAL        # NOVO!
```

### Implementação em PySpark:

```python
from pyspark.sql import Window
from pyspark.sql.functions import last, col

# Buscar último valor conhecido para forward fill
window_spec = Window.partitionBy("purchase_id").orderBy("effective_date")

df_with_forward_fill = df.withColumn(
    "buyer_id_filled",
    last("buyer_id", ignorenulls=True).over(window_spec)
).withColumn(
    "subsidiary_filled",
    last("subsidiary", ignorenulls=True).over(window_spec)
)
# ... para todos os campos
```

---

## ⚙️ Garantia de Idempotência

### Desafio:

**Como garantir que processar Janeiro/2023 10 vezes gera sempre o mesmo resultado?**

### Estratégia Implementada:

```python
def process_partition(spark, process_date):
    """
    Processa uma partição de forma idempotente.
    
    Estratégia:
    1. DELETE da partição existente
    2. Reconstruir do zero baseado apenas em eventos de process_date
    3. INSERT da nova partição
    
    Resultado: Sempre determinístico!
    """
    
    # 1. Remover partição antiga (se existir)
    spark.sql(f"""
        DELETE FROM fact_purchase_history
        WHERE transaction_date = '{process_date}'
    """)
    
    # 2. Processar eventos de process_date
    df_new_partition = build_partition_from_events(
        spark, 
        process_date,
        include_forward_fill=True
    )
    
    # 3. Inserir nova partição
    df_new_partition.write \
        .mode("append") \
        .partitionBy("transaction_date") \
        .saveAsTable("fact_purchase_history")
    
    # 4. Atualizar flags is_current e end_date de registros antigos
    update_scd_flags(spark, process_date)
```

### Testes de Idempotência:

```python
def test_idempotency():
    """Testa que reprocessar gera o mesmo resultado."""
    
    # Processar primeira vez
    result_1 = process_partition(spark, "2023-01-20")
    hash_1 = result_1.collect().hashCode()
    
    # Processar segunda vez (reprocessamento)
    result_2 = process_partition(spark, "2023-01-20")
    hash_2 = result_2.collect().hashCode()
    
    # Processar terceira vez
    result_3 = process_partition(spark, "2023-01-20")
    hash_3 = result_3.collect().hashCode()
    
    assert hash_1 == hash_2 == hash_3, "Idempotência violada!"
```

---

## 🕐 Time Travel (Navegação Temporal)

### Requisito:

> "Eu preciso conseguir navegar entre períodos diferentes, tanto me posicionando no passado, como trazendo para outros momentos."

### Implementação:

```sql
-- GMV de Janeiro/2023 no fechamento (31/01/2023)
-- Ou seja: "Como estava no último dia do mês?"
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(purchase_value) AS gmv_total
FROM fact_purchase_history
WHERE 
    order_date >= '2023-01-01' 
    AND order_date < '2023-02-01'
    AND release_date IS NOT NULL
    -- Time Travel: Me posiciono em 31/01/2023
    AND effective_date <= '2023-01-31'
    AND (end_date > '2023-01-31' OR is_current = TRUE)
GROUP BY DATE_TRUNC('month', order_date);

-- Resultado: 100.000,00 (fechamento de janeiro)


-- GMV de Janeiro/2023 visto de Fevereiro (28/02/2023)
-- Ou seja: "Como está agora, considerando alterações posteriores?"
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(purchase_value) AS gmv_total
FROM fact_purchase_history
WHERE 
    order_date >= '2023-01-01' 
    AND order_date < '2023-02-01'
    AND release_date IS NOT NULL
    -- Time Travel: Me posiciono em 28/02/2023
    AND effective_date <= '2023-02-28'
    AND (end_date > '2023-02-28' OR is_current = TRUE)
GROUP BY DATE_TRUNC('month', order_date);

-- Resultado: 98.500,00 (uma compra foi estornada em fevereiro)
```

### Diagrama de Time Travel:

```
Linha do Tempo da Compra 55:

┌────────────────────────────────────────────────────────────────┐
│                                                                │
│  2023-01-20          2023-01-23          2023-02-05           │
│      │                   │                   │                 │
│      ▼                   ▼                   ▼                 │
│   v1: 1000          v2: 1000          v3: 800                 │
│   (criada)          (+subsidiary)     (valor alterado)        │
│                                                                │
└────────────────────────────────────────────────────────────────┘

Query: GMV em 31/01/2023
  → Pega v2 (effective_date <= 31/01 E end_date > 31/01)
  → purchase_value = 1000

Query: GMV em 28/02/2023
  → Pega v3 (effective_date <= 28/02 E end_date > 28/02 OR is_current)
  → purchase_value = 800

Diferença: 1000 - 800 = 200 (alteração retroativa em fevereiro)
```

---

## 📁 Estrutura de Arquivos

```
exercise_2_pyspark_etl/
├── README.md (este arquivo)
├── src/
│   ├── etl_main.py                 # Pipeline principal
│   ├── transformations.py          # Lógica de transformação
│   ├── data_quality.py             # Validações
│   └── utils.py                    # Funções auxiliares
├── queries/
│   ├── gmv_daily_by_subsidiary.sql
│   ├── current_state.sql
│   └── time_travel_validation.sql
├── tests/
│   ├── test_transformations.py
│   ├── test_idempotency.py
│   └── test_time_travel.py
└── data/
    ├── input/
    │   ├── purchase_events.csv
    │   ├── product_item_events.csv
    │   └── purchase_extra_info_events.csv
    └── expected_output/
        └── fact_purchase_history_sample.csv
```

---

## 🚀 Execução

```bash
# Processar eventos de uma data específica (D-1)
python src/etl_main.py --mode process --date 2023-01-22

# Reprocessar uma partição (idempotente)
python src/etl_main.py --mode reprocess --date 2023-01-20

# Consultar GMV com time travel
python src/etl_main.py --mode query --query-type gmv --as-of-date 2023-01-31

# Executar testes
pytest tests/ -v --cov=src
```

---

## 🧪 Estratégia de Testes

### 1. Teste de Assincronismo

```python
def test_async_arrival():
    """Testa que eventos chegando em ordem diferente geram resultado correto."""
    
    # Cenário: purchase chega D+0, product_item D+0, extra_info D+3
    events_d0 = [
        {"purchase_id": 1, "buyer_id": 100, "transaction_date": "2023-01-20"},
        {"purchase_id": 1, "product_id": 200, "transaction_date": "2023-01-20"},
    ]
    
    result_d0 = process_partition(spark, "2023-01-20", events_d0)
    assert result_d0.filter("subsidiary IS NOT NULL").count() == 0
    
    events_d3 = [
        {"purchase_id": 1, "subsidiary": "NATIONAL", "transaction_date": "2023-01-23"}
    ]
    
    result_d3 = process_partition(spark, "2023-01-23", events_d3)
    
    # Forward fill deve ter repetido buyer_id e product_id
    row = result_d3.filter("purchase_id = 1 AND effective_date = '2023-01-23'").first()
    assert row.buyer_id == 100
    assert row.product_id == 200
    assert row.subsidiary == "NATIONAL"
```

### 2. Teste de Idempotência

```python
def test_reprocessing_generates_same_result():
    """Testa que reprocessar 10x gera sempre o mesmo resultado."""
    
    checksums = []
    for i in range(10):
        result = process_partition(spark, "2023-01-20")
        checksum = result.selectExpr("md5(concat_ws('|', *))").collect()[0][0]
        checksums.append(checksum)
    
    assert len(set(checksums)) == 1, "Reprocessamento gerou resultados diferentes!"
```

### 3. Teste de Time Travel

```python
def test_time_travel_accuracy():
    """Testa que time travel retorna valores corretos do passado."""
    
    # Criar histórico com 3 versões
    create_sample_data_with_changes()
    
    # Consultar GMV em 3 momentos diferentes
    gmv_jan_31 = query_gmv(as_of_date="2023-01-31")
    gmv_feb_28 = query_gmv(as_of_date="2023-02-28")
    gmv_mar_31 = query_gmv(as_of_date="2023-03-31")
    
    # Validar que valores são diferentes (refletindo alterações retroativas)
    assert gmv_jan_31 != gmv_feb_28  # Houve alteração em fevereiro
    assert gmv_feb_28 == gmv_mar_31  # Não houve alteração em março
```

---

## 💡 Decisões de Nível Sênior

### 1. **Por que SCD Type 2 e não Type 1?**

| Aspecto | Type 1 (Overwrite) | Type 2 (Versioning) | Decisão |
|---------|-------------------|---------------------|---------|
| Rastreabilidade | ❌ Perde histórico | ✅ Mantém tudo | Type 2 |
| Storage | ✅ Menor | ❌ Maior | Aceitável |
| Complexidade Query | ✅ Simples | ❌ Complexa | Aceitável |
| Auditoria | ❌ Impossível | ✅ Completa | **Requisito crítico** |

**Justificativa**: Requisitos de auditoria e time travel tornam Type 2 obrigatório.

### 2. **Por que DELETE + INSERT e não MERGE?**

| Abordagem | Prós | Contras |
|-----------|------|---------|
| MERGE | Atualiza apenas registros alterados | Complexidade alta, dificulta debugging |
| DELETE + INSERT | Sempre gera resultado determinístico | Reescreve partição inteira |

**Decisão**: DELETE + INSERT para garantir idempotência.

**Justificativa**: 
- Partições diárias são pequenas (~1 dia de dados)
- Idempotência é requisito crítico
- Debugging é mais fácil

### 3. **Por que PySpark e não SQL?**

**Requisito do teste**: *"Se você tiver conhecimento com programação, Python, Spark ou Scala, você possa fazer utilizando uma linguagem de programação."*

**Decisão**: PySpark

**Justificativa**:
- ✅ Escala para grandes volumes
- ✅ Permite lógica complexa (forward fill, hash MD5)
- ✅ Suporta testes unitários
- ✅ Integração com ecossistema de dados moderno

---

## 📚 Referências e Leituras Recomendadas

- [Slowly Changing Dimensions (Kimball)](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Idempotent Consumer](https://microservices.io/patterns/communication-style/idempotent-consumer.html)
- [PySpark Window Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)

---

**Próximo:** Implementação do código Python completo em [`src/etl_main.py`](./src/etl_main.py)
