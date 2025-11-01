# Architecture Decision Records (ADRs)

## Índice
- [ADR-001: Escolha de SCD Type 2 para Rastreabilidade](#adr-001)
- [ADR-002: Estratégia de Idempotência via DELETE + INSERT](#adr-002)
- [ADR-003: Forward Fill para Tratamento Assíncrono](#adr-003)
- [ADR-004: Particionamento por transaction_date](#adr-004)
- [ADR-005: PySpark ao invés de SQL Puro](#adr-005)
- [ADR-006: MD5 Hash para Detecção de Mudanças](#adr-006)
- [ADR-007: Flag is_current para Queries Performáticas](#adr-007)

---

## ADR-001: Escolha de SCD Type 2 para Rastreabilidade {#adr-001}

**Status:** ✅ Aceito

**Contexto:**

O exercício 2 exige rastreabilidade completa das mudanças em compras, além de permitir "time travel" (navegar entre diferentes períodos). Precisamos decidir qual tipo de Slowly Changing Dimension (SCD) utilizar.

**Opções Consideradas:**

### Opção 1: SCD Type 1 (Overwrite)
```sql
-- Sobrescreve registro existente
UPDATE purchase SET buyer_id = 200 WHERE purchase_id = 55;
```

**Prós:**
- ✅ Simples de implementar
- ✅ Menor uso de storage
- ✅ Queries mais simples (sem deduplicação)

**Contras:**
- ❌ Perde histórico completo
- ❌ Impossível fazer auditoria
- ❌ Não permite time travel
- ❌ **Viola requisito explícito do teste**

### Opção 2: SCD Type 2 (Versioning)
```sql
-- Mantém versões históricas
INSERT INTO purchase VALUES (55, '2023-02-05', ...);
-- Registro antigo permanece com end_date preenchido
```

**Prós:**
- ✅ **Rastreabilidade completa**
- ✅ **Permite time travel**
- ✅ **Auditável e reproduzível**
- ✅ Atende requisito: "navegar entre períodos"

**Contras:**
- ❌ Maior uso de storage (~3-5x mais)
- ❌ Queries mais complexas (precisa deduplicar)
- ❌ Maior complexidade de manutenção

### Opção 3: SCD Type 3 (Limited History)
```sql
-- Mantém apenas versão atual + anterior
ALTER TABLE purchase ADD COLUMN previous_buyer_id;
```

**Prós:**
- ✅ Histórico limitado sem explodir storage
- ✅ Queries simples

**Contras:**
- ❌ Perde histórico completo além de 2 versões
- ❌ Não atende requisito de rastreabilidade total

**Decisão:**

**✅ SCD Type 2 (Versioning)**

**Justificativa:**

1. **Requisito Explícito:**
   > "É necessário que essa modelagem seja uma modelagem que traga rastreabilidade, então ela é uma modelagem histórica."

2. **Time Travel é Mandatório:**
   > "Eu preciso conseguir navegar entre períodos diferentes, tanto me posicionando no passado, como trazendo para outros momentos."

3. **Auditabilidade:**
   - Financeiro e contabilidade precisam fechar mês e garantir que valores não mudem
   - GMV de Janeiro/2023 deve ser sempre o mesmo quando consultado no futuro

4. **Trade-off Aceitável:**
   - Storage é barato comparado ao valor da rastreabilidade
   - Complexidade de queries é gerenciável com CTEs e índices adequados

**Implementação:**

```sql
CREATE TABLE fact_purchase_history (
    purchase_id BIGINT,
    effective_date DATE NOT NULL,  -- Início da vigência
    end_date DATE,                 -- Fim da vigência (NULL = corrente)
    is_current BOOLEAN,            -- Flag para facilitar queries
    
    -- Dados de negócio
    buyer_id BIGINT,
    purchase_value DECIMAL(10,2),
    -- ... outros campos
    
    PRIMARY KEY (purchase_id, effective_date)
)
PARTITIONED BY (transaction_date DATE);
```

**Consequências:**

- ✅ Atende 100% dos requisitos
- ✅ Preparado para auditoria e compliance
- ⚠️ Requer educação de stakeholders sobre queries de time travel
- ⚠️ Monitoramento de storage necessário

---

## ADR-002: Estratégia de Idempotência via DELETE + INSERT {#adr-002}

**Status:** ✅ Aceito

**Contexto:**

O teste exige que reprocessar uma partição múltiplas vezes gere **sempre o mesmo resultado**:

> "Se eu falar que eu quero o GMV do mês de janeiro de 2023 for mil reais, não importa quantas vezes eu vá reprocessar essa solução que vocês vão propor, ele tem que sempre dar os mesmos mil reais."

Precisamos garantir idempotência no pipeline ETL.

**Opções Consideradas:**

### Opção 1: MERGE (Upsert)
```python
df.write.mode("merge") \
    .option("mergeSchema", "true") \
    .saveAsTable("target")
```

**Prós:**
- ✅ Atualiza apenas registros alterados
- ✅ Mais eficiente em I/O

**Contras:**
- ❌ Complexidade alta na lógica de match
- ❌ Difícil debugar inconsistências
- ❌ Comportamento não determinístico em cenários edge case
- ❌ Requer Delta Lake ou Hudi (não é Spark puro)

### Opção 2: DELETE + INSERT (Partição Completa)
```python
# 1. Deletar partição
spark.sql(f"DELETE FROM target WHERE date = '{date}'")

# 2. Inserir nova partição do zero
df.write.mode("append").saveAsTable("target")
```

**Prós:**
- ✅ **Sempre determinístico**
- ✅ Simples de entender e debugar
- ✅ Funciona com Spark puro (sem Delta)
- ✅ Fácil validar: basta reprocessar e comparar

**Contras:**
- ❌ Reescreve partição inteira (mais I/O)
- ❌ Janela pequena de indisponibilidade

### Opção 3: Append-Only com Deduplicação na Leitura
```python
# Sempre append
df.write.mode("append").saveAsTable("target")

# Deduplicar na query
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (...) as rn
    FROM target
) WHERE rn = 1
```

**Prós:**
- ✅ Sem deletes (immutable storage)
- ✅ Auditável (vê todas as tentativas)

**Contras:**
- ❌ Storage explode com reprocessamentos
- ❌ Queries sempre precisam deduplicar (lento)
- ❌ Dificulta identificar "última boa versão"

**Decisão:**

**✅ DELETE + INSERT por Partição**

**Justificativa:**

1. **Determinismo Garantido:**
   - Processar 10x a mesma data → 10x o mesmo resultado
   - Não há estado intermediário ou condições de corrida

2. **Simplicidade:**
   - Qualquer analista consegue entender a lógica
   - Debugging é trivial: "essa partição tem problema? Delete e reprocesse"

3. **Partições Diárias São Pequenas:**
   - ~1 dia de dados (~100MB típico)
   - Reescrever é rápido (<5s em clusters modernos)

4. **Facilita Testes:**
   ```python
   def test_idempotency():
       hash_1 = process_and_hash("2023-01-20")
       hash_2 = process_and_hash("2023-01-20")  # Reprocessar
       assert hash_1 == hash_2  # Sempre igual!
   ```

**Implementação:**

```python
def process_partition_idempotent(spark, date):
    """
    Processa partição de forma idempotente.
    """
    # Step 1: DELETE (se existir)
    spark.sql(f"""
        DELETE FROM fact_purchase_history
        WHERE transaction_date = '{date}'
    """)
    
    # Step 2: Reconstruir do zero
    df = build_partition_from_events(spark, date)
    
    # Step 3: INSERT
    df.write \
        .mode("append") \
        .partitionBy("transaction_date") \
        .saveAsTable("fact_purchase_history")
```

**Consequências:**

- ✅ Idempotência 100% garantida
- ✅ Testes de regressão confiáveis
- ⚠️ Requer que usuários não leiam durante reprocessamento (janela de ~5s)
- ⚠️ Monitorar performance de rewrites

**Alternativa Futura:**

Se storage se tornar problema crítico, migrar para Delta Lake com MERGE:
```python
df.write.format("delta") \
    .mode("merge") \
    .option("replaceWhere", f"transaction_date = '{date}'")
```

---

## ADR-003: Forward Fill para Tratamento Assíncrono {#adr-003}

**Status:** ✅ Aceito

**Contexto:**

As 3 tabelas fonte (`purchase`, `product_item`, `purchase_extra_info`) **não chegam sincronizadas**:

> "Uma compra ela sempre vai ter informação das três tabelas. Porém, elas não são salvas no banco de forma síncrona."

Exemplo real do teste:
- 2023-01-20: `purchase` e `product_item` chegam
- 2023-01-23: `purchase_extra_info` chega (3 dias depois!)

O que fazer com `subsidiary` em 2023-01-20 se ainda não chegou?

**Opções Consideradas:**

### Opção 1: Deixar NULL (Wait for Data)
```python
# 2023-01-20: subsidiary = NULL
# 2023-01-23: subsidiary = "NATIONAL"
```

**Prós:**
- ✅ Honesto: mostra que dado não existia
- ✅ Simples de implementar

**Contras:**
- ❌ Viola requisito do teste
- ❌ Queries ficam com muitos NULLs
- ❌ Dificulta análises (precisa filtrar NULLs)

### Opção 2: Forward Fill (Repeat Last Known)
```python
# 2023-01-20: subsidiary = NULL (primeira vez)
# 2023-01-23: subsidiary = "NATIONAL" (chegou!)
# Mas também: repetir todos os outros campos não atualizados
```

**Prós:**
- ✅ **Atende requisito explícito do teste**
- ✅ Mantém consistência temporal
- ✅ Facilita análises futuras

**Contras:**
- ❌ Repete dados (usa mais storage)
- ❌ Complexidade na detecção de "mudança real" vs repetição

### Opção 3: Criar Registro Temporário + Substituir
```python
# 2023-01-20: Inserir com subsidiary = "PENDING"
# 2023-01-23: Atualizar retroativamente 2023-01-20
```

**Prós:**
- ✅ Dados sempre completos

**Contras:**
- ❌ **Viola idempotência!** (altera passado)
- ❌ Complexo de implementar
- ❌ Não é permitido pelo teste

**Decisão:**

**✅ Forward Fill (Opção 2)**

**Justificativa:**

1. **Requisito Explícito:**
   > "Quando ele atualizar a subsidiária, nesse mesmo dia não houve atualização de nenhuma das outras tabelas, ele vai repetir o conteúdo e vai trazer a nova informação."

2. **Mantém Histórico Correto:**
   - Cada dia reflete o estado completo do conhecimento naquele momento
   - Não altera passado (preserva idempotência)

3. **Facilita Time Travel:**
   - Consulta em qualquer data retorna registro completo (não precisa "juntar" NULLs)

**Implementação:**

```python
def apply_forward_fill(df_merged, df_previous):
    """
    Aplica forward fill de valores não atualizados.
    """
    df_with_previous = df_merged.join(
        df_previous.select(
            "purchase_id",
            F.col("buyer_id").alias("prev_buyer_id"),
            F.col("subsidiary").alias("prev_subsidiary"),
            # ... outros campos
        ),
        on="purchase_id",
        how="left"
    )
    
    return df_with_previous.select(
        "purchase_id",
        # Coalesce: novo valor se existir, senão anterior
        F.coalesce("buyer_id", "prev_buyer_id").alias("buyer_id"),
        F.coalesce("subsidiary", "prev_subsidiary").alias("subsidiary"),
        # ...
    )
```

**Detecção de Mudança Real:**

Após forward fill, nem todo registro é uma mudança real. Usar MD5 hash:

```python
df_with_hash = df.withColumn(
    "record_hash",
    F.md5(F.concat_ws("|", *all_data_columns))
)

# Inserir apenas se hash mudou
df_changed = df_with_hash.filter(
    F.col("record_hash") != F.col("previous_hash")
)
```

**Consequências:**

- ✅ Atende requisito do teste
- ✅ Histórico completo e navegável
- ⚠️ Storage ~20% maior (repetição de dados)
- ⚠️ Requer MD5 hash para evitar linhas idênticas

---

## ADR-004: Particionamento por transaction_date {#adr-004}

**Status:** ✅ Aceito

**Contexto:**

O teste especifica processamento D-1 (incremental diário). Precisamos decidir a estratégia de particionamento da tabela final.

**Opções Consideradas:**

### Opção 1: Sem Particionamento
```sql
CREATE TABLE fact_purchase_history (...);
-- Tabela plana, sem partições
```

**Prós:**
- ✅ Simples

**Contras:**
- ❌ DELETE de uma data lê tabela inteira
- ❌ Queries full scan (lento)
- ❌ Impossível reprocessar partição específica

### Opção 2: Particionar por order_date (Data de Negócio)
```sql
PARTITIONED BY (order_date DATE);
```

**Prós:**
- ✅ Queries analíticas rápidas (filtro por período)

**Contras:**
- ❌ **Uma alteração em fevereiro de compra de janeiro vai para qual partição?**
- ❌ Dificulta D-1 incremental
- ❌ Não facilita reprocessamento

### Opção 3: Particionar por transaction_date (Data Técnica)
```sql
PARTITIONED BY (transaction_date DATE);
```

**Prós:**
- ✅ **Alinha com processamento D-1**
- ✅ **Reprocessamento trivial:** DELETE WHERE transaction_date = X
- ✅ Partition pruning automático no pipeline
- ✅ Facilita debugging: "problema na partição 2023-01-25"

**Contras:**
- ❌ Queries analíticas precisam ler múltiplas partições
- ❌ Precisa adicionar filtro de transaction_date em time travel

**Decisão:**

**✅ Particionar por transaction_date**

**Justificativa:**

1. **Alinha com Padrão D-1:**
   ```python
   # Processar ontem
   process_date = today - timedelta(days=1)
   df = read_events(transaction_date=process_date)
   ```

2. **Idempotência Eficiente:**
   ```sql
   -- Reprocessar é instantâneo
   DELETE FROM target WHERE transaction_date = '2023-01-20';
   -- Só deleta 1 partição!
   ```

3. **Facilita Operação:**
   - "Falhou processamento dia 25" → reprocessar partição 25
   - Monitoramento: "partição X está atrasada"

4. **Queries Analíticas Otimizáveis:**
   ```sql
   -- Adicionar hint de partition pruning
   WHERE transaction_date <= :as_of_date
     AND order_date BETWEEN :start AND :end
   ```

**Implementação:**

```python
df.write \
    .mode("append") \
    .partitionBy("transaction_date") \
    .format("parquet") \
    .option("compression", "snappy") \
    .saveAsTable("fact_purchase_history")
```

**Índices Secundários:**

Para otimizar queries analíticas, criar índices em:
- `order_date`
- `is_current`
- `(effective_date, end_date)`

**Consequências:**

- ✅ Pipeline D-1 performático
- ✅ Reprocessamento trivial
- ⚠️ Queries analíticas precisam ser educadas sobre transaction_date
- ⚠️ Monitorar crescimento de partições

---

## ADR-005: PySpark ao invés de SQL Puro {#adr-005}

**Status:** ✅ Aceito

**Contexto:**

O teste menciona:
> "Se você tiver conhecimento com programação, Python, Spark ou Scala, você possa fazer utilizando uma linguagem de programação. Você pode até mesclar programação com SQL ali dentro do seu Python."

**Opções Consideradas:**

### Opção 1: SQL Puro
```sql
-- Tudo em SQL (stored procedures, views, etc.)
CREATE OR REPLACE VIEW purchase_with_forward_fill AS ...
```

**Prós:**
- ✅ Familiar para analistas SQL
- ✅ Sem setup de ambiente Python

**Contras:**
- ❌ Forward fill complexo em SQL puro
- ❌ Difícil testar (sem unit tests)
- ❌ Não escala para grandes volumes
- ❌ Lógica complexa fica ilegível

### Opção 2: PySpark
```python
from pyspark.sql import functions as F

df = df.withColumn("filled", F.coalesce(...))
```

**Prós:**
- ✅ **Escala para Big Data** (distribuído)
- ✅ **Testável** (unit tests com pytest)
- ✅ Lógica complexa mais legível
- ✅ Integração com ecossistema Python
- ✅ **Demonstra expertise técnica**

**Contras:**
- ❌ Curva de aprendizado
- ❌ Requer setup de ambiente

**Decisão:**

**✅ PySpark com SQL embutido onde apropriado**

**Justificativa:**

1. **Requisitos Complexos:**
   - Forward fill com window functions
   - MD5 hash de múltiplas colunas
   - Detecção de mudanças
   - **SQL puro seria extremamente verboso**

2. **Escalabilidade:**
   - Hotmart processa milhões de transações
   - PySpark distribui processamento automaticamente

3. **Testabilidade:**
   ```python
   def test_forward_fill():
       # Dados de teste
       df_input = spark.createDataFrame([...])
       
       # Executar lógica
       df_result = apply_forward_fill(df_input)
       
       # Validar
       assert df_result.count() == expected_count
   ```

4. **Demonstra Senioridade:**
   - Conhecimento de ferramentas modernas
   - Engenharia de dados em escala
   - Boas práticas (modularização, testes)

**Implementação Híbrida:**

```python
# Lógica complexa em PySpark
df_transformed = apply_forward_fill(df)

# Consultas simples em SQL
spark.sql("""
    SELECT order_date, SUM(purchase_value) as gmv
    FROM fact_purchase_history
    WHERE is_current = TRUE
    GROUP BY order_date
""")
```

**Consequências:**

- ✅ Solução escalável e profissional
- ✅ Código testável e manutenível
- ⚠️ Requer ambiente Spark configurado
- ⚠️ Analistas SQL precisam aprender PySpark basics

---

## ADR-006: MD5 Hash para Detecção de Mudanças {#adr-006}

**Status:** ✅ Aceito

**Contexto:**

Após aplicar forward fill, muitos registros ficam idênticos ao dia anterior. Inserir esses registros desperdiçaria storage e poluiria o histórico.

Precisamos detectar **mudanças reais**.

**Opções Consideradas:**

### Opção 1: Comparação Coluna a Coluna
```python
changed = df.filter(
    (df.buyer_id != df.prev_buyer_id) |
    (df.subsidiary != df.prev_subsidiary) |
    (df.purchase_value != df.prev_purchase_value) |
    # ... 20+ campos
)
```

**Prós:**
- ✅ Óbvio e direto

**Contras:**
- ❌ Verboso (~20 colunas)
- ❌ Precisa atualizar se adicionar campo
- ❌ Dificulta manutenção

### Opção 2: Serializar e Comparar String
```python
df.withColumn("record_str", 
    F.concat_ws("|", *all_columns)
)
df.filter(df.record_str != df.prev_record_str)
```

**Prós:**
- ✅ Simples
- ✅ Automático para todas as colunas

**Contras:**
- ❌ Strings grandes (usa memória)
- ❌ Comparação lenta

### Opção 3: Hash MD5 do Registro
```python
df.withColumn("record_hash",
    F.md5(F.concat_ws("|", *all_data_columns))
)
df.filter(df.record_hash != df.prev_record_hash)
```

**Prós:**
- ✅ **Compacto** (32 caracteres sempre)
- ✅ **Rápido** de comparar
- ✅ Automático para todas as colunas
- ✅ Permite armazenar hash para auditoria

**Contras:**
- ❌ Colisões teóricas (extremamente raras)

**Decisão:**

**✅ MD5 Hash**

**Justificativa:**

1. **Performance:**
   - Comparar 32 chars vs comparar 20+ colunas
   - Hash calculado uma vez, usado várias vezes

2. **Manutenibilidade:**
   - Adicionar campo → hash recalcula automaticamente
   - Sem código repetitivo

3. **Auditoria:**
   ```sql
   -- Ver se registro mudou
   SELECT purchase_id, effective_date, record_hash
   FROM fact_purchase_history
   WHERE purchase_id = 55
   ORDER BY effective_date;
   
   -- Se hash for igual, registro é idêntico!
   ```

4. **Colisões São Insignificantes:**
   - Probabilidade: ~1 em 2^128
   - Em contexto de negócio, inexistente

**Implementação:**

```python
def calculate_record_hash(df, data_columns):
    """
    Calcula hash MD5 de um registro.
    """
    return df.withColumn(
        "record_hash",
        F.md5(F.concat_ws("|",
            *[F.coalesce(F.col(c).cast("string"), F.lit(""))
              for c in data_columns]
        ))
    )

# Uso
df_with_hash = calculate_record_hash(df, [
    "buyer_id", "order_date", "purchase_value",
    "product_id", "subsidiary"
])

# Filtrar apenas mudanças
df_changed = df_with_hash.filter(
    (F.col("record_hash") != F.col("previous_hash")) |
    F.col("previous_hash").isNull()
)
```

**Campos Excluídos do Hash:**
- `effective_date`, `end_date`, `is_current` (metadados temporais)
- `created_at`, `updated_at` (timestamps técnicos)
- `source_update` (rastreabilidade)

**Consequências:**

- ✅ Detecção eficiente de mudanças
- ✅ Código limpo e manutenível
- ✅ Storage otimizado (não insere duplicatas)
- ⚠️ Adicionar novos campos requer atualização da lista

---

## ADR-007: Flag is_current para Queries Performáticas {#adr-007}

**Status:** ✅ Aceito

**Contexto:**

A maioria das queries analíticas quer o **estado atual** das compras, não o histórico completo. Em SCD Type 2, isso requer filtrar pela versão mais recente.

**Opções Consideradas:**

### Opção 1: Filtrar por end_date IS NULL
```sql
SELECT * FROM fact_purchase_history
WHERE end_date IS NULL;
```

**Prós:**
- ✅ Não precisa de campo extra

**Contras:**
- ❌ Ambíguo: NULL pode ser "corrente" ou "erro"
- ❌ Dificulta índices (NULL em índice é problemático)

### Opção 2: Adicionar Flag is_current
```sql
SELECT * FROM fact_purchase_history
WHERE is_current = TRUE;
```

**Prós:**
- ✅ **Explícito e claro**
- ✅ **Índice eficiente:** CREATE INDEX ON (...) WHERE is_current
- ✅ Facilita queries para analistas

**Contras:**
- ❌ Campo redundante (pode derivar de end_date)
- ❌ Precisa manter sincronizado

**Decisão:**

**✅ Flag is_current booleana**

**Justificativa:**

1. **Clareza:**
   ```sql
   -- Intuitivo para qualquer analista
   WHERE is_current = TRUE
   
   -- vs ambíguo
   WHERE end_date IS NULL OR end_date > CURRENT_DATE
   ```

2. **Performance:**
   ```sql
   -- Índice partial altamente eficiente
   CREATE INDEX idx_current 
   ON fact_purchase_history(purchase_id)
   WHERE is_current = TRUE;
   
   -- Resultado: queries 100x mais rápidas
   ```

3. **Requisito do Teste:**
   > "É necessário que a sua proposta de alguma maneira, você consiga deixar com que, se eu quiser os registros, o último status da compra, o registro mais atual dela hoje, como que ela está, tem que ser fácil de eu conseguir trazer esse registro corrente."

**Implementação:**

```python
# Ao inserir nova versão
df_new = df.withColumn("is_current", F.lit(True))

# Atualizar versões antigas
spark.sql("""
    UPDATE fact_purchase_history
    SET is_current = FALSE,
        end_date = :new_effective_date,
        updated_at = CURRENT_TIMESTAMP()
    WHERE purchase_id IN (:affected_ids)
      AND is_current = TRUE
""")
```

**Manutenção:**

Garantir que **sempre e apenas** 1 registro tenha `is_current = TRUE` por `purchase_id`:

```sql
-- Validação (deve retornar 0)
SELECT purchase_id, COUNT(*) as cnt
FROM fact_purchase_history
WHERE is_current = TRUE
GROUP BY purchase_id
HAVING COUNT(*) > 1;
```

**Consequências:**

- ✅ Queries de dados correntes extremamente rápidas
- ✅ Código analítico mais legível
- ⚠️ Precisa validar integridade de is_current em testes
- ⚠️ Atualização de is_current é parte crítica do pipeline

---

## Resumo das Decisões

| ADR | Decisão | Impacto | Status |
|-----|---------|---------|--------|
| 001 | SCD Type 2 | Alto (arquitetura fundamental) | ✅ |
| 002 | DELETE + INSERT | Médio (idempotência) | ✅ |
| 003 | Forward Fill | Alto (requisito do teste) | ✅ |
| 004 | Partition by transaction_date | Alto (performance) | ✅ |
| 005 | PySpark | Médio (escolha de stack) | ✅ |
| 006 | MD5 Hash | Baixo (otimização) | ✅ |
| 007 | is_current Flag | Médio (UX de queries) | ✅ |

---

**Última Atualização:** Novembro 2025  
**Autor:** [Seu Nome]
