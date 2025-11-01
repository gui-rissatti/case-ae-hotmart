# Exercício 1: SQL Queries

## 📌 Contexto

Este exercício simula o modelo de negócio da Hotmart, onde:
- **Creators (Produtores)**: Disponibilizam produtos (cursos, ebooks, etc.) na plataforma
- **Buyers (Compradores)**: Adquirem esses produtos
- **Faturamento (GMV)**: Valor bruto das compras **pagas** (release_date IS NOT NULL)

## 🗂️ Modelo de Dados

### Tabela: `purchase` (Compras - Corrente)

Esta tabela mantém o **último status** de cada compra.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `purchase_id` | BIGINT | Identificador único da compra |
| `buyer_id` | BIGINT | Identificador do comprador |
| `purchase_relation_id` | BIGINT | Relacionamento entre compra e item |
| `order_date` | DATE | Data em que o pedido foi efetuado |
| `release_date` | DATE | Data em que o pagamento foi confirmado (NULL = não pago) |
| `producer_id` | BIGINT | Identificador do produtor |
| `purchase_value` | DECIMAL(10,2) | Valor bruto da compra |

**Observações Importantes:**
- Apenas registros com `release_date IS NOT NULL` representam compras pagas
- Se `release_date` é NULL, a compra não foi concluída (aguardando pagamento, cancelada, etc.)

### Tabela: `product_item` (Itens e Produtos - Corrente)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `product_item_id` | BIGINT | Identificador único do item de produto |
| `purchase_relation_id` | BIGINT | FK para relacionar com purchase |
| `product_id` | BIGINT | Identificador do produto |
| `item_value` | DECIMAL(10,2) | Valor do item específico |

**Relacionamento:**
```
purchase (1) ----< (N) product_item
   via purchase_relation_id
```

## 🎯 Objetivos

### Query 1: Top 50 Produtores em Faturamento (2021)
Identificar os 50 produtores que mais faturaram em 2021, ordenados do maior para o menor.

### Query 2: Top 2 Produtos por Produtor
Para cada produtor, identificar os 2 produtos que mais geraram faturamento.

## 📝 Perguntas Negociais

### **1. Quais são os 50 maiores produtores em faturamento de 2021?**

**Definições:**
- **Faturamento**: Soma de `purchase_value` de todas as compras **pagas** (release_date NOT NULL)
- **Ano 2021**: Filtro no campo `order_date`
- **Top 50**: Ordenar decrescente e limitar em 50 registros

➡️ **Solução:** [`query_1_top_50_producers.sql`](./query_1_top_50_producers.sql)

---

### **2. Quais são os 2 produtos que mais faturaram de cada produtor?**

**Definições:**
- **Por Produtor**: Agrupar por `producer_id` e ranquear dentro de cada grupo
- **Faturamento por Produto**: Soma de `purchase_value` para cada `product_id`
- **Top 2**: Usando `ROW_NUMBER()` ou `RANK()` para pegar apenas os 2 primeiros

➡️ **Solução:** [`query_2_top_2_products_per_producer.sql`](./query_2_top_2_products_per_producer.sql)

---

## 🔍 Decisões Técnicas

### Decisão 1: Filtro de Compras Pagas

**Contexto:**  
A transcrição enfatiza: *"É importante ressaltar que se a compra não foi paga, a empresa não tem faturamento."*

**Decisão:**  
Aplicar filtro `WHERE release_date IS NOT NULL` em todas as queries.

**Alternativas Consideradas:**
- ❌ Filtrar por status de pagamento (se existisse coluna específica)
- ✅ Usar `release_date IS NOT NULL` (campo disponível que indica pagamento confirmado)

**Justificativa:**  
O campo `release_date` representa o momento em que o pagamento foi liberado/confirmado. Se for NULL, a compra não gerou faturamento real.

---

### Decisão 2: Extração do Ano

**Contexto:**  
Precisamos filtrar apenas compras de 2021.

**Decisão:**  
Usar `EXTRACT(YEAR FROM order_date) = 2021` ou `order_date BETWEEN '2021-01-01' AND '2021-12-31'`.

**Alternativas Consideradas:**
- ❌ `YEAR(order_date) = 2021` (menos portável entre SGBDs)
- ✅ `EXTRACT(YEAR FROM order_date) = 2021` (padrão SQL ANSI)
- ✅ `order_date >= '2021-01-01' AND order_date < '2022-01-01'` (pode usar índice)

**Justificativa:**  
`EXTRACT` é mais legível e padrão. Para performance crítica, range de datas seria preferível (permite uso de índice na coluna `order_date`).

---

### Decisão 3: ROW_NUMBER vs RANK vs DENSE_RANK

**Contexto:**  
Query 2 precisa ranquear produtos dentro de cada produtor.

**Decisão:**  
Usar `ROW_NUMBER()` para garantir exatamente 2 produtos por produtor.

**Diferenças:**

| Função | Comportamento com Empates | Uso |
|--------|---------------------------|-----|
| `ROW_NUMBER()` | Atribui números únicos (quebra empates arbitrariamente) | Quando queremos exatamente N registros |
| `RANK()` | Pula números após empates (1,1,3) | Quando empates devem ter mesmo rank |
| `DENSE_RANK()` | Não pula números (1,1,2) | Quando queremos ranks consecutivos |

**Exemplo:**
```
Produtor 42:
  Produto 101: 1000 reais
  Produto 102: 1000 reais (empate!)
  Produto 103: 800 reais

ROW_NUMBER(): 101(1), 102(2), 103(3) → Retorna 101 e 102
RANK():       101(1), 102(1), 103(3) → Retorna 101, 102 e 103!
```

**Justificativa:**  
Como o requisito é "**2 produtos** que mais faturaram", `ROW_NUMBER()` garante exatamente 2 registros. Se houvesse necessidade de tratar empates igualmente, usaríamos `RANK()` com filtro `<= 2` (mas retornaria mais que 2 em casos de empate).

---

### Decisão 4: Join vs Subquery

**Contexto:**  
Query 2 precisa combinar `purchase` e `product_item`.

**Decisão:**  
Usar `INNER JOIN` explícito via `purchase_relation_id`.

**Alternativas Consideradas:**
- ❌ Subquery correlacionada (menos performático)
- ✅ `INNER JOIN` (mais legível e otimizável pelo motor SQL)

**Justificativa:**  
Joins são mais eficientes e legíveis para combinar tabelas relacionadas. O otimizador SQL tem mais liberdade para escolher estratégias de execução.

---

### Decisão 5: Agregação Direta vs CTE

**Contexto:**  
Query 1 precisa apenas somar e ordenar.

**Decisão Query 1:**  
Agregação direta sem CTEs desnecessárias.

```sql
SELECT producer_id, SUM(purchase_value) AS total_revenue
FROM purchase
WHERE ...
GROUP BY producer_id
ORDER BY total_revenue DESC
LIMIT 50;
```

**Decisão Query 2:**  
Usar CTE para separar cálculo de ranking e filtragem.

```sql
WITH ranked_products AS (
    SELECT 
        producer_id,
        product_id,
        SUM(purchase_value) AS revenue,
        ROW_NUMBER() OVER (PARTITION BY producer_id ORDER BY SUM(purchase_value) DESC) AS rank
    FROM ...
    GROUP BY producer_id, product_id
)
SELECT * FROM ranked_products WHERE rank <= 2;
```

**Justificativa:**  
- Query 1: Simples, não precisa de CTEs (princípio KISS - Keep It Simple)
- Query 2: CTE melhora legibilidade ao separar lógica de ranking e filtragem

---

## 📊 Exemplo de Resultados Esperados

### Query 1: Top 50 Produtores

```
 producer_id | total_revenue | num_purchases 
-------------+---------------+---------------
          42 |   1250000.00  |      3421
          17 |    980500.50  |      2105
         123 |    856000.00  |      1890
         ...
```

### Query 2: Top 2 Produtos por Produtor

```
 producer_id | product_id | revenue    | rank 
-------------+------------+------------+------
          42 |       501  | 750000.00  |  1
          42 |       502  | 500000.00  |  2
          17 |       301  | 600000.00  |  1
          17 |       305  | 380500.50  |  2
         ...
```

---

## 🧪 Testes e Validações

### Casos de Teste

1. **Compras Não Pagas**: Garantir que `release_date IS NULL` não entra no cálculo
2. **Ano Incorreto**: Compras de 2020 ou 2022 não devem aparecer
3. **Produtores sem Vendas em 2021**: Não devem aparecer no resultado
4. **Empates no Top 2**: Validar comportamento do `ROW_NUMBER()`

### Queries de Validação

```sql
-- Validar que apenas compras pagas foram consideradas
SELECT COUNT(*) FROM purchase 
WHERE release_date IS NULL 
  AND EXTRACT(YEAR FROM order_date) = 2021;
-- Esperado: Não devem estar no resultado final

-- Validar total de faturamento 2021
SELECT SUM(purchase_value) AS total_gmv_2021
FROM purchase
WHERE EXTRACT(YEAR FROM order_date) = 2021
  AND release_date IS NOT NULL;
```

---

## 📚 Referências

- [Material de Apoio - DER](link-para-diagrama-fornecido)
- Transcrição do vídeo explicativo
- Requisitos do teste técnico

---

## 💡 Observações Finais

- **Portabilidade**: Queries escritas em SQL padrão ANSI quando possível
- **Performance**: Considerações de índices (order_date, producer_id, product_id)
- **Manutenibilidade**: Código comentado e formatado para fácil compreensão
- **Extensibilidade**: Estrutura permite facilmente adicionar filtros adicionais

---

**Desenvolvido por:** [Seu Nome]  
**Data:** Novembro 2025
