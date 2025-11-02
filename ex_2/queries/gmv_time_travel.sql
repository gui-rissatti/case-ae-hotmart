/*
================================================================================
CONSULTA GMV DIÁRIO POR SUBSIDIÁRIA - COM TIME TRAVEL
================================================================================

Esta query demonstra como consultar o GMV (Gross Merchandise Value) diário
por subsidiária usando a tabela fact_purchase_history com SCD Type 2.

Requisitos Atendidos:
✅ GMV diário por subsidiária
✅ Time travel (consulta em qualquer data histórica)
✅ Apenas compras pagas (release_date NOT NULL)
✅ Evita duplicação (usa snapshot temporal correto)

================================================================================
*/

-- =============================================================================
-- OPÇÃO 1: GMV CORRENTE (Dados Atuais)
-- =============================================================================
-- Usa registros marcados como correntes (is_current = TRUE)

SELECT
    order_date AS data_compra,
    subsidiary AS subsidiaria,
    COUNT(DISTINCT purchase_id) AS total_transacoes,
    SUM(purchase_value) AS gmv_total,
    ROUND(AVG(purchase_value), 2) AS ticket_medio
FROM
    fact_purchase_history
WHERE
    is_current = TRUE                    -- Apenas registros correntes
    AND release_date IS NOT NULL         -- Apenas compras pagas
    AND subsidiary IS NOT NULL           -- Filtrar registros sem subsidiária
GROUP BY
    order_date,
    subsidiary
ORDER BY
    order_date,
    subsidiary;


-- =============================================================================
-- OPÇÃO 2: GMV COM TIME TRAVEL (Dados em uma Data Específica)
-- =============================================================================
-- Consulta o estado dos dados em uma data histórica específica
-- Exemplo: "Qual era o GMV de janeiro/2023 em 31/01/2023?"

-- Parâmetro: @as_of_date (ex: '2023-01-31')

SELECT
    order_date AS data_compra,
    subsidiary AS subsidiaria,
    COUNT(DISTINCT purchase_id) AS total_transacoes,
    SUM(purchase_value) AS gmv_total,
    ROUND(AVG(purchase_value), 2) AS ticket_medio
FROM
    fact_purchase_history
WHERE
    -- TIME TRAVEL: Registros válidos naquela data
    effective_date <= '2023-01-31'
    AND (end_date > '2023-01-31' OR end_date IS NULL)
    
    -- Apenas compras pagas
    AND release_date IS NOT NULL
    
    -- Filtrar subsidiárias válidas
    AND subsidiary IS NOT NULL
GROUP BY
    order_date,
    subsidiary
ORDER BY
    order_date,
    subsidiary;


-- =============================================================================
-- OPÇÃO 3: GMV DE UM PERÍODO ESPECÍFICO (Ex: Janeiro/2023)
-- =============================================================================
-- Com Time Travel

SELECT
    order_date AS data_compra,
    subsidiary AS subsidiaria,
    COUNT(DISTINCT purchase_id) AS total_transacoes,
    SUM(purchase_value) AS gmv_total,
    ROUND(AVG(purchase_value), 2) AS ticket_medio
FROM
    fact_purchase_history
WHERE
    -- Filtro de período
    order_date >= '2023-01-01'
    AND order_date < '2023-02-01'
    
    -- Time Travel (opcional - remover para dados correntes)
    AND effective_date <= '2023-01-31'
    AND (end_date > '2023-01-31' OR end_date IS NULL)
    
    -- Apenas compras pagas
    AND release_date IS NOT NULL
    
    -- Filtrar subsidiárias válidas
    AND subsidiary IS NOT NULL
GROUP BY
    order_date,
    subsidiary
ORDER BY
    order_date,
    subsidiary;


-- =============================================================================
-- OPÇÃO 4: GMV MENSAL CONSOLIDADO
-- =============================================================================
-- Agregação por mês e subsidiária

SELECT
    DATE_TRUNC('month', order_date) AS mes,
    subsidiary AS subsidiaria,
    COUNT(DISTINCT purchase_id) AS total_transacoes,
    SUM(purchase_value) AS gmv_total,
    ROUND(AVG(purchase_value), 2) AS ticket_medio
FROM
    fact_purchase_history
WHERE
    is_current = TRUE                    -- Dados correntes
    AND release_date IS NOT NULL         -- Apenas compras pagas
    AND subsidiary IS NOT NULL
GROUP BY
    DATE_TRUNC('month', order_date),
    subsidiary
ORDER BY
    mes,
    subsidiary;


-- =============================================================================
-- VALIDAÇÃO: Comparar GMV em Diferentes Momentos (Time Travel)
-- =============================================================================
-- Esta query mostra como o GMV de janeiro/2023 pode mudar dependendo
-- da data de referência (devido a alterações posteriores)

WITH gmv_jan_31 AS (
    -- GMV de janeiro visto em 31/01/2023
    SELECT
        SUM(purchase_value) AS gmv
    FROM
        fact_purchase_history
    WHERE
        order_date >= '2023-01-01'
        AND order_date < '2023-02-01'
        AND effective_date <= '2023-01-31'
        AND (end_date > '2023-01-31' OR end_date IS NULL)
        AND release_date IS NOT NULL
),
gmv_jul_31 AS (
    -- GMV de janeiro visto em 31/07/2023 (após alterações posteriores)
    SELECT
        SUM(purchase_value) AS gmv
    FROM
        fact_purchase_history
    WHERE
        order_date >= '2023-01-01'
        AND order_date < '2023-02-01'
        AND effective_date <= '2023-07-31'
        AND (end_date > '2023-07-31' OR end_date IS NULL)
        AND release_date IS NOT NULL
)
SELECT
    'Janeiro 2023 em 31/01/2023' AS cenario,
    gmv
FROM
    gmv_jan_31
UNION ALL
SELECT
    'Janeiro 2023 em 31/07/2023' AS cenario,
    gmv
FROM
    gmv_jul_31;


-- =============================================================================
-- NOTAS IMPORTANTES
-- =============================================================================

/*
1. TIME TRAVEL - Como Funciona:
   
   Para obter o snapshot em uma data específica:
   - effective_date <= @as_of_date
   - end_date > @as_of_date OR end_date IS NULL
   
   Isso garante que apenas registros VÁLIDOS naquela data sejam considerados.

2. EVITANDO DUPLICAÇÃO:
   
   ❌ ERRADO: SELECT SUM(purchase_value) FROM fact_purchase_history
   
   Isso vai somar TODAS as versões de cada compra, causando duplicação!
   
   ✅ CORRETO: Usar is_current = TRUE OU filtro temporal correto

3. GMV vs ITEM_VALUE:
   
   - GMV = purchase_value (valor total da compra)
   - item_value é o valor do produto individual
   
   Para GMV, usar purchase_value!

4. APENAS COMPRAS PAGAS:
   
   release_date IS NOT NULL garante que apenas compras efetivamente
   pagas são consideradas no faturamento.

5. IDEMPOTÊNCIA:
   
   Executar a mesma query com mesmo @as_of_date SEMPRE retorna
   o mesmo resultado, mesmo após reprocessamentos.
*/
