/*
================================================================================
QUERY: GMV DIÁRIO POR SUBSIDIÁRIA
================================================================================

OBJETIVO:
  Calcular o GMV (Gross Merchandise Value) diário por subsidiária,
  garantindo que:
  1. Não haja duplicação de valores (tabela histórica tem múltiplas versões)
  2. Apenas compras pagas sejam consideradas
  3. Time travel funcione corretamente

DEFINIÇÕES:
  - GMV: Valor bruto das vendas (purchase_value)
  - Subsidiária: NATIONAL ou INTERNATIONAL
  - Diário: Agrupado por order_date

DESAFIO:
  A tabela fact_purchase_history é uma tabela SCD Type 2, onde a mesma
  purchase_id pode aparecer múltiplas vezes com effective_dates diferentes.
  
  Exemplo:
    purchase_id | effective_date | purchase_value
    55          | 2023-01-20     | 1000
    55          | 2023-01-23     | 1000  <- Mesma compra, nova versão!
    55          | 2023-02-05     | 800   <- Valor alterado
  
  Se fizermos simplesmente SUM(purchase_value), vamos contar:
  1000 + 1000 + 800 = 2800 (ERRADO! Triplicou o valor)
  
  Precisamos pegar apenas a versão CORRETA para cada data de consulta.

ESTRATÉGIA:
  1. Definir AS-OF-DATE: Em que ponto no tempo queremos ver os dados?
  2. Filtrar apenas a versão válida naquele momento usando:
     effective_date <= AS-OF-DATE AND (end_date > AS-OF-DATE OR is_current = TRUE)
  3. Garantir que pegamos apenas 1 versão por purchase_id

PARÂMETROS:
  :as_of_date - Data de corte para time travel (ex: '2023-01-31')

================================================================================
*/

-- VERSÃO 1: GMV Diário por Subsidiária (Com Time Travel)
WITH purchase_snapshot AS (
    /*
    Pega snapshot das compras válidas em uma data específica.
    
    Lógica:
    - effective_date <= :as_of_date: A mudança já havia ocorrido
    - end_date > :as_of_date: A mudança ainda estava válida
    - OR is_current = TRUE: Ou é a versão mais recente (sem end_date)
    
    Resultado: 1 registro por purchase_id (não há duplicação)
    */
    SELECT 
        purchase_id,
        order_date,
        release_date,
        purchase_value,
        subsidiary,
        effective_date,
        end_date
    FROM 
        fact_purchase_history
    WHERE 
        -- Time Travel: Pegar versão válida em :as_of_date
        effective_date <= :as_of_date
        AND (
            end_date > :as_of_date 
            OR end_date IS NULL 
            OR is_current = TRUE
        )
)

SELECT 
    order_date AS data,
    subsidiary AS subsidiaria,
    COUNT(DISTINCT purchase_id) AS num_compras,
    SUM(purchase_value) AS gmv_total,
    AVG(purchase_value) AS ticket_medio,
    MIN(purchase_value) AS valor_minimo,
    MAX(purchase_value) AS valor_maximo
FROM 
    purchase_snapshot
WHERE 
    -- Apenas compras pagas geram faturamento
    release_date IS NOT NULL
    
    -- Filtrar período de análise (ex: Janeiro/2023)
    AND order_date >= :start_date
    AND order_date < :end_date
GROUP BY 
    order_date,
    subsidiary
ORDER BY 
    order_date ASC,
    subsidiary ASC;


/*
================================================================================
EXEMPLO DE RESULTADO:
================================================================================

AS-OF-DATE: 2023-01-31 (Fechamento de Janeiro)

 data       | subsidiaria   | num_compras | gmv_total   | ticket_medio 
------------+---------------+-------------+-------------+--------------
 2023-01-20 | NATIONAL      |      12     |  50000.00   |   4166.67
 2023-01-20 | INTERNATIONAL |       8     |  30000.00   |   3750.00
 2023-01-21 | NATIONAL      |      15     |  65000.00   |   4333.33
 2023-01-21 | INTERNATIONAL |      10     |  42000.00   |   4200.00
 ...

INTERPRETAÇÃO:
- No dia 20/01, vendemos 50k nacional e 30k internacional
- Totalizando 80k de GMV no dia


AS-OF-DATE: 2023-02-28 (Fechamento de Fevereiro)

Se alguma compra de Janeiro foi alterada em Fevereiro, o resultado será diferente!

 data       | subsidiaria   | num_compras | gmv_total   | ticket_medio 
------------+---------------+-------------+-------------+--------------
 2023-01-20 | NATIONAL      |      12     |  48500.00   |   4041.67  <- MUDOU!
 2023-01-20 | INTERNATIONAL |       8     |  30000.00   |   3750.00
 ...

Diferença: 50000 - 48500 = 1500 (uma compra foi estornada/alterada)

================================================================================
*/


/*
================================================================================
VERSÃO 2: GMV Mensal Agregado
================================================================================
*/

WITH purchase_snapshot AS (
    SELECT 
        purchase_id,
        order_date,
        release_date,
        purchase_value,
        subsidiary
    FROM 
        fact_purchase_history
    WHERE 
        effective_date <= :as_of_date
        AND (end_date > :as_of_date OR end_date IS NULL OR is_current = TRUE)
)

SELECT 
    DATE_TRUNC('month', order_date) AS mes,
    subsidiary AS subsidiaria,
    COUNT(DISTINCT purchase_id) AS total_compras,
    SUM(purchase_value) AS gmv_mensal,
    AVG(purchase_value) AS ticket_medio_mensal
FROM 
    purchase_snapshot
WHERE 
    release_date IS NOT NULL
    AND order_date >= :start_date
    AND order_date < :end_date
GROUP BY 
    DATE_TRUNC('month', order_date),
    subsidiary
ORDER BY 
    mes ASC,
    subsidiary ASC;


/*
================================================================================
VERSÃO 3: GMV Corrente (Sem Time Travel - Sempre Atual)
================================================================================

Se não precisar de time travel e quiser sempre o estado MAIS RECENTE:
*/

WITH purchase_current AS (
    /*
    Pega apenas registros correntes (is_current = TRUE).
    Mais simples e performático para consultas do "agora".
    */
    SELECT 
        purchase_id,
        order_date,
        release_date,
        purchase_value,
        subsidiary
    FROM 
        fact_purchase_history
    WHERE 
        is_current = TRUE
)

SELECT 
    order_date AS data,
    subsidiary AS subsidiaria,
    COUNT(DISTINCT purchase_id) AS num_compras,
    SUM(purchase_value) AS gmv_total
FROM 
    purchase_current
WHERE 
    release_date IS NOT NULL
    AND order_date >= :start_date
    AND order_date < :end_date
GROUP BY 
    order_date,
    subsidiary
ORDER BY 
    order_date ASC,
    subsidiary ASC;


/*
================================================================================
VALIDAÇÕES RECOMENDADAS
================================================================================

1. Verificar se não há duplicação de purchase_id no snapshot:
*/
WITH purchase_snapshot AS (
    SELECT purchase_id, COUNT(*) as cnt
    FROM fact_purchase_history
    WHERE effective_date <= '2023-01-31'
      AND (end_date > '2023-01-31' OR end_date IS NULL OR is_current = TRUE)
    GROUP BY purchase_id
    HAVING COUNT(*) > 1
)
SELECT * FROM purchase_snapshot;
-- Esperado: 0 registros (nenhuma duplicação)


/*
2. Comparar GMV entre dois períodos (detectar alterações retroativas):
*/
WITH gmv_jan_closing AS (
    -- GMV no fechamento de janeiro (31/01)
    SELECT SUM(purchase_value) as gmv
    FROM fact_purchase_history
    WHERE order_date BETWEEN '2023-01-01' AND '2023-01-31'
      AND release_date IS NOT NULL
      AND effective_date <= '2023-01-31'
      AND (end_date > '2023-01-31' OR end_date IS NULL OR is_current = TRUE)
),
gmv_feb_view AS (
    -- GMV de janeiro visto de fevereiro (28/02)
    SELECT SUM(purchase_value) as gmv
    FROM fact_purchase_history
    WHERE order_date BETWEEN '2023-01-01' AND '2023-01-31'
      AND release_date IS NOT NULL
      AND effective_date <= '2023-02-28'
      AND (end_date > '2023-02-28' OR end_date IS NULL OR is_current = TRUE)
)
SELECT 
    j.gmv as gmv_jan_closing,
    f.gmv as gmv_feb_view,
    f.gmv - j.gmv as difference,
    ROUND(100.0 * (f.gmv - j.gmv) / NULLIF(j.gmv, 0), 2) as pct_change
FROM gmv_jan_closing j, gmv_feb_view f;


/*
================================================================================
CONSIDERAÇÕES DE PERFORMANCE
================================================================================

1. ÍNDICES RECOMENDADOS:
   - (is_current) WHERE is_current = TRUE
   - (effective_date, end_date) para time travel
   - (order_date, release_date) para filtros temporais

2. PARTICIONAMENTO:
   - Tabela particionada por transaction_date
   - Para queries de GMV, usar filtro adicional:
     AND transaction_date <= :as_of_date
     (Prunning de partições futuras)

3. CACHING:
   - Se rodar múltiplas queries com mesmo as_of_date, cachear snapshot:
     CREATE TEMP TABLE purchase_snapshot_20230131 AS ...

4. MATERIALIZAÇÃO:
   - Para relatórios recorrentes, materializar snapshots mensais
   - Executar fim de mês e persistir resultado

================================================================================
*/
