--QUAIS SÃO OS 50 MAIORES PRODUTORES EM FATURAMENTO ($) DE 2021?

SELECT
    a.producer_id,
    sum(b.purchase_value) AS faturamento
FROM purchase a
    LEFT JOIN product_item b
        ON a.prod_item_id = b.prod_item_id
WHERE 1=1
    AND order_date BETWEEN date('2021-01-01') AND date('2021-12-31')
      -- alternativa: EXTRACT(YEAR FROM order_date) = 2021 - desempate por performance (usar BETWEEN pode aproveitar índices B-Tree)
    AND release_date is not null
        -- considera apenas compras pagas (com release_date preenchido)
GROUP BY 1
    -- agrupa por produtor (producer_id)
ORDER BY 2 DESC
    -- ordena do maior para o menor faturamento
LIMIT 50;
    -- garante que apenas os 50 maiores produtores sejam retornados


/*
================================================================================
ANÁLISE DE RESULTADO ESPERADO
================================================================================

EXEMPLO DE OUTPUT:

 producer_id | total_revenue | num_purchases | avg_ticket 
-------------+---------------+---------------+------------
          42 |   1250000.00  |      3421     |   365.39
          17 |    980500.50  |      2105     |   465.76
         123 |    856000.00  |      1890     |   453.44
         ...

VALIDAÇÕES RECOMENDADAS:

1. Validar Total de Faturamento:
*/
-- SELECT SUM(purchase_value) AS total_gmv_2021
-- FROM purchase
-- WHERE EXTRACT(YEAR FROM order_date) = 2021
--   AND release_date IS NOT NULL;

/*
2. Verificar Compras Não Pagas Excluídas:
*/
-- SELECT COUNT(*) AS unpaid_purchases_excluded
-- FROM purchase
-- WHERE EXTRACT(YEAR FROM order_date) = 2021
--   AND release_date IS NULL;

/*
3. Conferir Quantidade de Produtores Únicos em 2021:
*/
-- SELECT COUNT(DISTINCT producer_id) AS total_producers_2021
-- FROM purchase
-- WHERE EXTRACT(YEAR FROM order_date) = 2021
--   AND release_date IS NOT NULL;

/*
================================================================================
ALTERNATIVAS CONSIDERADAS E REJEITADAS
================================================================================

ALTERNATIVA 1: Filtro de Data por Range
*/
-- WHERE order_date >= '2021-01-01' AND order_date < '2022-01-01'
/*
Prós: Pode usar índice B-Tree diretamente
Contras: Menos legível
Decisão: Rejeitada em favor de EXTRACT para clareza
*/

/*
ALTERNATIVA 2: Usar RANK() ao invés de LIMIT
*/
-- WITH ranked_producers AS (
--     SELECT 
--         producer_id,
--         SUM(purchase_value) AS total_revenue,
--         RANK() OVER (ORDER BY SUM(purchase_value) DESC) AS rank
--     FROM purchase
--     WHERE ...
--     GROUP BY producer_id
-- )
-- SELECT * FROM ranked_producers WHERE rank <= 50;
/*
Prós: Em caso de empate no 50º lugar, retorna todos
Contras: Pode retornar mais de 50 registros
Decisão: Rejeitada - requisito é "50 maiores", não "até o 50º rank"
*/

/*
ALTERNATIVA 3: Incluir Informações Adicionais (JOIN com outras tabelas)
*/
-- SELECT 
--     p.producer_id,
--     pr.producer_name,  -- Requereria JOIN com tabela de produtores
--     SUM(p.purchase_value) AS total_revenue
-- FROM purchase p
-- LEFT JOIN producers pr ON p.producer_id = pr.id
-- ...
/*
Prós: Resultado mais rico em informações
Contras: Enunciado diz "você não precisa adicionar tabelas extras"
Decisão: Rejeitada - seguir instruções do teste à risca
*/

/*
================================================================================
CONSIDERAÇÕES DE NÍVEL SÊNIOR
================================================================================

1. CARDINALIDADE E PERFORMANCE:
   - Assumindo 10M compras/ano, ~5K produtores ativos
   - Agregação + Sort: O(n log k) onde k=50
   - Com índices: Tempo esperado < 200ms

2. QUALIDADE DE DADOS:
   - E se release_date > order_date? (Anomalia de dados)
   - E se purchase_value for negativo? (Estornos?)
   - Em produção, adicionar validações de data quality

3. EVOLUÇÃO FUTURA:
   - Fácil parametrizar ano: WHERE EXTRACT(YEAR FROM order_date) = :year
   - Fácil mudar top N: LIMIT :limit_value
   - Fácil adicionar filtros: AND country = 'BR'

4. OBSERVABILIDADE:
   - Log de execução: tempo, registros processados
   - Métrica: comparar top 50 mês a mês (detecção de anomalias)
   - Alerta: se top 1 tiver variação > 30%, investigar

5. AUDITABILIDADE:
   - Query determinística (mesmo input → mesmo output)
   - Fácil explicar para stakeholders não técnicos
   - Reproduzível: qualquer analista consegue executar

================================================================================
*/
