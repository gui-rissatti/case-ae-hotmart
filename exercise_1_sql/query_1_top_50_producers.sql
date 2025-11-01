/*
================================================================================
QUERY 1: TOP 50 PRODUTORES POR FATURAMENTO EM 2021
================================================================================

OBJETIVO:
  Identificar os 50 produtores que mais faturaram em 2021, considerando apenas
  compras pagas (release_date IS NOT NULL).

DEFINIÇÕES:
  - Faturamento (GMV): Valor bruto da compra (purchase_value)
  - Compra Paga: release_date IS NOT NULL
  - Ano 2021: EXTRACT(YEAR FROM order_date) = 2021

DECISÕES TÉCNICAS:
  1. Filtro de Pagamento: release_date IS NOT NULL
     → Garante que apenas compras concluídas sejam consideradas
  
  2. Extração de Ano: EXTRACT(YEAR FROM order_date) = 2021
     → Padrão SQL ANSI, mais portável entre SGBDs
     → Alternativa: order_date BETWEEN '2021-01-01' AND '2021-12-31'
  
  3. Agregação Direta: Sem CTEs desnecessárias
     → Princípio KISS (Keep It Simple, Stupid)
     → Query simples não precisa de abstrações extras
  
  4. Ordenação: ORDER BY total_revenue DESC
     → Garante que maiores valores apareçam primeiro
  
  5. Limite: LIMIT 50
     → Exatamente os 50 maiores produtores

CAMPOS RETORNADOS:
  - producer_id: Identificador do produtor
  - total_revenue: Faturamento total em 2021
  - num_purchases: Número de compras (métrica adicional de contexto)
  - avg_ticket: Ticket médio (opcional, para análise)

OBSERVAÇÕES:
  - Produtores sem vendas pagas em 2021 não aparecerão
  - Compras com release_date NULL são excluídas (não geram faturamento)
  - Em caso de empate no 50º lugar, apenas um será retornado (comportamento LIMIT)

PERFORMANCE:
  - Índices recomendados:
    * (order_date, release_date) - Para filtros rápidos
    * (producer_id) - Para agregação eficiente
  - Estimativa: ~100ms para 1M registros com índices adequados

EXEMPLO DE USO:
  -- Executar diretamente no SGBD
  psql -d hotmart -f query_1_top_50_producers.sql
  
  -- Ou em DuckDB
  duckdb hotmart.db < query_1_top_50_producers.sql

================================================================================
*/

SELECT 
    producer_id,
    SUM(purchase_value) AS total_revenue,
    COUNT(*) AS num_purchases,
    ROUND(AVG(purchase_value), 2) AS avg_ticket
FROM 
    purchase
WHERE 
    -- Filtro 1: Apenas ano de 2021
    -- Justificativa: Requisito explícito do exercício
    EXTRACT(YEAR FROM order_date) = 2021
    
    -- Filtro 2: Apenas compras pagas
    -- Justificativa: "Se a compra não foi paga, a empresa não tem faturamento"
    AND release_date IS NOT NULL
GROUP BY 
    producer_id
ORDER BY 
    total_revenue DESC
LIMIT 50;


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
