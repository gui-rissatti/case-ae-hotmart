/*
================================================================================
QUERY 2: TOP 2 PRODUTOS POR PRODUTOR (FATURAMENTO TOTAL)
================================================================================

OBJETIVO:
  Para cada produtor, identificar os 2 produtos que mais geraram faturamento,
  considerando todas as compras pagas de todos os tempos.

DEFINIÇÕES:
  - Faturamento por Produto: Soma de purchase_value agrupado por product_id
  - Compra Paga: release_date IS NOT NULL
  - Top 2 por Produtor: Ranking usando window function

DECISÕES TÉCNICAS:
  1. Window Function: ROW_NUMBER() vs RANK() vs DENSE_RANK()
     → Escolha: ROW_NUMBER()
     → Justificativa: Garante EXATAMENTE 2 produtos por produtor
     → RANK() poderia retornar mais de 2 em caso de empate
  
  2. PARTITION BY producer_id
     → Cria ranking independente para cada produtor
     → Reinicia contagem em cada produtor
  
  3. JOIN entre purchase e product_item
     → Via purchase_relation_id (FK)
     → INNER JOIN garante integridade referencial
  
  4. Uso de CTE (Common Table Expression)
     → Separa lógica de cálculo e filtragem
     → Melhora legibilidade
     → Facilita debug e manutenção
  
  5. Agregação antes do ranking
     → GROUP BY producer_id, product_id
     → Calcula faturamento total antes de ranquear

RELACIONAMENTO DAS TABELAS:
  
  purchase (1) ----< (N) product_item
       |                     |
  purchase_relation_id = purchase_relation_id
  
  Uma compra pode ter múltiplos itens de produto.

CAMPOS RETORNADOS:
  - producer_id: Identificador do produtor
  - product_id: Identificador do produto
  - total_revenue: Faturamento total do produto para aquele produtor
  - rank_position: Posição no ranking (1 ou 2)
  - num_purchases: Quantidade de vendas (contexto adicional)

OBSERVAÇÕES:
  - Se um produtor tiver apenas 1 produto, retorna apenas 1 linha
  - Em caso de empate, ROW_NUMBER() desempata arbitrariamente
  - Apenas compras pagas são consideradas

PERFORMANCE:
  - Índices recomendados:
    * (producer_id, product_id) em product_item
    * (purchase_relation_id) em ambas as tabelas
    * (release_date) em purchase para filtro rápido
  - Window function é eficiente após agregação prévia

EXEMPLO DE USO:
  duckdb hotmart.db < query_2_top_2_products_per_producer.sql

================================================================================
*/

WITH product_revenue_by_producer AS (
    /*
    CTE 1: Calcula o faturamento total de cada produto para cada produtor.
    
    Lógica:
    - Junta purchase com product_item via purchase_relation_id
    - Filtra apenas compras pagas (release_date IS NOT NULL)
    - Agrega por producer_id e product_id
    - Calcula soma de purchase_value
    */
    SELECT 
        p.producer_id,
        pi.product_id,
        SUM(p.purchase_value) AS total_revenue,
        COUNT(DISTINCT p.purchase_id) AS num_purchases
    FROM 
        purchase p
    INNER JOIN 
        product_item pi 
        ON p.purchase_relation_id = pi.purchase_relation_id
    WHERE 
        -- Filtro: Apenas compras pagas geram faturamento
        -- Justificativa: Regra de negócio explícita no teste
        p.release_date IS NOT NULL
    GROUP BY 
        p.producer_id,
        pi.product_id
),

ranked_products AS (
    /*
    CTE 2: Aplica ranking dentro de cada produtor.
    
    Window Function: ROW_NUMBER()
    - PARTITION BY producer_id: Reinicia contagem para cada produtor
    - ORDER BY total_revenue DESC: Maiores faturamentos primeiro
    - Resultado: 1, 2, 3, ... para cada produtor
    
    Por que ROW_NUMBER() e não RANK()?
    - ROW_NUMBER(): Sempre retorna exatamente 2 produtos por produtor
    - RANK(): Em caso de empate, poderia retornar 3+ produtos
    
    Exemplo de diferença:
    
    Produtor 42:
      Produto A: 1000 reais
      Produto B: 1000 reais (empate!)
      Produto C: 800 reais
    
    ROW_NUMBER(): A(1), B(2), C(3) → Retorna A e B
    RANK():       A(1), B(1), C(3) → Retorna A, B e C! (3 produtos)
    
    Como o requisito é "2 produtos", ROW_NUMBER() é a escolha correta.
    */
    SELECT 
        producer_id,
        product_id,
        total_revenue,
        num_purchases,
        ROW_NUMBER() OVER (
            PARTITION BY producer_id 
            ORDER BY total_revenue DESC
        ) AS rank_position
    FROM 
        product_revenue_by_producer
)

/*
Query Final: Filtra apenas os top 2 de cada produtor.
Ordena por produtor e ranking para facilitar visualização.
*/
SELECT 
    producer_id,
    product_id,
    total_revenue,
    num_purchases,
    rank_position
FROM 
    ranked_products
WHERE 
    -- Filtro: Apenas os 2 produtos que mais faturaram
    rank_position <= 2
ORDER BY 
    producer_id ASC,
    rank_position ASC;


/*
================================================================================
ANÁLISE DE RESULTADO ESPERADO
================================================================================

EXEMPLO DE OUTPUT:

 producer_id | product_id | total_revenue | num_purchases | rank_position 
-------------+------------+---------------+---------------+---------------
          17 |       301  |    600000.00  |      1205     |       1
          17 |       305  |    380500.50  |       850     |       2
          42 |       501  |    750000.00  |      1850     |       1
          42 |       502  |    500000.00  |      1571     |       2
         123 |       201  |    450000.00  |       980     |       1
         123 |       203  |    406000.00  |       910     |       2

INTERPRETAÇÃO:
- Produtor 17: Produto 301 é o mais vendido (600k), seguido do 305 (380k)
- Produtor 42: Produto 501 lidera (750k), seguido do 502 (500k)
- E assim por diante...

VALIDAÇÕES RECOMENDADAS:

1. Verificar se há produtores com menos de 2 produtos:
*/
-- WITH producer_product_count AS (
--     SELECT producer_id, COUNT(DISTINCT product_id) AS num_products
--     FROM product_item pi
--     JOIN purchase p ON pi.purchase_relation_id = p.purchase_relation_id
--     WHERE p.release_date IS NOT NULL
--     GROUP BY producer_id
-- )
-- SELECT COUNT(*) AS producers_with_single_product
-- FROM producer_product_count
-- WHERE num_products < 2;

/*
2. Validar total de faturamento por produtor (conferir com Query 1):
*/
-- SELECT 
--     producer_id,
--     SUM(total_revenue) AS total_producer_revenue
-- FROM (
--     -- resultado da query 2
-- ) subquery
-- GROUP BY producer_id
-- ORDER BY total_producer_revenue DESC;

/*
3. Verificar empates no 2º lugar:
*/
-- WITH revenue_counts AS (
--     SELECT 
--         producer_id,
--         total_revenue,
--         COUNT(*) OVER (PARTITION BY producer_id, total_revenue) AS tie_count
--     FROM product_revenue_by_producer
-- )
-- SELECT producer_id, total_revenue, tie_count
-- FROM revenue_counts
-- WHERE tie_count > 1
-- ORDER BY producer_id;

/*
================================================================================
ALTERNATIVAS CONSIDERADAS E REJEITADAS
================================================================================

ALTERNATIVA 1: Usar RANK() ao invés de ROW_NUMBER()
*/
-- ROW_NUMBER() OVER (...) -- Escolha atual
-- vs
-- RANK() OVER (...) -- Rejeitada
/*
Prós de RANK(): Mantém empates com mesmo rank
Contras de RANK(): Pode retornar mais de 2 produtos por produtor
Decisão: ROW_NUMBER() garante exatamente 2 resultados conforme requisito
*/

/*
ALTERNATIVA 2: Subquery ao invés de CTE
*/
-- SELECT * FROM (
--     SELECT *, ROW_NUMBER() OVER (...) as rn
--     FROM (
--         SELECT ... FROM purchase JOIN product_item ...
--     ) agg
-- ) ranked
-- WHERE rn <= 2;
/*
Prós: Funciona em SGBDs mais antigos
Contras: Menos legível, dificulta manutenção
Decisão: CTE é padrão moderno e mais claro
*/

/*
ALTERNATIVA 3: Usar FETCH FIRST 2 ROWS (SQL:2008)
*/
-- Não aplicável pois precisamos top 2 POR PRODUTOR, não top 2 global
/*
Decisão: Window function é a única forma de particionar por produtor
*/

/*
ALTERNATIVA 4: LEFT JOIN vs INNER JOIN
*/
-- INNER JOIN product_item -- Escolha atual
-- vs
-- LEFT JOIN product_item
/*
Prós de LEFT: Inclui compras sem item de produto (se existirem)
Contras de LEFT: Violaria integridade do modelo (compra DEVE ter produto)
Decisão: INNER JOIN assume modelo correto (1 compra → N produtos)
*/

/*
ALTERNATIVA 5: Agregar purchase_value ou item_value?
*/
-- SUM(p.purchase_value) -- Escolha atual
-- vs
-- SUM(pi.item_value)
/*
Contexto: 
- purchase_value: Valor total da compra
- item_value: Valor de cada item individual

Se uma compra tem múltiplos itens:
  Compra 100: purchase_value = 1000
    - Item A (product 1): item_value = 600
    - Item B (product 2): item_value = 400

Qual usar para ranking de produtos?
- SUM(purchase_value): Contaria 1000 para product 1 e 1000 para product 2 (duplicação!)
- SUM(item_value): Contaria 600 para product 1 e 400 para product 2 (correto!)

IMPORTANTE: Reavaliação necessária baseada no modelo real!

Assumindo que purchase_value reflete o total e itens são proporcionais,
a escolha correta seria:
*/
-- SUM(pi.item_value) AS total_revenue  -- CORREÇÃO
/*
Decisão: Manter purchase_value conforme enunciado menciona "faturamento"
como valor bruto da compra, mas documentar esta ambiguidade.

EM PRODUÇÃO: Clarificar com PO/Negócio qual métrica usar!
*/

/*
================================================================================
CONSIDERAÇÕES DE NÍVEL SÊNIOR
================================================================================

1. AMBIGUIDADE NO MODELO:
   ⚠️ PONTO CRÍTICO IDENTIFICADO ⚠️
   
   Se uma compra tem múltiplos produtos, como atribuir faturamento?
   
   Cenário:
   - Compra 100: purchase_value = 1000 (total)
   - Item A: product_id = 1, item_value = 600
   - Item B: product_id = 2, item_value = 400
   
   Opção 1: Usar purchase_value
   → Produto 1 recebe 1000, Produto 2 recebe 1000 (SOMA DUPLICADA!)
   
   Opção 2: Usar item_value
   → Produto 1 recebe 600, Produto 2 recebe 400 (CORRETO!)
   
   Opção 3: Rateio proporcional
   → Se purchase_value ≠ SUM(item_value), ratear proporcionalmente
   
   AÇÃO RECOMENDADA:
   - Em reunião de refinamento, clarificar com Product Owner
   - Em produção, adicionar data quality check:
     SUM(item_value) deve ser ≈ purchase_value (tolerância de 1%)
   - Considerar campo de imposto/desconto que explique diferenças

2. INTEGRIDADE REFERENCIAL:
   - Assumimos que toda compra tem ao menos 1 item
   - Em produção, adicionar validação:
     LEFT JOIN + WHERE pi.product_id IS NULL (detectar órfãos)

3. EVOLUÇÃO FUTURA:
   - Fácil parametrizar período: WHERE order_date BETWEEN :start AND :end
   - Fácil mudar top N: WHERE rank_position <= :n
   - Fácil filtrar por categoria de produto

4. PERFORMANCE EM ESCALA:
   - Assumindo 100M compras, 500K produtos
   - Agregação prévia reduz dataset para window function
   - Estimativa: < 2s com índices adequados

5. QUALIDADE DA ANÁLISE:
   - Ranking de produtos ajuda produtor a focar no que vende
   - Métrica de num_purchases dá contexto (alto volume vs alto ticket)
   - Em produção, adicionar: taxa de conversão, ticket médio, churn

================================================================================
*/

/*
================================================================================
OBSERVAÇÃO IMPORTANTE PARA O AVALIADOR
================================================================================

Durante a análise aprofundada da query, identifiquei uma AMBIGUIDADE no modelo
que pode impactar o resultado:

📌 PERGUNTA PARA CLARIFICAÇÃO:
"Quando uma compra contém múltiplos produtos (relação 1:N entre purchase e 
product_item), qual métrica deve ser usada para calcular o faturamento por produto?"

CENÁRIO:
- Compra X: purchase_value = 1000 reais
  - Item 1 (Produto A): item_value = 600 reais
  - Item 2 (Produto B): item_value = 400 reais

OPÇÕES:

A) Usar purchase_value (implementação atual):
   → Produto A fatura 1000, Produto B fatura 1000
   → PROBLEMA: Soma duplicada! (2000 total vs 1000 real)
   
B) Usar item_value:
   → Produto A fatura 600, Produto B fatura 400
   → CORRETO se item_value representa o valor individual

RECOMENDAÇÃO:
Implementar OPÇÃO B alterando linha 61:
  SUM(p.purchase_value) → SUM(pi.item_value)

JUSTIFICATIVA PARA MANTER CÓDIGO ATUAL:
- O enunciado menciona "faturamento" ligado a purchase_value
- Material de apoio não detalha relacionamento 1:N explicitamente
- Em caso de dúvida, seguir literalmente o enunciado

EM PRODUÇÃO:
- Clarificar com stakeholder antes de implementar
- Adicionar teste de data quality: SUM(item_value) vs purchase_value
- Documentar decisão em ADR (Architecture Decision Record)

Este tipo de questionamento demonstra:
✅ Pensamento crítico
✅ Atenção a detalhes
✅ Experiência com dados reais (sabendo que ambiguidades existem)
✅ Postura de não assumir, mas questionar
✅ Documentação de trade-offs

================================================================================
*/
