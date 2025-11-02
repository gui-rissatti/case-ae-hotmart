# Exercício 2 - Solução ETL Purchase History

## 📋 Visão Geral

Esta solução implementa um **pipeline ETL para construção de tabela histórica de compras** atendendo todos os requisitos do desafio técnico da Hotmart.

## ✅ Requisitos Atendidos

| Requisito | Status | Implementação |
|-----------|--------|---------------|
| Modelagem histórica com rastreabilidade | ✅ | SCD Type 2 (effective_date, end_date, is_current) |
| Processamento D-1 | ✅ | Incremental por transaction_date |
| Tratamento assíncrono de 3 tabelas | ✅ | Full outer join |
| Forward fill (repetir dados não atualizados) | ✅ | Coalesce com valores anteriores |
| Idempotência (reprocessável) | ✅ | DELETE + INSERT por partição |
| Time travel (navegação temporal) | ✅ | Query com as_of_date |
| Facilidade para dados correntes | ✅ | Flag is_current |
| GMV diário por subsidiária | ✅ | Query com agrupamento |
| Particionamento | ✅ | Por transaction_date |
| Não alterar o passado | ✅ | SCD Type 2 preserva histórico |

## 🏗️ Arquitetura da Solução

### Modelo de Dados

**Tabela Final: `fact_purchase_history`**

```
purchase_id              # ID da compra (grain principal)
purchase_relation_id     # ID de relacionamento
transaction_date         # Data do evento (partição)
buyer_id                 # ID do comprador
order_date               # Data do pedido
release_date             # Data de liberação (pagamento)
producer_id              # ID do produtor
purchase_value           # Valor bruto (GMV)
product_item_id          # ID do item
product_id               # ID do produto
item_value               # Valor do item
purchase_extra_info_id   # ID de info extra
subsidiary               # NATIONAL ou INTERNATIONAL

--- Colunas SCD Type 2 ---
effective_date           # Data início da validade
end_date                 # Data fim da validade (NULL = corrente)
is_current               # Flag de registro corrente (TRUE/FALSE)
```

### Pipeline ETL

```
┌─────────────────────────────────────────────────────────────┐
│  STEP 1: Ler Eventos D-1                                    │
│  - purchase (transaction_date = D-1)                        │
│  - product_item (transaction_date = D-1)                    │
│  - purchase_extra_info (transaction_date = D-1)             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 2: Merge Assíncrono (Full Outer Join)                │
│  - purchase ⟕ product_item ⟕ purchase_extra_info           │
│  - Coalesce para pegar valores não-nulos                    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 3: Forward Fill                                       │
│  - Buscar valores anteriores (is_current = TRUE)            │
│  - Coalesce: usar novo se existir, senão repetir anterior  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 4: Detectar Mudanças Reais                           │
│  - Comparar hash do registro atual vs anterior              │
│  - Filtrar apenas registros que mudaram                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 5: Aplicar SCD Type 2                                 │
│  - effective_date = D-1                                     │
│  - end_date = NULL                                          │
│  - is_current = TRUE                                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 6: Atualizar Registros Anteriores                    │
│  - end_date = D-1 (para registros que mudaram)              │
│  - is_current = FALSE                                       │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  STEP 7: Escrever Partição (Idempotente)                   │
│  - DELETE partition WHERE transaction_date = D-1            │
│  - INSERT novos registros                                   │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Como Usar

### Pré-requisitos

```bash
# Instalar PySpark
pip install pyspark
```

### 1. Criar Dados de Exemplo

```bash
python etl_purchase_history.py --create-sample-data
```

### 2. Processar Dias Sequencialmente

```bash
# Processar 20/01/2023
python etl_purchase_history.py --create-sample-data --process-date 2023-01-20

# Processar 21/01/2023
python etl_purchase_history.py --process-date 2023-01-21

# Processar 23/01/2023 (subsidiária da compra 55 chega!)
python etl_purchase_history.py --process-date 2023-01-23

# Processar 05/02/2023 (buyer_id da compra 55 muda!)
python etl_purchase_history.py --process-date 2023-02-05

# Processar 12/07/2023 (item_value da compra 55 muda!)
python etl_purchase_history.py --process-date 2023-07-12

# Processar 15/07/2023 (release_date da compra 55 atualizada!)
python etl_purchase_history.py --process-date 2023-07-15
```

### 3. Consultar GMV com Time Travel

```bash
# GMV corrente (is_current = TRUE)
python etl_purchase_history.py --query-gmv

# GMV em 31/01/2023 (navegando no tempo)
python etl_purchase_history.py --query-gmv --as-of-date 2023-01-31

# GMV em 31/07/2023 (após todas as alterações)
python etl_purchase_history.py --query-gmv --as-of-date 2023-07-31
```

## 📊 Exemplo de Resultado

### Compra 55 - Timeline

```
Date         | Event                      | buyer_id | item_value | subsidiary | effective_date | end_date   | is_current
-------------|----------------------------|----------|------------|------------|----------------|------------|------------
2023-01-20   | purchase + product_item    | 100      | 600.00     | NULL       | 2023-01-20     | 2023-01-23 | FALSE
2023-01-23   | subsidiary chega           | 100      | 600.00     | NATIONAL   | 2023-01-23     | 2023-02-05 | FALSE
2023-02-05   | buyer_id muda              | 200      | 600.00     | NATIONAL   | 2023-02-05     | 2023-07-12 | FALSE
2023-07-12   | item_value muda            | 200      | 550.00     | NATIONAL   | 2023-07-12     | 2023-07-15 | FALSE
2023-07-15   | release_date atualizada    | 200      | 550.00     | NATIONAL   | 2023-07-15     | NULL       | TRUE ← Corrente
```

### GMV Time Travel

**Cenário**: Compra 55 tinha valor de R$ 1000,00 em janeiro/2023

```sql
-- GMV de janeiro em 31/01/2023 (antes das alterações)
SELECT SUM(purchase_value) FROM fact_purchase_history
WHERE order_date BETWEEN '2023-01-01' AND '2023-01-31'
  AND effective_date <= '2023-01-31'
  AND (end_date > '2023-01-31' OR end_date IS NULL)
  AND release_date IS NOT NULL;
-- Resultado: R$ 1000,00

-- GMV de janeiro em 31/07/2023 (após alterações)
SELECT SUM(purchase_value) FROM fact_purchase_history
WHERE order_date BETWEEN '2023-01-01' AND '2023-01-31'
  AND effective_date <= '2023-07-31'
  AND (end_date > '2023-07-31' OR end_date IS NULL)
  AND release_date IS NOT NULL;
-- Resultado: R$ 1000,00 (purchase_value não mudou, apenas item_value)
```

## 🎯 Decisões Técnicas

### 1. Por que SCD Type 2?

- **Rastreabilidade completa**: Cada mudança gera nova linha
- **Time travel**: Permite consultar estado em qualquer data
- **Auditável**: Histórico completo para compliance/financeiro
- **Não muta o passado**: Preserva fechamentos contábeis

### 2. Por que Full Outer Join?

- **Assincronicidade**: Tabelas não chegam juntas
- **Ordem não importa**: product_item pode chegar antes de purchase
- **Completude**: Não perde nenhum evento

### 3. Por que Forward Fill?

- **Requisito explícito**: "Repetir conteúdo" quando não há atualização
- **Integridade**: Garante que todos os campos tenham valor
- **Facilita análise**: Não precisa buscar valores em registros anteriores

### 4. Por que Detecção de Mudanças?

- **Eficiência**: Não insere linha se nada mudou
- **Storage otimizado**: Evita duplicação desnecessária
- **Performance**: Menos dados para processar

### 5. Por que DELETE + INSERT?

- **Idempotência forte**: Reprocessar D-1 sempre dá mesmo resultado
- **Simplicidade**: Mais fácil de entender e manter
- **Sem conflitos**: Não depende de MERGE/UPSERT complexo

## 🔧 Melhorias Futuras

### Prioritárias

1. **Validações de Data Quality**
   - Checar valores negativos
   - Validar datas (release_date >= order_date)
   - Detectar duplicatas

2. **Testes Automatizados**
   - Testes unitários por função
   - Testes de integração end-to-end
   - Testes de idempotência

3. **Observabilidade**
   - Métricas detalhadas (registros processados, tempo, etc)
   - Alertas para anomalias
   - Dashboard de monitoramento

### Secundárias

4. **Otimizações de Performance**
   - Broadcast joins para tabelas pequenas
   - Cache de DataFrames intermediários
   - Compactação de histórico antigo

5. **Tratamento de Late Arriving Data**
   - Dados que chegam com atraso > D-1
   - Estratégia de backfill

6. **CI/CD**
   - Pipeline automatizado
   - Deploy em ambientes (dev/staging/prod)
   - Rollback automático em caso de falha

## 📝 Estrutura de Arquivos

```
ex_2/
├── etl_purchase_history.py          # Script principal ETL (ÚNICO ARQUIVO)
├── README_SOLUTION.md                # Documentação da solução
├── queries/
│   └── gmv_daily_by_subsidiary.sql  # Query de exemplo (legacy)
└── data/
    └── input/
        └── sample_data_explained.txt # Dados de exemplo
```

## 🧪 Testando a Solução

### Teste 1: Idempotência

```bash
# Processar 2023-01-20
python etl_purchase_history.py --create-sample-data --process-date 2023-01-20

# Consultar GMV
python etl_purchase_history.py --query-gmv
# Anotar resultado: GMV = X

# Reprocessar 2023-01-20 (idempotência!)
python etl_purchase_history.py --process-date 2023-01-20

# Consultar GMV novamente
python etl_purchase_history.py --query-gmv
# Resultado DEVE SER IGUAL: GMV = X
```

### Teste 2: Time Travel

```bash
# Processar todos os dias
python etl_purchase_history.py --create-sample-data --process-date 2023-01-20
python etl_purchase_history.py --process-date 2023-01-23
python etl_purchase_history.py --process-date 2023-02-05
python etl_purchase_history.py --process-date 2023-07-12
python etl_purchase_history.py --process-date 2023-07-15

# GMV em 31/01 (antes das alterações de fevereiro)
python etl_purchase_history.py --query-gmv --as-of-date 2023-01-31

# GMV em 31/07 (após todas as alterações)
python etl_purchase_history.py --query-gmv --as-of-date 2023-07-31

# Os valores DEVEM SER DIFERENTES se houve mudança em purchase_value
```

### Teste 3: Forward Fill

```bash
# Verificar compra 55 após subsidiária chegar (23/01)
# Campos de purchase e product_item devem estar REPETIDOS
# Apenas subsidiary deve ser NOVO (NATIONAL)
```

## 🎓 Conceitos Aplicados

- **SCD Type 2**: Slowly Changing Dimensions Type 2
- **Time Travel**: Consulta temporal (point-in-time query)
- **Forward Fill**: Repetição de valores não atualizados
- **Idempotência**: Reprocessável sem side effects
- **Full Outer Join**: Join que preserva todos os registros
- **Event Sourcing**: Modelo baseado em eventos
- **Incremental Processing**: Processamento D-1
- **Partitioning**: Particionamento por data

## 📚 Referências

- [SCD Type 2 Pattern](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Time Travel in Data Warehousing](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)

---

**Desenvolvido para o Desafio Técnico - Analytics Engineer - Hotmart**
