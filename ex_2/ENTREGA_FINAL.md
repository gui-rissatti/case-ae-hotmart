# ✅ Exercício 2 - COMPLETO

## 📋 Checklist de Requisitos

| # | Requisito | Status | Implementação |
|---|-----------|--------|---------------|
| 1 | Modelagem histórica com rastreabilidade | ✅ | SCD Type 2 (effective_date, end_date, is_current) |
| 2 | Processamento D-1 | ✅ | Incremental por transaction_date |
| 3 | Tratamento assíncrono de 3 tabelas | ✅ | Full outer join |
| 4 | Forward fill (repetir dados não atualizados) | ✅ | Coalesce com valores anteriores |
| 5 | Idempotência (reprocessável) | ✅ | DELETE + INSERT por partição |
| 6 | Time travel (navegação temporal) | ✅ | Query com as_of_date |
| 7 | Facilidade para dados correntes | ✅ | Flag is_current |
| 8 | GMV diário por subsidiária | ✅ | Query com agrupamento |
| 9 | Particionamento | ✅ | Por transaction_date |
| 10 | Não alterar o passado | ✅ | SCD Type 2 preserva histórico |
| 11 | **Apenas 1 script Python** | ✅ | etl_purchase_history.py |

## 📁 Arquivos Entregues

### Arquivo Principal (SOLUÇÃO)
- **`etl_purchase_history.py`** - Script ETL completo (600 linhas)

### Documentação
- **`README_SOLUTION.md`** - Documentação detalhada da solução
- **`REFATORACAO.md`** - Explicação das mudanças vs versão anterior
- **`README.md`** - Contexto do exercício (atualizado)

### Queries SQL
- **`queries/gmv_time_travel.sql`** - Exemplos de consulta GMV com time travel

### Scripts de Teste
- **`run_tests.ps1`** - Script PowerShell para executar suite de testes

### Dados de Exemplo
- **`data/input/sample_data_explained.txt`** - Dados de exemplo com explicações

### Arquivos Legados (Referência)
- `src/etl_main.py` - Versão complexa anterior (incompleta)
- `src/data_quality.py` - Módulo de validações (referência)
- `src/utils.py` - Utilitários (referência)

## 🚀 Quick Start

### 1. Instalar Dependências
```bash
pip install pyspark
```

### 2. Executar Testes Completos
```powershell
# PowerShell
.\run_tests.ps1
```

Ou manualmente:
```bash
# Criar dados de exemplo e processar
python etl_purchase_history.py --create-sample-data --process-date 2023-01-20

# Processar outros dias
python etl_purchase_history.py --process-date 2023-01-21
python etl_purchase_history.py --process-date 2023-01-23

# Consultar GMV
python etl_purchase_history.py --query-gmv

# Time travel
python etl_purchase_history.py --query-gmv --as-of-date 2023-01-31
```

### 3. Verificar Resultados
- Tabela `fact_purchase_history` será criada com histórico completo
- Consultas GMV mostram valores por subsidiária
- Time travel permite navegar no tempo

## 🎯 Decisões de Design

### 1. Um Único Script
**Por quê?** Requisito explícito do desafio + simplicidade

**Trade-offs:**
- ✅ Fácil de revisar e executar
- ✅ Sem dependências entre arquivos
- ⚠️ Arquivo grande (~600 linhas)
- ⚠️ Menos modular

**Decisão:** Simplicidade > Modularidade neste contexto

### 2. SCD Type 2 vs SCD Type 3
**Por quê SCD Type 2?**
- Rastreabilidade completa (requisito)
- Time travel (requisito)
- Auditabilidade (compliance/financeiro)

**Trade-off:**
- ✅ Histórico completo
- ⚠️ Mais storage (múltiplas versões)

**Decisão:** SCD Type 2 atende melhor os requisitos

### 3. Full Outer Join vs Left Join
**Por quê Full Outer Join?**
- Assincronicidade: dados chegam fora de ordem
- Exemplo: product_item pode chegar ANTES de purchase

**Trade-off:**
- ✅ Não perde nenhum evento
- ⚠️ Mais complexo

**Decisão:** Full Outer Join é necessário

### 4. Forward Fill vs NULL
**Por quê Forward Fill?**
- Requisito explícito no vídeo
- Facilita análise (todos os campos preenchidos)

**Trade-off:**
- ✅ Integridade dos dados
- ⚠️ Mais complexidade

**Decisão:** Forward Fill conforme requisito

### 5. DELETE + INSERT vs MERGE
**Por quê DELETE + INSERT?**
- Idempotência forte
- Simplicidade de implementação

**Trade-off:**
- ✅ Mais fácil de entender
- ✅ Idempotência garantida
- ⚠️ Menos performático

**Decisão:** Simplicidade > Performance (neste caso)

## 🔍 Validações Implementadas

### Processamento
- ✅ Verifica eventos vazios
- ✅ Detecta mudanças reais (hash)
- ✅ Evita inserir linhas idênticas

### Time Travel
- ✅ Filtro temporal correto (effective_date <= as_of_date)
- ✅ Apenas registros válidos na data
- ✅ Evita duplicação (snapshot único)

### GMV
- ✅ Apenas compras pagas (release_date NOT NULL)
- ✅ Agrupa por order_date (data da compra)
- ✅ Suporta time travel

## 🧪 Cenários Testados

### 1. Chegada Síncrona
- ✅ Compra 57: purchase + product_item + extra_info no mesmo dia

### 2. Chegada Assíncrona (Atrasada)
- ✅ Compra 55: extra_info chega 3 dias depois
- ✅ Forward fill aplicado corretamente

### 3. Chegada Fora de Ordem
- ✅ Compra 56: product_item chega ANTES de purchase
- ✅ Full outer join trata corretamente

### 4. Múltiplas Alterações
- ✅ Compra 55: 5 versões diferentes
- ✅ SCD Type 2 registra todas as mudanças

### 5. Idempotência
- ✅ Reprocessar 2023-01-20 dá mesmo resultado
- ✅ GMV não muda após reprocessamento

### 6. Time Travel
- ✅ GMV em 31/01 vs 31/07 reflete mudanças
- ✅ Fechamentos contábeis preservados

## 📊 Exemplo de Resultado

### Compra 55 - Evolução Temporal

```
effective_date | buyer_id | item_value | subsidiary | is_current
---------------|----------|------------|------------|------------
2023-01-20     | 100      | 600.00     | NULL       | FALSE
2023-01-23     | 100      | 600.00     | NATIONAL   | FALSE  ← subsidiary chegou
2023-02-05     | 200      | 600.00     | NATIONAL   | FALSE  ← buyer_id mudou
2023-07-12     | 200      | 550.00     | NATIONAL   | FALSE  ← item_value mudou
2023-07-15     | 200      | 550.00     | NATIONAL   | TRUE   ← release_date (CORRENTE)
```

### GMV Time Travel

```sql
-- Em 31/01/2023 (antes das mudanças)
SELECT SUM(purchase_value) WHERE ... AND as_of_date = '2023-01-31'
-- R$ 1000,00

-- Em 31/07/2023 (após mudanças)
SELECT SUM(purchase_value) WHERE ... AND as_of_date = '2023-07-31'
-- R$ 1000,00 (purchase_value não mudou)
```

## 💡 Melhorias Futuras

### Prioridade Alta
1. **Validações de Data Quality**
   - Valores negativos
   - Datas inválidas (release < order)
   - Duplicatas

2. **Testes Automatizados**
   - Testes unitários
   - Testes de integração
   - Testes de idempotência

3. **Observabilidade**
   - Métricas detalhadas
   - Alertas
   - Dashboard

### Prioridade Média
4. **Otimizações**
   - Broadcast joins
   - Cache
   - Compactação

5. **Late Arriving Data**
   - Dados com atraso > D-1
   - Backfill

### Prioridade Baixa
6. **CI/CD**
   - Pipeline automatizado
   - Deploy multi-ambiente

## 📚 Conceitos Demonstrados

- ✅ **SCD Type 2** - Slowly Changing Dimensions
- ✅ **Time Travel** - Point-in-time queries
- ✅ **Forward Fill** - Repetição de valores
- ✅ **Idempotência** - Reprocessamento seguro
- ✅ **Full Outer Join** - Merge assíncrono
- ✅ **Event Sourcing** - Modelo de eventos
- ✅ **Incremental Processing** - D-1
- ✅ **Partitioning** - Por transaction_date

## ✅ Entregável Final

**Arquivo principal:** `etl_purchase_history.py`

**Como avaliar:**
1. Ler `README_SOLUTION.md` (documentação completa)
2. Executar `run_tests.ps1` (suite de testes)
3. Revisar código em `etl_purchase_history.py`
4. Consultar `queries/gmv_time_travel.sql` (exemplos SQL)

---

**Desenvolvido para o Desafio Técnico - Analytics Engineer - Hotmart**

**Data:** Novembro 2025

**Status:** ✅ COMPLETO E FUNCIONAL
