### Exercício 1: SQL Queries

| Item | Status | Localização |
|------|--------|-------------|
| Query 1: Top 50 Produtores 2021 | ✅ | [`exercise_1_sql/query_1_top_50_producers.sql`](../exercise_1_sql/query_1_top_50_producers.sql) |
| Query 2: Top 2 Produtos/Produtor | ✅ | [`exercise_1_sql/query_2_top_2_products_per_producer.sql`](../exercise_1_sql/query_2_top_2_products_per_producer.sql) |
| Documentação das decisões | ✅ | [`exercise_1_sql/README.md`](../exercise_1_sql/README.md) |
| Comentários detalhados | ✅ | Dentro de cada query |
---

### Exercício 2: ETL PySpark ⚡ **REFATORADO**

| Item | Status | Localização |
|------|--------|-------------|
| **🎯 Script ETL Principal (ÚNICO)** | ✅ | [`ex_2/etl_purchase_history.py`](ex_2/etl_purchase_history.py) |
| **📖 Documentação Completa** | ✅ | [`ex_2/README_SOLUTION.md`](ex_2/README_SOLUTION.md) |
| **📝 Explicação da Refatoração** | ✅ | [`ex_2/REFATORACAO.md`](ex_2/REFATORACAO.md) |
| **📋 Resumo da Entrega** | ✅ | [`ex_2/ENTREGA_FINAL.md`](ex_2/ENTREGA_FINAL.md) |
| Query GMV com Time Travel | ✅ | [`ex_2/queries/gmv_time_travel.sql`](ex_2/queries/gmv_time_travel.sql) |
| Script de Testes | ✅ | [`ex_2/run_tests.ps1`](ex_2/run_tests.ps1) |
| Dados de exemplo | ✅ | [`ex_2/data/input/`](ex_2/data/input/) |
| Arquivos legados (referência) | 📚 | [`ex_2/src/`](ex_2/src/) |

#### ⚡ Mudanças Principais
- **Antes:** 4 arquivos Python complexos e incompletos
- **Depois:** 1 único script Python completo e funcional (~600 linhas)
- **Motivo:** Requisito "apenas UM script" + simplicidade + completude