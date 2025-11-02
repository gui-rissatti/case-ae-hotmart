# Sumário - Desafio Técnico Analytics Engineer | Hotmart

## Índice Geral

1. [Visão Geral](#visão-geral)
2. [Exercício 1: Consultas SQL](#exercício-1-consultas-sql)
3. [Exercício 2: Pipeline ETL e GMV](#exercício-2-pipeline-etl-e-gmv)
4. [Requisitos Funcionais](#requisitos-funcionais)
5. [Melhorias e Trade-offs](#melhorias-e-trade-offs)

---

## Estrutura do Repositório

### Diretório `ex_1/` - Consultas SQL

| Arquivo | Descrição | Link |
|---------|-----------|------|
| `query_1.sql` | Top 50 produtores por faturamento em 2021 | [Acessar](./ex_1/query_1.sql) |
| `query_2.sql` | Top 2 produtos que mais faturaram por produtor | [Acessar](./ex_1/query_2.sql) |

### Diretório `ex_2/` - Pipeline ETL

| Arquivo | Descrição | Link |
|---------|-----------|------|
| `script_etl.py` | Pipeline PySpark/Glue para consolidação diária de compras | [Acessar](./ex_2/script_etl.py) |
| `query.sql` | Consulta GMV diário por subsidiária | [Acessar](./ex_2/query.sql) |
| `output_exemplo.csv` | Exemplo de saída consolidada (snapshot diário) | [Acessar](./ex_2/output_exemplo.csv) |