# Contexto de Negócio - Hotmart

## 📌 Visão Geral

A Hotmart é uma plataforma de distribuição de produtos digitais que conecta **creators (produtores)** com **buyers (compradores)**. Este documento descreve o modelo de negócio e como ele se reflete nos dados.

---

## 🎯 Modelo de Negócio

### Atores Principais

```
┌─────────────┐                    ┌─────────────┐
│   CREATOR   │                    │    BUYER    │
│ (Produtor)  │                    │ (Comprador) │
└──────┬──────┘                    └──────┬──────┘
       │                                  │
       │ 1. Disponibiliza produto         │
       │    (curso, ebook, etc.)          │
       │                                  │
       ▼                                  │
┌─────────────────────────────────────┐  │
│      PLATAFORMA HOTMART             │  │
│  ┌─────────────────────────────┐   │  │
│  │   Curso de Python - R$199   │   │  │
│  │   Ebook de Marketing - R$50 │   │  │
│  │   ...                       │   │  │
│  └─────────────────────────────┘   │  │
└─────────────────────────────────────┘  │
                                         │ 2. Compra produto
                                         ▼
                             ┌───────────────────┐
                             │  TRANSAÇÃO        │
                             │  GMV gerado!      │
                             └───────────────────┘
```

### Fluxo de Uma Transação

```
1. Compra Efetuada (order_date)
   ↓
2. Pagamento Processado
   ↓
3. Pagamento Confirmado (release_date)
   ↓
4. GMV Reconhecido ✅
```

**Importante:** Apenas compras com `release_date IS NOT NULL` geram faturamento!

---

## 💰 Definições Financeiras

### GMV (Gross Merchandise Value)

**Definição:**  
Valor bruto total transacionado na plataforma, **antes** de descontar taxas, impostos ou comissões.

**Cálculo:**
```sql
GMV = SUM(purchase_value) 
WHERE release_date IS NOT NULL  -- Apenas compras pagas
```

**Exemplo:**
- Compra de R$ 199,00
- Hotmart cobra 10% de taxa → R$ 19,90
- Produtor recebe → R$ 179,10
- **GMV = R$ 199,00** (valor bruto, antes de descontos)

### Faturamento vs Revenue

| Métrica | Definição | Exemplo |
|---------|-----------|---------|
| **GMV (Faturamento)** | Valor bruto total | R$ 199,00 |
| **Revenue (Receita Hotmart)** | Taxas cobradas | R$ 19,90 (10%) |
| **Net Revenue (Produtor)** | Após taxas e impostos | R$ 179,10 |

**No teste, usamos GMV = Faturamento.**

---

## 🌍 Subsidiárias

### NATIONAL vs INTERNATIONAL

```
┌──────────────────────────────────────────────────┐
│  NATIONAL                                        │
│  - Vendas dentro do Brasil                      │
│  - Pagamento em BRL                              │
│  - Impostos brasileiros                          │
└──────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────┐
│  INTERNATIONAL                                   │
│  - Vendas fora do Brasil                         │
│  - Pagamento em USD, EUR, etc.                   │
│  - Regulamentação internacional                  │
└──────────────────────────────────────────────────┘
```

**Importância:**
- Diferentes regras fiscais
- Diferentes taxas de conversão
- Segregação para relatórios financeiros

---

## 📊 Jornada do Dado

### Do Pedido à Confirmação

```
Day 0: Cliente faz pedido
  ↓
  purchase: order_date = 2023-01-20
           release_date = NULL (aguardando pagamento)
           
  ❌ NÃO CONTA COMO GMV AINDA

Day 1: Boleto pago / Cartão aprovado
  ↓
  purchase: order_date = 2023-01-20
           release_date = 2023-01-21 ✅
           
  ✅ AGORA CONTA COMO GMV!
```

### Ciclo de Vida de Uma Compra

```
┌─────────────────────────────────────────────────────┐
│ Status            │ release_date │ Conta no GMV?   │
├───────────────────┼──────────────┼─────────────────┤
│ Pedido criado     │ NULL         │ ❌ Não          │
│ Aguardando pgto   │ NULL         │ ❌ Não          │
│ Boleto emitido    │ NULL         │ ❌ Não          │
│ Pagamento aprovado│ 2023-01-21   │ ✅ Sim          │
│ Cancelado         │ NULL         │ ❌ Não          │
│ Estornado         │ *removida    │ ❌ Não          │
└───────────────────┴──────────────┴─────────────────┘
```

---

## 🔄 Modelo de Dados: Corrente vs Eventos

### Exercício 1: Tabelas Correntes

```sql
-- Snapshot do ÚLTIMO STATUS de cada compra
CREATE TABLE purchase (
    purchase_id BIGINT PRIMARY KEY,
    buyer_id BIGINT,
    order_date DATE,
    release_date DATE,
    purchase_value DECIMAL(10,2),
    -- ...
);

-- Cada purchase_id aparece UMA VEZ
-- Se compra muda, registro é ATUALIZADO
```

**Exemplo:**
```
Day 1: INSERT INTO purchase VALUES (55, 100, '2023-01-20', NULL, 1000);
Day 2: UPDATE purchase SET release_date = '2023-01-21' WHERE purchase_id = 55;
       
Resultado: Apenas 1 linha com dados atuais
```

### Exercício 2: Tabelas de Eventos

```sql
-- Histórico de TODAS AS MUDANÇAS
CREATE TABLE purchase_events (
    purchase_id BIGINT,  -- Não é PK!
    transaction_date DATE,
    buyer_id BIGINT,
    order_date DATE,
    release_date DATE,
    purchase_value DECIMAL(10,2),
    -- ...
);

-- Cada purchase_id pode aparecer MÚLTIPLAS VEZES
-- Cada mudança é um NOVO REGISTRO
```

**Exemplo:**
```
Day 1: INSERT INTO purchase_events VALUES (55, '2023-01-20', 100, ..., NULL, 1000);
Day 2: INSERT INTO purchase_events VALUES (55, '2023-01-21', 100, ..., '2023-01-21', 1000);
Day 5: INSERT INTO purchase_events VALUES (55, '2023-01-24', 200, ..., '2023-01-21', 1000);
       
Resultado: 3 linhas (histórico completo de mudanças)
```

---

## 🎯 Métricas de Negócio Chave

### 1. GMV por Produtor

**Pergunta:** Quem são os produtores que mais faturam?

**Impacto:**
- Identificar top performers
- Alocar recursos de suporte
- Oferecer benefícios especiais

**Query:**
```sql
SELECT producer_id, SUM(purchase_value) AS gmv
FROM purchase
WHERE release_date IS NOT NULL
GROUP BY producer_id
ORDER BY gmv DESC;
```

### 2. Produtos Mais Vendidos por Produtor

**Pergunta:** Quais produtos de cada produtor vendem mais?

**Impacto:**
- Insights para criação de novos produtos
- Otimizar mix de produtos
- Estratégias de marketing

### 3. GMV Diário por Subsidiária

**Pergunta:** Qual a evolução diária de vendas nacional vs internacional?

**Impacto:**
- Planejamento financeiro
- Previsão de receita
- Análise de sazonalidade

---

## 📈 Casos de Uso Reais

### Caso 1: Fechamento Mensal

**Cenário:**  
Dia 31/01/2023 - Time financeiro precisa fechar o mês.

**Requisito:**  
GMV de Janeiro/2023 deve ser **exatamente X reais**.

**Desafio:**  
Se reprocessarmos dados em 15/02, o GMV de Janeiro **deve continuar X reais**, mesmo se houver correções/estornos posteriores.

**Solução:**  
Time travel permite ver dados "como estavam" em 31/01.

### Caso 2: Auditoria Fiscal

**Cenário:**  
Receita Federal audita transações de 2021.

**Requisito:**  
Provar que valores declarados estão corretos.

**Desafio:**  
Dados podem ter sido corrigidos desde então.

**Solução:**  
Histórico completo (SCD Type 2) permite reconstruir estado de qualquer data.

### Caso 3: Análise de Churn de Produtores

**Cenário:**  
Produtor que vendia R$ 100k/mês caiu para R$ 10k/mês.

**Requisito:**  
Investigar quando e por que isso aconteceu.

**Desafio:**  
Tabela corrente só mostra estado atual.

**Solução:**  
Tabela histórica mostra evolução temporal de vendas.

---

## 🚨 Armadilhas Comuns

### Armadilha 1: Contar Compras Não Pagas

```sql
-- ❌ ERRADO
SELECT SUM(purchase_value) FROM purchase;

-- ✅ CORRETO
SELECT SUM(purchase_value) 
FROM purchase 
WHERE release_date IS NOT NULL;
```

### Armadilha 2: Duplicar GMV em Tabelas Históricas

```sql
-- ❌ ERRADO (em tabela SCD Type 2)
SELECT SUM(purchase_value) FROM fact_purchase_history;
-- Resultado: R$ 5 milhões (ERRADO! Triplicou porque mesma compra aparece 3x)

-- ✅ CORRETO
SELECT SUM(purchase_value)
FROM fact_purchase_history
WHERE is_current = TRUE;
-- Resultado: R$ 1,5 milhões (correto!)
```

### Armadilha 3: Ignorar Assincronismo

```sql
-- ❌ ASSUMIR que todas as tabelas atualizam juntas
SELECT p.*, pi.product_id, pei.subsidiary
FROM purchase p
INNER JOIN product_item pi ON ...
INNER JOIN purchase_extra_info pei ON ...;
-- Problema: Registros sumem se uma tabela não atualizou!

-- ✅ CORRETO
SELECT p.*, pi.product_id, pei.subsidiary
FROM purchase p
LEFT JOIN product_item pi ON ...
LEFT JOIN purchase_extra_info pei ON ...;
-- Ou melhor: FULL OUTER JOIN + Forward Fill
```

---

## 💡 Glossário de Termos

| Termo | Definição | Exemplo |
|-------|-----------|---------|
| **Creator / Produtor** | Quem cria e vende produtos na plataforma | Professor que vende curso online |
| **Buyer / Comprador** | Quem compra produtos | Aluno que compra o curso |
| **GMV** | Gross Merchandise Value (valor bruto) | R$ 199,00 |
| **order_date** | Data em que pedido foi criado | 2023-01-20 |
| **release_date** | Data em que pagamento foi confirmado | 2023-01-21 |
| **transaction_date** | Data em que evento foi salvo no banco | 2023-01-20 10:30:45 |
| **Subsidiária** | Nacional ou Internacional | NATIONAL |
| **SCD Type 2** | Slowly Changing Dimension - mantém histórico | Ver ADR-001 |
| **Forward Fill** | Repetir valores anteriores quando não há atualização | Ver ADR-003 |
| **Idempotência** | Processar N vezes = mesmo resultado | Ver ADR-002 |
| **Time Travel** | Consultar dados como estavam no passado | Ver ADR-001 |

---

## 📚 Referências

- [Material de Apoio - DER Fornecido](link-para-diagrama)
- [Transcrição do Vídeo Explicativo](../README.md)
- [ADRs - Decisões Arquiteturais](./02_architectural_decisions.md)

---

**Última Atualização:** Novembro 2025
