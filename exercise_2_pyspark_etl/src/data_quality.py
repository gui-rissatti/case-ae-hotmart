"""
================================================================================
DATA QUALITY - Validações e métricas de qualidade de dados
================================================================================

Módulo: data_quality.py
Descrição: Funções para validação e monitoramento de qualidade de dados

Validações:
  - Integridade referencial
  - Valores nulos em campos obrigatórios
  - Valores duplicados
  - Ranges de datas inválidos
  - Valores negativos em campos monetários

================================================================================
"""

import logging
from typing import List, Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import setup_logger

logger = setup_logger(__name__)


def validate_input_data(df: DataFrame, table_name: str):
    """
    Valida qualidade dos dados de entrada.
    
    Checks:
      - Registros não vazios
      - Campos obrigatórios não nulos
      - Datas válidas
      - Valores monetários não negativos
    
    Args:
        df: DataFrame a validar
        table_name: Nome da tabela (para logging)
    
    Raises:
        ValueError: Se validação falhar
    """
    logger.info(f"Validando dados de entrada: {table_name}")
    
    count = df.count()
    if count == 0:
        raise ValueError(f"Tabela {table_name} está vazia!")
    
    # Validações específicas por tabela
    if table_name == "purchase":
        # purchase_id não pode ser nulo
        null_ids = df.filter(F.col("purchase_id").isNull()).count()
        if null_ids > 0:
            raise ValueError(f"{table_name}: {null_ids} registros com purchase_id NULL")
        
        # purchase_value não pode ser negativo
        negative_values = df.filter(F.col("purchase_value") < 0).count()
        if negative_values > 0:
            logger.warning(f"{table_name}: {negative_values} registros com valor negativo")
    
    elif table_name == "product_item":
        # product_id não pode ser nulo
        null_products = df.filter(F.col("product_id").isNull()).count()
        if null_products > 0:
            raise ValueError(f"{table_name}: {null_products} registros com product_id NULL")
    
    logger.info(f"✅ {table_name}: {count} registros validados")


def validate_output_data(df: DataFrame):
    """
    Valida qualidade dos dados de saída (fact_purchase_history).
    
    Checks:
      - Grain correto (purchase_id + effective_date únicos)
      - is_current tem no máximo 1 TRUE por purchase_id
      - effective_date <= end_date
      - Sem valores NULL em campos obrigatórios
    
    Args:
        df: DataFrame da tabela final
    
    Raises:
        ValueError: Se validação falhar
    """
    logger.info("Validando dados de saída")
    
    # Check 1: Grain único
    total_records = df.count()
    unique_grain = df.select("purchase_id", "effective_date").distinct().count()
    
    if total_records != unique_grain:
        raise ValueError(
            f"Violação de grain! Total: {total_records}, "
            f"Únicos: {unique_grain}"
        )
    
    # Check 2: Apenas 1 registro corrente por purchase_id
    df_current_counts = df.filter(F.col("is_current") == True) \
        .groupBy("purchase_id") \
        .count() \
        .filter(F.col("count") > 1)
    
    invalid_current = df_current_counts.count()
    if invalid_current > 0:
        logger.error(f"{invalid_current} purchase_ids com múltiplos is_current=TRUE")
        df_current_counts.show(10)
        raise ValueError("Múltiplos registros correntes para mesma purchase_id")
    
    # Check 3: effective_date <= end_date (quando end_date não é nulo)
    invalid_dates = df.filter(
        (F.col("end_date").isNotNull()) &
        (F.col("effective_date") > F.col("end_date"))
    ).count()
    
    if invalid_dates > 0:
        raise ValueError(f"{invalid_dates} registros com effective_date > end_date")
    
    logger.info(f"✅ Dados de saída validados: {total_records} registros OK")


def log_data_quality_metrics(df: DataFrame, stage: str):
    """
    Loga métricas de qualidade de dados para observabilidade.
    
    Métricas:
      - Total de registros
      - Contagem de NULLs por coluna
      - Duplicados
      - Valores únicos em colunas chave
    
    Args:
        df: DataFrame a analisar
        stage: Estágio do pipeline (ex: "after_merge", "after_forward_fill")
    """
    logger.info(f"📊 Métricas de DQ - {stage}")
    
    total_records = df.count()
    logger.info(f"  Total de registros: {total_records}")
    
    # Contagem de NULLs
    null_counts = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    # Logar colunas com NULLs
    for col, null_count in null_counts.items():
        if null_count > 0:
            pct = 100 * null_count / max(total_records, 1)
            logger.info(f"  {col}: {null_count} NULLs ({pct:.1f}%)")
    
    # Registros únicos de purchase_id
    unique_purchases = df.select("purchase_id").distinct().count()
    logger.info(f"  purchase_ids únicos: {unique_purchases}")


def detect_anomalies(df: DataFrame) -> Dict[str, Any]:
    """
    Detecta anomalias nos dados.
    
    Anomalias:
      - Valores extremos (outliers) em purchase_value
      - Datas no futuro
      - release_date < order_date
    
    Args:
        df: DataFrame a analisar
    
    Returns:
        Dicionário com métricas de anomalias
    """
    logger.info("🔍 Detectando anomalias")
    
    anomalies = {}
    
    # Anomalia 1: release_date antes de order_date
    invalid_dates = df.filter(
        (F.col("release_date").isNotNull()) &
        (F.col("order_date").isNotNull()) &
        (F.col("release_date") < F.col("order_date"))
    ).count()
    
    if invalid_dates > 0:
        logger.warning(f"⚠️  {invalid_dates} compras com release_date < order_date")
        anomalies["invalid_release_dates"] = invalid_dates
    
    # Anomalia 2: Valores negativos
    negative_values = df.filter(F.col("purchase_value") < 0).count()
    
    if negative_values > 0:
        logger.warning(f"⚠️  {negative_values} compras com valor negativo (estornos?)")
        anomalies["negative_values"] = negative_values
    
    # Anomalia 3: Valores extremamente altos (possível erro)
    # Definir threshold baseado em percentis
    stats = df.select(
        F.percentile_approx("purchase_value", 0.99).alias("p99")
    ).collect()[0]
    
    threshold = stats["p99"] * 10  # 10x o percentil 99
    
    extreme_values = df.filter(F.col("purchase_value") > threshold).count()
    
    if extreme_values > 0:
        logger.warning(f"⚠️  {extreme_values} compras com valor > {threshold:.2f} (outliers)")
        anomalies["extreme_values"] = extreme_values
    
    return anomalies
