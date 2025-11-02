"""
================================================================================
UTILITÁRIOS - Funções auxiliares para o ETL
================================================================================

Módulo: utils.py
Descrição: Funções utilitárias reutilizáveis

Funções:
  - setup_logger: Configuração de logging estruturado
  - timer: Decorator para medir tempo de execução
  - calculate_md5_hash: Hash MD5 para detecção de mudanças
  - get_spark_session: Factory de SparkSession com configs otimizadas

================================================================================
"""

import hashlib
import logging
import time
from datetime import datetime
from functools import wraps
from typing import Callable, Any

from pyspark.sql import SparkSession


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configura logger estruturado com formato padronizado.
    
    Args:
        name: Nome do logger (geralmente __name__ do módulo)
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Evitar duplicação de handlers
    if not logger.handlers:
        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        
        # Formato: [2023-01-20 10:30:45] [INFO] [etl_main] Mensagem
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    
    return logger


def timer(func: Callable) -> Callable:
    """
    Decorator para medir e logar tempo de execução de funções.
    
    Uso:
        @timer
        def minha_funcao():
            # código
            pass
    
    Args:
        func: Função a ser decorada
    
    Returns:
        Função decorada que loga tempo de execução
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        start_time = time.time()
        
        logger.info(f"⏱️  Iniciando {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            logger.info(
                f"✅ {func.__name__} concluído em {duration:.2f}s"
            )
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f"❌ {func.__name__} falhou após {duration:.2f}s: {str(e)}"
            )
            raise
    
    return wrapper


def calculate_md5_hash(data: str) -> str:
    """
    Calcula hash MD5 de uma string.
    
    Uso:
        hash_value = calculate_md5_hash("buyer_id=100|product_id=200")
    
    Args:
        data: String para calcular hash
    
    Returns:
        Hash MD5 em hexadecimal (32 caracteres)
    """
    return hashlib.md5(data.encode('utf-8')).hexdigest()


def get_spark_session(app_name: str) -> SparkSession:
    """
    Factory de SparkSession com configurações otimizadas.
    
    Configurações aplicadas:
      - Adaptive Query Execution (AQE) habilitado
      - Broadcast join threshold otimizado
      - Shuffle partitions baseado em carga
      - Compressão Snappy para Parquet
    
    Args:
        app_name: Nome da aplicação Spark
    
    Returns:
        SparkSession configurada
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Configurar log level do Spark (reduzir verbosidade)
    spark.sparkContext.setLogLevel("WARN")
    
    logger = setup_logger(__name__)
    logger.info(f"SparkSession '{app_name}' criada com sucesso")
    logger.info(f"Spark version: {spark.version}")
    
    return spark


def format_number(num: int) -> str:
    """
    Formata número com separador de milhares.
    
    Exemplo:
        1000000 -> "1,000,000"
    
    Args:
        num: Número inteiro
    
    Returns:
        String formatada
    """
    return f"{num:,}"


def validate_date_format(date_str: str) -> bool:
    """
    Valida se string está no formato YYYY-MM-DD.
    
    Args:
        date_str: String de data
    
    Returns:
        True se válido, False caso contrário
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
