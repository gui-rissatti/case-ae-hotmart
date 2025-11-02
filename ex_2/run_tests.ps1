# Script de Testes - ETL Purchase History
# Executa sequência completa de testes com dados de exemplo

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  ETL PURCHASE HISTORY - SUITE DE TESTES" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# Verificar se Python está disponível
try {
    $pythonVersion = python --version
    Write-Host "✅ Python encontrado: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Python não encontrado. Instale Python 3.x" -ForegroundColor Red
    exit 1
}

# Verificar se PySpark está instalado
Write-Host ""
Write-Host "Verificando dependências..." -ForegroundColor Yellow
python -c "import pyspark" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ PySpark não encontrado" -ForegroundColor Red
    Write-Host "   Instalando PySpark..." -ForegroundColor Yellow
    pip install pyspark
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Falha ao instalar PySpark" -ForegroundColor Red
        exit 1
    }
}
Write-Host "✅ PySpark instalado" -ForegroundColor Green

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  TESTE 1: PROCESSAR DIAS SEQUENCIALMENTE" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# Dia 1: 2023-01-20 (compras 55 e 57 chegam)
Write-Host "📅 Processando 2023-01-20..." -ForegroundColor Yellow
python etl_purchase_history.py --create-sample-data --process-date 2023-01-20
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao processar 2023-01-20" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 2023-01-20 processado" -ForegroundColor Green
Write-Host ""

# Dia 2: 2023-01-21 (compra 58 chega)
Write-Host "📅 Processando 2023-01-21..." -ForegroundColor Yellow
python etl_purchase_history.py --process-date 2023-01-21
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao processar 2023-01-21" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 2023-01-21 processado" -ForegroundColor Green
Write-Host ""

# Dia 3: 2023-01-23 (subsidiária da compra 55 chega - FORWARD FILL!)
Write-Host "📅 Processando 2023-01-23 (subsidiária chega - teste de forward fill)..." -ForegroundColor Yellow
python etl_purchase_history.py --process-date 2023-01-23
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao processar 2023-01-23" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 2023-01-23 processado" -ForegroundColor Green
Write-Host ""

# Dia 4: 2023-01-25 (product_item da compra 56 chega ANTES da purchase!)
Write-Host "📅 Processando 2023-01-25 (product_item chega antes - teste assíncrono)..." -ForegroundColor Yellow
python etl_purchase_history.py --process-date 2023-01-25
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao processar 2023-01-25" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 2023-01-25 processado" -ForegroundColor Green
Write-Host ""

# Dia 5: 2023-01-26 (purchase 56 chega DEPOIS do product_item)
Write-Host "📅 Processando 2023-01-26 (purchase chega depois)..." -ForegroundColor Yellow
python etl_purchase_history.py --process-date 2023-01-26
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao processar 2023-01-26" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 2023-01-26 processado" -ForegroundColor Green
Write-Host ""

# Dia 6: 2023-02-05 (buyer_id da compra 55 muda)
Write-Host "📅 Processando 2023-02-05 (buyer_id muda - teste de mudança)..." -ForegroundColor Yellow
python etl_purchase_history.py --process-date 2023-02-05
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao processar 2023-02-05" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 2023-02-05 processado" -ForegroundColor Green
Write-Host ""

# Dia 7: 2023-07-12 (item_value da compra 55 muda)
Write-Host "📅 Processando 2023-07-12 (item_value muda)..." -ForegroundColor Yellow
python etl_purchase_history.py --process-date 2023-07-12
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao processar 2023-07-12" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 2023-07-12 processado" -ForegroundColor Green
Write-Host ""

# Dia 8: 2023-07-15 (release_date da compra 55 atualizada)
Write-Host "📅 Processando 2023-07-15 (release_date atualizada)..." -ForegroundColor Yellow
python etl_purchase_history.py --process-date 2023-07-15
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao processar 2023-07-15" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 2023-07-15 processado" -ForegroundColor Green
Write-Host ""

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  TESTE 2: CONSULTAR GMV CORRENTE" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

python etl_purchase_history.py --query-gmv
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao consultar GMV" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  TESTE 3: TIME TRAVEL - GMV EM 31/01/2023" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

python etl_purchase_history.py --query-gmv --as-of-date 2023-01-31
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao consultar GMV com time travel" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  TESTE 4: TIME TRAVEL - GMV EM 31/07/2023" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

python etl_purchase_history.py --query-gmv --as-of-date 2023-07-31
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao consultar GMV com time travel" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  TESTE 5: IDEMPOTÊNCIA - REPROCESSAR 2023-01-20" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Reprocessando 2023-01-20 (deve dar mesmo resultado)..." -ForegroundColor Yellow
python etl_purchase_history.py --process-date 2023-01-20
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Falha ao reprocessar 2023-01-20" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Reprocessamento concluído" -ForegroundColor Green

Write-Host ""
Write-Host "Consultando GMV novamente (deve ser idêntico)..." -ForegroundColor Yellow
python etl_purchase_history.py --query-gmv --as-of-date 2023-01-31

Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host "  ✅ TODOS OS TESTES CONCLUÍDOS COM SUCESSO!" -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
Write-Host ""

Write-Host "Próximos passos:" -ForegroundColor Cyan
Write-Host "  1. Revisar logs de processamento acima" -ForegroundColor White
Write-Host "  2. Verificar que GMV é consistente após reprocessamento" -ForegroundColor White
Write-Host "  3. Analisar diferenças entre time travel em 31/01 vs 31/07" -ForegroundColor White
Write-Host ""
