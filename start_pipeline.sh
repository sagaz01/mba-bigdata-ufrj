#!/bin/bash

echo "======================================================="
echo "🚀 INICIANDO PIPELINE DE BIG DATA - SÍSMICA OFFSHORE 🚀"
echo "======================================================="

# Passo 1: Iniciar o Gerador de Dados em segundo plano
echo "[1/3] Iniciando Simulador IoT (iot_seismic_generator.py)..."
python3 iot_seismic_generator.py > generator.log 2>&1 &
PID_GEN=$!

# Passo 2: Iniciar o Processamento Spark em segundo plano
echo "[2/3] Iniciando Motor Apache Spark (spark_seismic_etl.py)..."
python3 spark_seismic_etl.py > spark.log 2>&1 &
PID_SPARK=$!

# Esperar 5 segundos para o Spark aquecer antes de subir a API
sleep 5

# Passo 3: Iniciar o Servidor Flask em segundo plano
echo "[3/3] Iniciando Servidor Web e API (server_api.py)..."
python3 server_api.py > api.log 2>&1 &
PID_API=$!

echo "======================================================="
echo "✅ TODOS OS SERVIÇOS ESTÃO RODANDO!"
echo "🌐 Acesse o Dashboard no seu navegador na porta 5000"
echo "======================================================="
echo "⚠️  Pressione [Ctrl+C] a qualquer momento para PARAR TODOS OS SERVIÇOS."

# Função de armadilha (trap) para desligar tudo corretamente quando você apertar Ctrl+C
cleanup() {
    echo ""
    echo "🛑 Desligando os serviços..."
    kill $PID_GEN $PID_SPARK $PID_API 2>/dev/null
    echo "✅ Pipeline encerrado com segurança."
    exit 0
}

# Fica escutando o comando de interrupção (Ctrl+C)
trap cleanup SIGINT SIGTERM

# Mantém o script bash rodando para segurar os processos vivos
wait
