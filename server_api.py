from flask import Flask, render_template, jsonify
import pandas as pd
from pathlib import Path
import random

app = Flask(__name__)

def fetch_all_parquets(directory_str):
    """Lê TODOS os arquivos parquet gerados, ignorando os vazios e metadados"""
    dir_path = Path(directory_str)
    if not dir_path.exists():
        return None
        
    # Procura estritamente por arquivos com extensão .parquet
    parquet_files = list(dir_path.glob("*.parquet"))
    
    # Filtra arquivos que possam estar vazios (0 bytes) sendo escritos agora
    valid_files = [f for f in parquet_files if f.stat().st_size > 0]
    
    if not valid_files:
        return None
        
    try:
        # Lê todos os arquivos válidos e junta em um único DataFrame
        df_list = [pd.read_parquet(f) for f in valid_files]
        return pd.concat(df_list, ignore_index=True)
    except Exception as e:
        print(f"Aviso ao ler parquets: {e}")
        return None

def evaluate_subsurface(amplitude, snr):
    """Regra de negócio para classificar o local"""
    if snr < 15.0:
        return {"label": "INTERFERÊNCIA", "hex_color": "#dc3545", "desc": "Ruído excessivo na fibra"}
    elif amplitude >= 70.0 and snr >= 20.0:
        return {"label": "ALTA PROBABILIDADE", "hex_color": "#28a745", "desc": "Assinatura de hidrocarboneto"}
    else:
        return {"label": "MAPEANDO", "hex_color": "#ffc107", "desc": "Coletando mais dados..."}

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/api/v1/seismic/current')
def get_current_readings():
    try:
        # Agora busca TODOS os dados da camada Silver
        df = fetch_all_parquets('datalake/silver_processed')
        if df is None or df.empty:
            return jsonify({"status": "waiting_data", "payload": []})
            
        # Ordena do mais recente para o mais antigo e pega os 100 últimos registros
        # (Limitamos a 100 para o navegador não travar renderizando milhares de linhas de HTML)
        df = df.sort_values('etl_timestamp', ascending=False).head(100)
        
        records = df.to_dict(orient='records')
        
        for item in records:
            # Atributos de apresentação
            item['confidence_score'] = round(random.uniform(80.0, 99.9), 1)
            item['analysis'] = evaluate_subsurface(item.get('mean_amplitude', 0), item.get('mean_snr', 0))
            
        return jsonify({"status": "success", "payload": records})
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/v1/seismic/discoveries')
def get_high_potential_sites():
    try:
        # Busca todas as anomalias da camada Gold
        df = fetch_all_parquets('datalake/gold_anomalies')
        if df is None or df.empty:
            return jsonify({"status": "success", "payload": []})
            
        records = df.to_dict(orient='records')
        return jsonify({"status": "success", "payload": records})
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route('/api/v1/system/health')
def system_health():
    """Endpoint simplificado para verificar se a API e os diretórios estão online"""
    silver_exists = Path('datalake/silver_processed').exists()
    return jsonify({
        "api_status": "ONLINE",
        "datalake_connected": silver_exists,
        "project": "MBA Engenharia de Software UFRJ"
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)