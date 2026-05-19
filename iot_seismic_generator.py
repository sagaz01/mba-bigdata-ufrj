import pandas as pd
import random
import time
from pathlib import Path
from datetime import datetime

INGESTION_DIR = Path("datalake/raw_seismic")
INGESTION_DIR.mkdir(parents=True, exist_ok=True)

# Sítios de exploração de petróleo
DRILL_SITES = [
    {"node_id": "FIBER-01A", "site_name": "Bloco Pré-Sal Alpha", "region": "Bacia de Santos"},
    {"node_id": "FIBER-02B", "site_name": "Bloco Pré-Sal Beta", "region": "Bacia de Santos"},
    {"node_id": "FIBER-03C", "site_name": "Sítio Exploratório Gama", "region": "Bacia de Campos"},
    {"node_id": "FIBER-04D", "site_name": "Poço Profundo Delta", "region": "Bacia do Espírito Santo"}
]

def simulate_sensor_reading():
    """Gera o lote de leituras simulando a rede de fibra óptica"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payloads = []
    
    for site in DRILL_SITES:
        # Valores base de prospecção
        base_amplitude = random.uniform(20.0, 60.0)
        base_snr = random.uniform(10.0, 30.0)
        
        # 15% de chance de achar uma anomalia sísmica forte (potencial reservatório)
        if random.random() < 0.15:
            base_amplitude += random.uniform(30.0, 60.0) # Pode passar de 75 (Alerta de Óleo)
            base_snr += random.uniform(5.0, 15.0)
            
        payloads.append({
            "event_time": current_time,
            "sensor_node": site["node_id"],
            "target_site": site["site_name"],
            "basin_region": site["region"],
            "acoustic_amplitude_db": round(base_amplitude, 2),
            "signal_noise_ratio": round(base_snr, 2)
        })
        
    return pd.DataFrame(payloads)

if __name__ == "__main__":
    print(">>> Iniciando emissão de sinais sísmicos IoT (Pressione Ctrl+C para parar)...")
    try:
        while True:
            df_signals = simulate_sensor_reading()
            
            # Gera nome de arquivo único baseado no timestamp unix
            file_name = INGESTION_DIR / f"seismic_batch_{int(time.time())}.csv"
            df_signals.to_csv(file_name, index=False)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Lote gravado: {file_name.name}")
            time.sleep(3) # Acelerado para 3 segundos para gerar dados mais rápido
            
    except KeyboardInterrupt:
        print("\n>>> Simulação finalizada com sucesso.")