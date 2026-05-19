import pandas as pd
import time
import random
from pathlib import Path
from datetime import datetime

# Aponta para a mesma pasta que o Spark está monitorando
INGESTION_DIR = Path("datalake/raw_seismic")
INGESTION_DIR.mkdir(parents=True, exist_ok=True)

def inject_hydrocarbon_burst():
    """Gera uma rajada (burst) de múltiplas descobertas simultâneas"""
    print("\n" + "="*60)
    print("🧨 INICIANDO VARREDURA DE ALTA FREQUÊNCIA (BURST)...")
    print("="*60)
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payloads = []
    
    # Criamos 4 sondas de teste para popular a tabela de uma vez
    novas_sondas = [
        {"node": "SONDA-ALFA-99", "site": "Bolsão Pré-Sal (Descoberta 1)", "basin": "Bacia de Santos"},
        {"node": "SONDA-BETA-88", "site": "Reserva Profunda (Descoberta 2)", "basin": "Bacia de Campos"},
        {"node": "SONDA-GAMA-77", "site": "Anomalia Acústica Leste", "basin": "Bacia do Espírito Santo"},
        {"node": "SONDA-DELTA-66", "site": "Complexo Submarino Norte", "basin": "Bacia de Santos"}
    ]
    
    print(">>> Simulando disparos sísmicos de alta penetração no leito marinho...")
    
    for sonda in novas_sondas:
        # Sorteia valores sempre acima da linha de corte da Camada Gold (>70dB e >20 SNR)
        amp = round(random.uniform(75.0, 98.0), 2)
        snr = round(random.uniform(22.0, 35.0), 2)
        
        payloads.append({
            "event_time": current_time,
            "sensor_node": sonda["node"],
            "target_site": sonda["site"],
            "basin_region": sonda["basin"],
            "acoustic_amplitude_db": amp,
            "signal_noise_ratio": snr
        })
        
        print(f"  -> Eco forte detectado: {sonda['node']} | Amp: {amp} dB | SNR: {snr}")
        time.sleep(0.5) # Pausa de meio segundo para dar um efeito dramático no terminal
        
    df_anomaly = pd.DataFrame(payloads)
    
    # Salva o arquivo CSV no datalake (Camada Bronze)
    file_name = INGESTION_DIR / f"manual_burst_{int(time.time())}.csv"
    df_anomaly.to_csv(file_name, index=False)
    
    print(f"\n[{current_time}] ✅ SUCESSO: Lote massivo gravado em {file_name.name}")
    print(">>> O Apache Spark já detectou o arquivo e está processando...")
    print(">>> O seu Dashboard vai receber uma injeção de 4 novas linhas verdes de uma vez!\n")

if __name__ == "__main__":
    inject_hydrocarbon_burst()