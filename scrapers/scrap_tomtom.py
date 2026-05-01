import requests
import os
import pandas as pd
from datetime import datetime

# Ambil API Key dari .env
API_KEY = os.getenv('TOMTOM_API_KEY')

LOCATIONS = [
    {"name": "Node_Solo_Ngarsopuro", "lat": -7.5676, "lon": 110.8231},
    {"name": "Node_Soba_Pandawa", "lat": -7.6056, "lon": 110.8143},
    {"name": "Node_Skh_Pasar", "lat": -7.6746, "lon": 110.8353},
    {"name": "Node_Kusuma_Grogol", "lat": -7.6254, "lon": 110.8198}
]

def get_warkop_traffic():
    all_results = []
    waktu_sekarang = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for loc in LOCATIONS:
        url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={API_KEY}&point={loc['lat']},{loc['lon']}"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                res_data = response.json()['flowSegmentData']
                
                hasil = {
                    'waktu': waktu_sekarang,
                    'lokasi': loc['name'],  
                    'kecepatan_sekarang': res_data['currentSpeed'],
                    'kecepatan_bebas': res_data['freeFlowSpeed'],
                    'tingkat_kelancaran': round((res_data['currentSpeed'] / res_data['freeFlowSpeed']) * 100, 2)
                }
                
                print(f"[{hasil['waktu']}] Kelancaran {hasil['lokasi']}: {hasil['tingkat_kelancaran']}%")
                all_results.append(hasil)
                
            else:
                print(f"Error API TomTom di {loc['name']}: {response.status_code}")
        except Exception as e:
            print(f"Gagal koneksi di {loc['name']}: {e}")

    # Simpan ke CSV
    if not all_results:
        raise ValueError("GAGAL: Tidak ada data yang berhasil ditarik dari API")
        
    df = pd.DataFrame(all_results)
    
    # Pastikan folder ada
    folder_path = '/opt/airflow/data/raw'
    os.makedirs(folder_path, exist_ok=True)
    
    # 1. Simpan ke History (Append) - Untuk Backup
    path_history = os.path.join(folder_path, 'traffic_history.csv')
    df.to_csv(path_history, mode='a', header=not os.path.exists(path_history), index=False)
    
    # 2. Simpan ke Staging (Write/Timpa) - Khusus untuk dibaca Airflow ke DB
    path_staging = os.path.join(folder_path, 'traffic_staging.csv')
    df.to_csv(path_staging, mode='w', header=True, index=False)
    
    print("Data sukses disimpan ke CSV Staging dan History.")

if __name__ == "__main__":
    get_warkop_traffic()