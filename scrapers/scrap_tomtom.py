import requests
import os
import pandas as pd
from datetime import datetime
import pytz

# Ambil API Key dari .env
API_KEY = os.getenv('TOMTOM_API_KEY')

# Timezone WIB
WIB = pytz.timezone('Asia/Jakarta')

LOCATIONS = [
    # Warkop Kusuma
    {"name": "Node_Warkop_Kusuma",                  "lat": -7.7073187,  "lon": 110.8379588},

    # Slamet Riyadi
    {"name": "Node_SlametRiyadi_ODDSnGETSEE",        "lat": -7.5684313,  "lon": 110.8161152},
    {"name": "Node_SlametRiyadi_IkanGorengCianjur",  "lat": -7.5698850,  "lon": 110.8208305},
    {"name": "Node_SlametRiyadi_BalaiKota",          "lat": -7.5710699,  "lon": 110.8298126},

    # Mangkunegara
    {"name": "Node_Mangkunegara",                    "lat": -7.5681764,  "lon": 110.8230816},

    # Manahan
    {"name": "Node_Manahan",                         "lat": -7.5563497,  "lon": 110.8043078},

    # Tjolomadu
    {"name": "Node_Tjolomadu",                       "lat": -7.5330220,  "lon": 110.7506626},

    # Jl. Solo - Wonogiri
    {"name": "Node_SoloWonogiri_Disdukcapil",        "lat": -7.6630165,  "lon": 110.8361118},
    {"name": "Node_SoloWonogiri_Univet",             "lat": -7.6660685,  "lon": 110.8383185},
    {"name": "Node_SoloWonogiri_RumahDinas",         "lat": -7.6812371,  "lon": 110.8422963},
    {"name": "Node_SoloWonogiri_AlunAlunSukoharjo",  "lat": -7.6823489,  "lon": 110.8407895},

    # GOR Bung Karno
    {"name": "Node_GOR_BungKarno",                   "lat": -7.6875272,  "lon": 110.8523032},
]

def get_warkop_traffic():
    all_results = []
    waktu_sekarang = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')
    
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
                
                print(f"[{hasil['waktu']} WIB] Kelancaran {hasil['lokasi']}: {hasil['tingkat_kelancaran']}%")
                all_results.append(hasil)
                
            else:
                print(f"Error API TomTom di {loc['name']}: {response.status_code}")
        except Exception as e:
            print(f"Gagal koneksi di {loc['name']}: {e}")

    if not all_results:
        raise ValueError("GAGAL: Tidak ada data yang berhasil ditarik dari API")
        
    df = pd.DataFrame(all_results)
    
    folder_path = '/opt/airflow/data/raw'
    os.makedirs(folder_path, exist_ok=True)
    
    # 1. Simpan ke History (Append)
    path_history = os.path.join(folder_path, 'traffic_history.csv')
    df.to_csv(path_history, mode='a', header=not os.path.exists(path_history), index=False)
    
    # 2. Simpan ke Staging (Timpa)
    path_staging = os.path.join(folder_path, 'traffic_staging.csv')
    df.to_csv(path_staging, mode='w', header=True, index=False)
    
    print("Data sukses disimpan ke CSV Staging dan History (WIB).")

if __name__ == "__main__":
    get_warkop_traffic()