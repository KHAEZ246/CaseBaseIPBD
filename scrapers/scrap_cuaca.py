import requests
import pandas as pd
import os
from datetime import datetime
import pytz

WIB = pytz.timezone('Asia/Jakarta')

API_KEY = os.getenv('OPENWEATHER_API_KEY')

# Koordinat Warkop Kusuma / Sukoharjo
LAT = -7.7073187
LON = 110.8379588
KOTA = 'Sukoharjo'

def get_cuaca_solo():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric&lang=id"

    try:
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Error API OpenWeather: {response.status_code}")
            return

        data = response.json()

        waktu_sekarang = datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')

        hasil = {
            'waktu'           : waktu_sekarang,
            'kota'            : KOTA,
            'suhu'            : data['main']['temp'],
            'suhu_terasa'     : data['main']['feels_like'],
            'kelembapan'      : data['main']['humidity'],
            'kondisi'         : data['weather'][0]['main'],        # Rain, Clear, Clouds, dll
            'deskripsi'       : data['weather'][0]['description'], # hujan lebat, cerah, dll
            'kecepatan_angin' : data['wind']['speed'],
            'curah_hujan'     : data.get('rain', {}).get('1h', 0), # mm/jam, 0 kalau tidak hujan
            'cloudiness'      : data['clouds']['all'],             # % awan
        }

        print(f"[{waktu_sekarang}] Cuaca Solo: {hasil['kondisi']} - {hasil['deskripsi']} | Suhu: {hasil['suhu']}°C | Hujan: {hasil['curah_hujan']}mm")

        # Simpan ke CSV
        folder_path = '/opt/airflow/data/raw'
        os.makedirs(folder_path, exist_ok=True)

        # History (append)
        path_history = os.path.join(folder_path, 'cuaca_history.csv')
        df = pd.DataFrame([hasil])
        df.to_csv(path_history, mode='a', header=not os.path.exists(path_history), index=False)

        # Staging (timpa)
        path_staging = os.path.join(folder_path, 'cuaca_staging.csv')
        df.to_csv(path_staging, mode='w', header=True, index=False)

        print("Data cuaca berhasil disimpan!")

    except Exception as e:
        print(f"Gagal ambil data cuaca: {e}")

if __name__ == "__main__":
    get_cuaca_solo()