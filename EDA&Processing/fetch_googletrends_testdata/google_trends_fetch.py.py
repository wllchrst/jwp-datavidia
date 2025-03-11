from pytrends.request import TrendReq
import pandas as pd
from time import sleep
from random import randint

# Inisialisasi Pytrends
pytrends = TrendReq(hl='id', tz=420)

# Daftar bahan pangan
keywords = [
    "bawang", "bawang merah", "bawang putih", "beras", "cabai",
    "cabai merah", "cabai rawit", "daging", "daging ayam", "daging sapi",
    "gula", "minyak goreng", "telur ayam", "tepung", "tepung terigu"
]

# Parameter pencarian
timeframe = "2024-10-01 2024-12-31"
geo = "ID"  # Indonesia

# Dictionary untuk menyimpan hasil
all_data = {}

# Looping untuk tarik data per keyword dengan delay acak
for keyword in keywords:
    print(f"Mengambil data untuk: {keyword}...")
    2
    try:
        pytrends.build_payload([keyword], timeframe=timeframe, geo=geo)
        data = pytrends.interest_over_time()
        
        if not data.empty:
            all_data[keyword] = data[keyword]
            print(f"✅ Data untuk {keyword} berhasil diambil.")
        else:
            print(f"⚠️ Data kosong untuk {keyword}.")
    
    except Exception as e:
        print(f"❌ Error saat mengambil data {keyword}: {e}")
    
    # Delay acak antara 10-30 detik untuk menghindari blokir
    delay = randint(25, 75)
    print(f"Menunggu {delay} detik sebelum request berikutnya...\n")
    sleep(delay)

# Gabungkan semua data ke dalam satu DataFrame
trends_df = pd.DataFrame(all_data)

# Simpan ke file CSV
trends_df.to_csv("./google_trends_test_data.csv")

print("✅ Semua data telah berhasil disimpan dalam 'google_trends_bahan_pangan.csv'")