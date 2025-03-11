from pytrends.request import TrendReq
import pandas as pd
from time import sleep
from random import randint

pytrends = TrendReq(hl='id', tz=420)

# Daftar bahan pangan
keywords = [
    "bawang", "bawang merah", "bawang putih", "beras", "cabai",
    "cabai merah", "cabai rawit", "daging", "daging ayam", "daging sapi",
    "gula", "minyak goreng", "telur ayam", "tepung", "tepung terigu"
]

timeframe = "2024-10-01 2024-12-31"
geo = "ID"

all_data = {}

while len(keywords) > 0:
    for keyword in keywords:
        print(f"Mengambil data untuk: {keyword}...")
        
        try:
            pytrends.build_payload([keyword], timeframe=timeframe, geo=geo)
            data = pytrends.interest_over_time()
            
            if not data.empty:
                all_data[keyword] = data[keyword]
                print(f"✅ Data untuk {keyword} berhasil diambil.")
            else:
                print(f"⚠️ Data kosong untuk {keyword}.")
                
            keywords.remove(keyword)
        
        except Exception as e:
            print(f"❌ Error saat mengambil data {keyword}: {e}")

    print('Delaying 30 seconds')
    sleep(30)

trends_df = pd.DataFrame(all_data)

trends_df.to_csv("./google_trends_test_data.csv")

print("✅ Semua data telah berhasil disimpan dalam 'google_trends_bahan_pangan.csv'")