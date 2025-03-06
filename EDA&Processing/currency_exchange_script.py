import yfinance as yf
import pandas as pd

currency_pairs = ['MYRUSD=X', 'SGDUSD=X', 'THBUSD=X', 'USDIDR=X']
start_date = '2024-10-01'
end_date = '2024-12-31'

currency_pairs = ['MYRUSD=X', 'SGDUSD=X', 'THBUSD=X', 'USDIDR=X']

for pair in currency_pairs:
    df = yf.download(pair, start=start_date, end=end_date)

    df.to_csv(f'../AdditionalDataset/CurrencyExchange/{pair}.csv', index=True)