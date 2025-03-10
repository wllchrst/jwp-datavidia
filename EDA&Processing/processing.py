'''
Script for processing all the dataset

TODOLIST:
[X] Global Community Data
[X] Google Trend
[X] Harga Bahan Pangan
[X] Mata Uang
[X] Joined Dataset into One

'''
import os
import warnings
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

# CONSTANTS
DATASET_PATH = "../comodity-price-prediction-penyisihan-arkavidia-9/"
GLOBAL_COMMODITY_FOLDER = os.path.join(DATASET_PATH, "Global Commodity Price")
GOOGLE_TREND_FOLDER = os.path.join(DATASET_PATH, 'Google Trend')
COMMODITY_PRICE_FOLDER = os.path.join(DATASET_PATH, 'Harga Bahan Pangan/train')
CURRENCY_EXCHANGE_FOLDER = os.path.join(DATASET_PATH, 'Mata Uang')

# Functions

def get_first_word(name: str):
    return name.split()[0].lower()

def fill_missing_dates(df: pd.DataFrame, start_date="2022-01-01", end_date="2024-09-30") -> pd.DataFrame:
    """
    Ensures the dataset has all dates between start_date and end_date.
    Missing dates are filled using forward fill (ffill) and backward fill (bfill).

    Parameters:
    df (pd.DataFrame): Input DataFrame with a 'Date' column.
    start_date (str): Start date in YYYY-MM-DD format.
    end_date (str): End date in YYYY-MM-DD format.

    Returns:
    pd.DataFrame: DataFrame with complete date range and missing values filled.
    """
    # Convert 'Date' column to datetime format
    df["Date"] = pd.to_datetime(df["Date"])

    # Create a full date range
    full_dates = pd.DataFrame({"Date": pd.date_range(start=start_date, end=end_date)})

    # Merge the dataset with full date range (ensuring all dates are present)
    df = full_dates.merge(df, on="Date", how="left")

    # Fill missing values using forward fill, then backward fill as backup
    df.ffill(inplace=True)
    df.bfill(inplace=True)

    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the given DataFrame by:
    1. Dropping rows with any NaN values.
    2. Dropping duplicate rows.
    3. Dropping columns that contain NaN values.

    Parameters:
    df (pd.DataFrame): Input DataFrame.

    Returns:
    pd.DataFrame: Cleaned DataFrame.
    """
    df = df.dropna()  # Drop rows with NaN values
    df = df.drop_duplicates()  # Drop duplicate rows
    df = df.dropna(axis=1)  # Drop columns that have any NaN values
    return df.reset_index(drop=True)  # Reset index for clean output

def replace_zeros_with_mean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces zero values in numerical columns with the mean of the column,
    ignoring zero values when calculating the mean.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame

    Returns:
    pd.DataFrame: DataFrame with zeros replaced by mean values.
    """
    df = df.copy()  # Avoid modifying original data
    
    for col in df.select_dtypes(include=[np.number]).columns:  # Only apply to numeric columns
        non_zero_mean = df.loc[df[col] != 0, col].mean()  # Compute mean ignoring zeros
        df[col] = df[col].replace(0, non_zero_mean)  # Replace zeros with computed mean
    
    return df

def get_global_commodity_data(folder_path=GLOBAL_COMMODITY_FOLDER, start_date='2022-01-01', end_date='2024-09-30') -> pd.DataFrame:
    """
    Get Global Commodity Data joined nad processed

    Returns:
        pd.DataFrame: Joined and processed DataFrame
    """
    csv_files = os.listdir(folder_path)
    joined_dataset = None

    for csv in csv_files:
        path = f'{folder_path}/{csv}'
        file_name = csv.split('Futures Historical Data')[0].strip()
        df = pd.read_csv(path)
        df['Commodity'] = file_name
        clean_df = clean_data(df)
        clean_df = fill_missing_dates(clean_df, start_date=start_date, end_date=end_date)

        joined_dataset = clean_df if joined_dataset is None else pd.concat([joined_dataset, clean_df], ignore_index=True)
        
    joined_dataset["Vol."] = joined_dataset["Vol."].str.replace("K", "").astype(float) * 1000
    joined_dataset["Change %"] = joined_dataset["Change %"].str.replace("%", "").astype(float)
    cols_to_convert = ["Price", "Open", "High", "Low"]
    joined_dataset[cols_to_convert] = joined_dataset[cols_to_convert].apply(pd.to_numeric, errors="coerce")
    
    joined_dataset = joined_dataset.groupby("Date").agg(
    {
        "Open": "mean",  # Average Open price
        "High": "max",  # Highest High price
        "Low": "min",  # Lowest Low price
        "Vol.": "sum",  # Total Volume
        "Change %": "mean",  # Average Change %
        "Price": "mean",  # Average Global Price
        "Commodity": lambda x: ", ".join(set(x))  # Unique list of commodities
    }
    ).reset_index()

    for column in joined_dataset.columns:
        if column == 'Date':
            continue
        joined_dataset = joined_dataset.rename(columns={column: f'Global{column}'})

    joined_dataset.drop(columns=['GlobalCommodity'], inplace=True)
    return joined_dataset

def get_google_trend_data() -> pd.DataFrame:
    """
    Get Google Trend data, process it, and calculate the average GTPrice per commodity per date.

    Returns:
        pd.DataFrame: Dataframe containing average GTPrice per commodity per date.
    """ 
    commodities = os.listdir(GOOGLE_TREND_FOLDER)
    joined_dataset = None

    for commodity in commodities:
        commodity_folder = f'{GOOGLE_TREND_FOLDER}/{commodity}'
        provinces = os.listdir(commodity_folder)

        for province_file in provinces:
            dataset_path = f'{commodity_folder}/{province_file}'
            province = province_file.split('.')[0]
            df = pd.read_csv(dataset_path)
            df['Commodity'] = commodity.lower()
            df['Province'] = province.lower()
            price_column = df.columns[1]
            df['GTPrice'] = df[price_column]

            df = replace_zeros_with_mean(df)
            df = clean_data(df)
            df = fill_missing_dates(df)
            df.drop(columns=[df.columns[1]], inplace=True)

            joined_dataset = df if joined_dataset is None else pd.concat([joined_dataset, df])
    
    avg_trend_by_date = joined_dataset.groupby(['Date', 'Commodity'])['GTPrice'].mean().reset_index()
    avg_trend_by_date['Commodity'] = avg_trend_by_date['Commodity'].apply(lambda x: x.split()[0] if len(x.split()) > 1 else x)
    
    return avg_trend_by_date

def get_indonesia_commodity_price_data(folder_path=COMMODITY_PRICE_FOLDER, start_date='2022-01-01', end_date='2024-09-30') -> pd.DataFrame:
    """
    Get Indonesia Commodity data all joined and processed

    Returns:
        pd.DataFrame: Joined and processed dataframe
    """
    csv_files = os.listdir(folder_path)
    final_df = None

    for file in csv_files:
        path = f'{folder_path}/{file}'
        commodity_name = file.split(".")[0].lower()
        df = pd.read_csv(path)
        df['commodity'] = commodity_name

        final_df = df if final_df is None else pd.concat([final_df, df])
   
    final_df = clean_data(final_df)
    final_df = fill_missing_dates(final_df, start_date, end_date)

    df_melted = final_df.melt(id_vars=["Date", "commodity"], var_name="province", value_name="price")

    return df_melted

def get_currency_exchange_data(folder_path=CURRENCY_EXCHANGE_FOLDER, start_date='2022-01-01', end_date='2024-09-30') -> pd.DataFrame:
    """
    Get Currency Exchange data all joined and processed

    Returns:
        pd.DataFrame: Joined and processed dataframe
    """
    csv_files = os.listdir(folder_path)
    final_df = None

    for file in csv_files:
        path = f'{folder_path}/{file}'
        file_description = file.split('=')[0]
        df = pd.read_csv(path)
        df.drop(columns=['Volume'], inplace=True)
        df = clean_data(df)
        df = fill_missing_dates(df, start_date, end_date)
        df['desc'] = file_description
        final_df = df if final_df is None else pd.concat([final_df, df])

    final_df['Date'] = pd.to_datetime(final_df['Date'])
    
    if 'Ajd Close' in final_df.columns:
        final_df.drop(columns=['Adj Close'], inplace=True)

    final_df = final_df.groupby("Date").agg(
    {
        "Close": "mean",
        "High": "max",
        "Low": "min",
        "Open": "mean",
        "desc": lambda x: ", ".join(set(x))
    }
    ).reset_index()

    final_df.drop(columns=['desc'], inplace=True)
    
    for col in final_df.columns:
        if col == 'Date':
            continue
        final_df = final_df.rename(columns={col: f'CE_{col}'})

    return final_df

##################################################################

def get_dataset(mixed_with_google_trend=False) -> pd.DataFrame:
    """Get dataset that can be used for training.

    Args:
        mixed_with_google_trend (bool): Whether to include Google Trends data.

    Returns:
        pd.DataFrame: Merged dataset.
    """
    if mixed_with_google_trend:
        google_trend_dataset = get_google_trend_data()
        google_trend_dataset['Date'] = pd.to_datetime(google_trend_dataset['Date'])

    global_commodity_dataset = get_global_commodity_data()
    indonesia_commodity_price = get_indonesia_commodity_price_data()
    currency_exchange_data = get_currency_exchange_data()

    global_commodity_dataset['Date'] = pd.to_datetime(global_commodity_dataset['Date'])
    indonesia_commodity_price['Date'] = pd.to_datetime(indonesia_commodity_price['Date'])
    currency_exchange_data['Date'] = pd.to_datetime(currency_exchange_data['Date'])

    trend_with_indonesia = pd.merge(indonesia_commodity_price, global_commodity_dataset, on='Date', how='left')
    final_df = pd.merge(trend_with_indonesia, currency_exchange_data, on='Date', how='left')

    if mixed_with_google_trend:
        final_df['commodity_norm'] = final_df['commodity'].apply(get_first_word)
        google_trend_dataset['Commodity_norm'] = google_trend_dataset['Commodity'].apply(get_first_word)

        merged_df = final_df.merge(
            google_trend_dataset[['Date', 'Commodity_norm', 'GTPrice']],
            left_on=['Date', 'commodity_norm'],
            right_on=['Date', 'Commodity_norm'],
            how='left'
        )
        
        merged_df.drop(columns=['commodity_norm', 'Commodity_norm'], inplace=True)

        return merged_df
    return final_df

def process_test_dataset() -> None:
    folder = '../AdditionalDataset/GlobalCommodity'

    for csv in os.listdir(folder):
        path = f'{folder}/{csv}'
        df = pd.read_csv(path, quotechar='"')
        df.to_csv(path, index=False)

def get_test_dataset() -> pd.DataFrame:
    process_test_dataset()

    global_data = get_global_commodity_data('../AdditionalDataset/GlobalCommodity/', start_date='2024-10-01', end_date='2024-12-31')
    currency_data = get_currency_exchange_data('../AdditionalDataset/CurrencyExchange/', start_date='2024-10-01', end_date='2024-12-31')
    commodity_test_dataset = get_indonesia_commodity_price_data('../comodity-price-prediction-penyisihan-arkavidia-9/Harga Bahan Pangan/test',
                                                    start_date='2024-10-01', end_date='2024-12-31')
    
    commodity_test_dataset.drop(columns=['price'], inplace=True)
    
    commodity_with_global = pd.merge(commodity_test_dataset, global_data,
                                       on='Date', how='left')
    
    merged = pd.merge(commodity_with_global, currency_data,
                        on='Date', how='left')

    return merged

if __name__ == "__main__":
    # training_dataset = get_dataset()
    # test_dataset = get_test_dataset()
    # mixed_training_dataset = get_dataset(mixed_with_google_trend=True)
    # training_dataset.to_csv("../comodity-price-prediction-penyisihan-arkavidia-9/training_dataset.csv")
    # test_dataset.to_csv("../comodity-price-prediction-penyisihan-arkavidia-9/testing_dataset.csv")
    # mixed_training_dataset.to_csv("../comodity-price-prediction-penyisihan-arkavidia-9/mixed_training_dataset.csv")
    
    # d = get_google_trend_data()
    
    # print(d.head())
    
    # print(d['Commodity'].value_counts())
    m = get_dataset(mixed_with_google_trend=True)
    print(m.head())
    print(m.columns)