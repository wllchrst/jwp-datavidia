'''
Script for processing all the dataset

TODOLIST:
[X] Global Community Data
[X] Google Trend
[] Harga Bahan Pangan
[X] Mata Uang
[] Joined Dataset into One

'''
import os
import pandas as pd
import numpy as np

# CONSTANTS
DATASET_PATH = "../comodity-price-prediction-penyisihan-arkavidia-9/"
GLOBAL_COMMODITY_FOLDER = os.path.join(DATASET_PATH, "Global Commodity Price")
GOOGLE_TREND_FODLER = os.path.join(DATASET_PATH, 'Google Trend')
COMMODITY_PRICE_FOLDER = os.path.join(DATASET_PATH, 'Harga Bahan Pangan/train')
CURRENCY_EXCHANGE_FOLDER = os.path.join(DATASET_PATH, 'Mata Uang')

# Functions

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

def get_global_community_data() -> pd.DataFrame:
    """
    Get Global Community Data joined nad processed

    Returns:
        pd.DataFrame: Joined and processed DataFrame
    """
    csv_files = os.listdir(GLOBAL_COMMODITY_FOLDER)
    final_df = None

    for csv in csv_files:
        path = f'{GLOBAL_COMMODITY_FOLDER}/{csv}'
        file_name = csv.split('Futures Historical Data')[0].strip()
        df = pd.read_csv(path)
        df['Commodity'] = file_name
        clean_df = clean_data(df)
        clean_df = fill_missing_dates(clean_df)
        final_df = clean_df if final_df is None else pd.concat([final_df, clean_df], ignore_index=True)
    
    return final_df

def get_google_trend_data() -> pd.DataFrame:
    """
    Get Google Trend data all joined and processed

    Returns:
        pd.DataFrame: Joined and processed dataframe
    """
   
    commodities = os.listdir(GOOGLE_TREND_FODLER)
    final_df = None

    for commodity in commodities:
        commodity_folder = f'{GOOGLE_TREND_FODLER}/{commodity}'
        provinces = os.listdir(commodity_folder)
        print(len(provinces))

        for province_file in provinces:
            dataset_path = f'{commodity_folder}/{province_file}'
            province = province_file.split('.')[0]
            df = pd.read_csv(dataset_path)
            df['Commodity'] = commodity.lower()
            df['Province'] = province.lower()

            df = replace_zeros_with_mean(df)
            df = clean_data(df)
            df = fill_missing_dates(df)

            final_df = df if final_df is None else pd.concat([final_df, df])

    return final_df

def get_indonesia_commodity_price_data() -> pd.DataFrame:
    """
    Get Indonesia Commodity data all joined and processed

    Returns:
        pd.DataFrame: Joined and processed dataframe
    """
    csv_files = os.listdir(COMMODITY_PRICE_FOLDER)
    final_df = None

    for file in csv_files:
        path = f'{COMMODITY_PRICE_FOLDER}/{file}'
        df = pd.read_csv(path)

        final_df = df if final_df is None else pd.concat([final_df, df])

    return final_df

def get_currency_exchange_data() -> pd.DataFrame:
    """
    Get Currency Exchange data all joined and processed

    Returns:
        pd.DataFrame: Joined and processed dataframe
    """
    csv_files = os.listdir(CURRENCY_EXCHANGE_FOLDER)
    final_df = None

    for file in csv_files:
        path = f'{CURRENCY_EXCHANGE_FOLDER}/{file}'
        file_description = file.split('=')[0]
        print(file_description)
        df = pd.read_csv(path)
        df.drop(columns=['Volume'])
        df = clean_data(df)
        df = fill_missing_dates(df)
        final_df = df if final_df is None else pd.concat([final_df, df])
    
    return final_df

if __name__ == '__main__':
    dataset = get_currency_exchange_data()
    print(dataset.head())