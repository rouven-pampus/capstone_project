import pandas as pd
import numpy as np
import datetime as dt
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional
import tensorflow as tf
from scikeras.wrappers import KerasRegressor
from datetime import datetime
from dateutil.relativedelta import relativedelta

##### Data #####

# Load environment variables from .env file
load_dotenv()

# Get database connection parameters from environment variables
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# Create the database URL
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create an engine
engine = create_engine(db_url)

# Define your queries -> set your table name here
query1 = 'SELECT * FROM "02_silver"."fact_full_weather_region"'
query2 = 'SELECT * FROM "03_gold"."fact_electricity_market_germany"'

print('loading data ...')

# Execute the query and load the data into a pandas DataFrame
weather = pd.read_sql(query1, engine).sort_values(by='timestamp').reset_index(drop=True)
gold = pd.read_sql(query2, engine).sort_values(by='timestamp').reset_index(drop=True)

print('Done. Data preparations ...')

# Isolate market from weather
market = gold.drop(columns=['temperature_2m', 'relative_humidity_2m', 'apparent_temperature', 'precipitation', 'cloud_cover', 'wind_speed_10m', 
                            'wind_direction_10m', 'direct_radiation', 'diffuse_radiation', 'sunshine_duration', 'date', 'time'])
# Deal with outliers in the price data
market.loc[market['price_eur_mwh'] < -200, 'price_eur_mwh'] = np.nan

# Replace NAs by imputation
market['price_eur_mwh'] = market['price_eur_mwh'].interpolate()

# Drop forecast column
weather = weather.drop(columns='is_forecast')

# Pivot weather data
weather = weather.pivot(index='timestamp', columns='region')

# Flatten the column multi-index
weather.columns = [f'{col[0]}_{col[1]}' for col in weather.columns]

# Reset the index to convert the timestamp from the index back to a column
weather = weather.reset_index()

# Set end date for last known price
end_date = market['timestamp'].max()

# Create a new DataFrame for the next 72 hours
future_timestamps = pd.date_range(start=end_date + pd.Timedelta(hours=1), periods=72, freq='h')
future_df = pd.DataFrame({'timestamp': future_timestamps})

# Function to create df with lags
def create_lagged_df(df, target='price_eur_mwh', lags=[24, 48, 72]):
    """
    Create a new DataFrame with lagged values for specified intervals.
    
    Parameters:
    - df: pd.DataFrame, the original DataFrame with 'timestamp' and variables.
    - lags: list, list of lag intervals (in hours).
    
    Returns:
    - pd.DataFrame, a new DataFrame with 'timestamp' and lagged variables.
    """
    
    # Initialize the new DataFrame with the timestamp
    new_df = df[['timestamp']].copy()
    
    # Create lagged features
    for col in df.columns:
        if col != 'timestamp':
            for lag in lags:
                new_df[f'{col}_lag_{lag}h'] = df[col].shift(lag)

    # Reset index to bring 'timestamp' back as a column
    new_df = new_df.reset_index(drop=True)
    
    return new_df

# Add future_df to market to create lags
market_lag = pd.concat([market, future_df], axis=0, ignore_index=True)
lagged_market = create_lagged_df(market_lag)

# For 24h forecast:
lagged_market_24 = pd.merge(lagged_market.dropna(), market[['timestamp', 'price_eur_mwh']], on='timestamp', how='left')

# For 48h forecast:
columns_to_keep_48 = [col for col in lagged_market.columns if col.endswith('_48h') or col.endswith('_72h')]
columns_to_keep_48 = ['timestamp'] + columns_to_keep_48
lagged_market_48 = pd.merge(lagged_market[columns_to_keep_48].dropna(), market[['timestamp', 'price_eur_mwh']], on='timestamp', how='left')

# For 72h forecast:
columns_to_keep_72 = [col for col in lagged_market.columns if col.endswith('_72h')]
columns_to_keep_72 = ['timestamp'] + columns_to_keep_72
lagged_market_72 = pd.merge(lagged_market[columns_to_keep_72].dropna(), market[['timestamp', 'price_eur_mwh']], on='timestamp', how='left')

##### Predictions with LSTM model #####

# 24h dataset
pred24 = pd.merge(lagged_market_24, weather, on='timestamp', how='left')

# Function for train-test-split
def train_test(df, datetime='timestamp', target='price_eur_mwh', months=6, enddate=end_date):
    enddate = pd.to_datetime(enddate)
    startdate = enddate - relativedelta(months=6)
    x0 = df[(df[datetime] < enddate) & (df[datetime] >= startdate)]
    X_train = x0.drop([target, datetime], axis=1)
    y_train = x0[target]
    X_test = df[df[datetime] > enddate].drop([target, datetime], axis=1)
    return X_train, y_train, X_test

# train-test-split
X_train_24, y_train_24, X_test_24 = train_test(pred24)

# LSTM prediction function
def lstm_pred(X_train, y_train, X_test):
    # Normalize the data
    scaler_X = MinMaxScaler()
    scaler_y = MinMaxScaler()
    X_train_scaled = scaler_X.fit_transform(X_train)
    X_test_scaled = scaler_X.transform(X_test)
    y_train_scaled = scaler_y.fit_transform(y_train.values.reshape(-1, 1))

    # Reshape data for LSTM [samples, time steps, features]
    X_train_scaled = X_train_scaled.reshape((X_train_scaled.shape[0], 1, X_train_scaled.shape[1]))
    X_test_scaled = X_test_scaled.reshape((X_test_scaled.shape[0], 1, X_test_scaled.shape[1]))

    # Build the model
    model = Sequential()
    model.add(Bidirectional(LSTM(150, activation='relu', return_sequences=True), input_shape=(X_train_scaled.shape[1], X_train_scaled.shape[2])))
    model.add(LSTM(150, activation='relu', return_sequences=True))
    model.add(LSTM(150, activation='relu', return_sequences=True))
    model.add(LSTM(150, activation='relu'))
    model.add(Dropout(0.5))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')
    model.fit(X_train_scaled, y_train_scaled, epochs=70, batch_size=16, verbose=0)

    # Make prediction
    y_pred_test_scaled = model.predict(X_test_scaled)
    y_pred_test = scaler_y.inverse_transform(y_pred_test_scaled).flatten()
    return y_pred_test

print('Done. 24 hour prediction ...')

# Make predictions
preds_24_list = lstm_pred(X_train_24, y_train_24, X_test_24)

print('Done. 48 hour prediction ...')

# 48h dataset
pred48 = pd.merge(lagged_market_48, weather, on='timestamp', how='left')
X_train_48, y_train_48, X_test_48 = train_test(pred48)
preds_48_list = lstm_pred(X_train_48, y_train_48, X_test_48)

print('Done. 72 hour prediction ...')

# 72h dataset
pred72 = pd.merge(lagged_market_72, weather, on='timestamp', how='left')
X_train_72, y_train_72, X_test_72 = train_test(pred72)
preds_72_list = lstm_pred(X_train_72, y_train_72, X_test_72)

print('All done! Predictions are available as preds_24_list, preds_48_list and preds_72_list.')