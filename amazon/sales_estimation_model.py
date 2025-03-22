import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import matplotlib.pyplot as plt
import json

# Load your dataset
df = pd.read_json('/Users/waleed/Library/CloudStorage/Box-Box/Scrapy02/scrapy_01/amazon/data/dataset.json')

# Convert rank columns to numeric values, coercing errors to NaN
df['best_sellers_rank_01_no'] = pd.to_numeric(df['best_sellers_rank_01_no'], errors='coerce')
df['best_sellers_rank_02_no'] = pd.to_numeric(df['best_sellers_rank_02_no'], errors='coerce')

# Refined log-transformation to prevent complex numbers (ensure no zero or negative ranks)
def safe_log(x):
    if x <= 0:
        return np.log(1)  # Return log(1) instead of log(0) to prevent complex numbers
    else:
        return np.log(x + 1)  # Apply log to valid positive values

# Apply the log transformation to rank columns
df['log_rank_01'] = df['best_sellers_rank_01_no'].apply(safe_log)
df['log_rank_02'] = df['best_sellers_rank_02_no'].apply(safe_log)

# Convert price, star_rating, and total_rating to numeric values, coercing errors to NaN
df['price'] = pd.to_numeric(df['price'], errors='coerce')
df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
df['total_rating'] = pd.to_numeric(df['total_rating'], errors='coerce')

# Handle missing values by filling them with 0 (or use forward filling if needed)
df.fillna(0, inplace=True)

# Apply np.real() to remove any complex numbers that may still exist in the dataset
df = df.applymap(lambda x: np.real(x) if isinstance(x, complex) else x)

# Convert scraping_time to datetime (str to datetime conversion)
df['scraping_time'] = pd.to_datetime(df['scraping_time'], format='%I:%M %p', errors='coerce')

# Convert to minutes past midnight
df['scraping_time_minutes'] = df['scraping_time'].dt.hour * 60 + df['scraping_time'].dt.minute

# Force the entire dataframe to have no complex numbers and be of type float64
df = df.astype('float64', errors='ignore')  # cast all columns to float64

# Define price categories based on price ranges
def categorize_price(price):
    if price <= 20:
        return 'Low'
    elif 20 < price <= 50:
        return 'Medium'
    else:
        return 'High'

# Apply price categorization
df['price_category'] = df['price'].apply(categorize_price)

# Encode price category as numeric values (optional)
df['price_category_encoded'] = df['price_category'].map({'Low': 0, 'Medium': 1, 'High': 2})

# Define additional features and handle potential issues with extreme values
df['discount_percentage'] = df['discount'].str.replace('%', '').astype(float, errors='ignore') / 100

# Interaction between features
df['star_rating_review_interaction'] = df['star_rating'] * df['total_rating']

# Availability flag (assuming "Available" is 1, others as 0)
df['availability_flag'] = df['availability'].apply(lambda x: 1 if x == 'Available' else 0)

# Amazon's choice flag (assuming "YES" is 1, "NO" is 0)
df['amazons_choice_flag'] = df['amazons_choice'].apply(lambda x: 1 if x == 'YES' else 0)

# Scraping month and weekday features
df['scraping_month'] = df['scraping_time'].dt.month
df['scraping_weekday'] = df['scraping_time'].dt.weekday

# Price to rating ratio
df['price_to_rating_ratio'] = df['price'] / df['star_rating']

# Log rank interaction (interaction term)
df['log_rank_interaction'] = df['log_rank_01'] * df['log_rank_02']

# Calculate the target variable 'estimated_sales_per_day'
def estimate_sales(rank):
    rank = max(rank, 1)  # Avoid invalid operations by ensuring rank is at least 1
    theta = -0.66  # Example value, can be customized per category
    return np.real((rank - 0.4) ** (-0.4 / theta))  # Ensure that the result is real

df['estimated_sales_per_day'] = df['best_sellers_rank_01_no'].apply(estimate_sales)

# Handling potential inf values
df.replace([np.inf, -np.inf], np.nan, inplace=True)

# Handle missing or NaN values in the features dataset by filling with 0
df.fillna(0, inplace=True)

# Define the features list before use
features = ['log_rank_01', 'log_rank_02', 'price', 'star_rating', 'total_rating', 'scraping_time_minutes', 
            'price_category_encoded', 'discount_percentage', 'star_rating_review_interaction', 
            'availability_flag', 'amazons_choice_flag', 'scraping_month', 
            'scraping_weekday', 'price_to_rating_ratio', 'log_rank_interaction']

# Features and target variable
target = 'estimated_sales_per_day'

# Prepare the features (X) and target variable (y)
X = df[features]
y = df[target]

# Check the data types to ensure everything is numeric and real
print("Features data types:")
print(X.dtypes)
print("Target data type:")
print(y.dtypes)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize the model
model = RandomForestRegressor(n_estimators=100, random_state=42)

# Train the model
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate the model
mae = mean_absolute_error(y_test, y_pred)
print(f"Mean Absolute Error (MAE): {mae}")

# Evaluate the model using R-squared
r2 = model.score(X_test, y_test)
print(f"R-squared: {r2}")

# Visualize the predicted vs actual values
plt.figure(figsize=(8, 6))
plt.scatter(y_test, y_pred)
plt.plot([0, max(y_test)], [0, max(y_test)], color='red', linestyle='--')
plt.xlabel('Actual Sales')
plt.ylabel('Predicted Sales')
plt.title('Actual vs Predicted Sales')
plt.show()

# Use the model to predict sales for the entire dataset
df['predicted_sales'] = model.predict(X)

# Convert 'scraping_time' to string for JSON serialization
df['scraping_time'] = df['scraping_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Save the results to a new JSON file with readable format (pretty-printing)
json_data = df.to_dict(orient='records')
with open('data2/sales_predictions.json', 'w') as json_file:
    json.dump(json_data, json_file, indent=4)

# Save the results in CSV format for easy access
df[['serial_number', 'title', 'category_01', 'best_sellers_rank_01_no', 'predicted_sales']].to_csv('data2/sales_predictions.csv', index=False)
