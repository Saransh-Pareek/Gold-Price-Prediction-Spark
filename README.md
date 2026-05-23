# Gold Price Prediction with Apache Spark

An end-to-end analytics pipeline for predicting Gold (XAU/USD) price movements using Apache Spark for distributed data processing.

## Overview

This project analyses 1-hour interval Gold vs USD candlestick data to build a forecasting model. It uses PySpark for ETL, feature engineering, anomaly detection, and time-series forecasting.

## Project Structure

```
├── gold_prediction_analysis.ipynb  # Main analysis notebook
├── config.yaml                     # Pipeline configuration
├── docker-compose.yml              # Spark cluster setup
├── requirements.txt                # Python dependencies
├── src/
│   ├── etl.py                      # Extract, Transform, Load pipeline
│   └── utils.py                    # Helper utilities
└── data/
    └── XAU_1h_data.csv             # Gold price dataset (1-hour OHLCV)
```

## Features

- **Distributed ETL** with Apache Spark
- **Feature Engineering**: technical indicators, rolling statistics, lag features
- **Anomaly Detection**: Z-score based outlier identification
- **Time-Series Forecasting**: 24-hour ahead prediction
- **K-Means Clustering** of price regimes

## Tech Stack

- Apache Spark (PySpark)
- Docker & Docker Compose
- Python (pandas, scikit-learn, matplotlib)
- Jupyter Notebook

## Setup

```bash
# Start Spark cluster
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run the notebook
jupyter notebook gold_prediction_analysis.ipynb
```

## Dataset

- **Source**: XAU/USD 1-hour OHLCV candlestick data
- **Columns**: Date, Open, High, Low, Close, Volume
- **Forecast horizon**: 24 intervals (1 day ahead)

## Author

Saransh Pareek — Master of Data Science, University of Queensland
