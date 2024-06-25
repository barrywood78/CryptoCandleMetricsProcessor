# Crypto Candle Metrics Processor

Crypto Candle Metrics Processor is a tool for importing, analyzing, and exporting cryptocurrency candle data. It includes functionality for calculating various technical indicators and exporting the data to CSV files.

## Features

- **Database Creation**: Create a SQLite database and define the schema for storing candle data.
- **CSV Import**: Import candle data from CSV files into the database.
- **Technical Analysis**: Calculate various technical indicators such as SMA, EMA, ATR, RSI, MACD, Bollinger Bands, etc.
- **CSV Export**: Export the processed candle data with calculated indicators to CSV files.

## Dependencies

- CsvHelper
- Math.Net.Numerics
- Microsoft.Data.Sqlite
- Skender.Stock.Indicators
