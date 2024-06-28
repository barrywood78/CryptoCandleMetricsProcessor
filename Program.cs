using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CryptoCandleMetricsProcessor.Analysis;
using CryptoCandleMetricsProcessor.Database;
using CryptoCandleMetricsProcessor.Models;
using CryptoCandleMetricsProcessor.Importers;
using Microsoft.Data.Sqlite;
using CryptoCandleMetricsProcessor.Exporters;

namespace CryptoCandleMetricsProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            var fields = new List<FieldDefinition>
            {
                new FieldDefinition { Name = "ProductId", DataType = "TEXT" },
                new FieldDefinition { Name = "Granularity", DataType = "TEXT" },
                new FieldDefinition { Name = "StartUnix", DataType = "INTEGER" },
                new FieldDefinition { Name = "StartDate", DataType = "TEXT" },
                new FieldDefinition { Name = "Low", DataType = "REAL" },
                new FieldDefinition { Name = "High", DataType = "REAL" },
                new FieldDefinition { Name = "Open", DataType = "REAL" },
                new FieldDefinition { Name = "Close", DataType = "REAL" },
                new FieldDefinition { Name = "Volume", DataType = "REAL" },
                new FieldDefinition { Name = "PriceUp", DataType = "INTEGER" }, // PriceUp (0 or 1)
                new FieldDefinition { Name = "PriceUpStreak", DataType = "INTEGER" },
                new FieldDefinition { Name = "BuyScore", DataType = "INTEGER" },
                new FieldDefinition { Name = "PercentileBuySignalRank", DataType = "INTEGER" },
                new FieldDefinition { Name = "FixedBuySignalRank", DataType = "INTEGER" },
                new FieldDefinition { Name = "SMA", DataType = "REAL" },  // Simple Moving Average
                new FieldDefinition { Name = "EMA", DataType = "REAL" },  // Exponential Moving Average
                new FieldDefinition { Name = "ATR", DataType = "REAL" },  // Average True Range
                new FieldDefinition { Name = "RSI", DataType = "REAL" },  // Relative Strength Index
                new FieldDefinition { Name = "ADX", DataType = "REAL" },  // Average Directional Index
                new FieldDefinition { Name = "TEMA", DataType = "REAL" }, // Triple Exponential Moving Average
                new FieldDefinition { Name = "BB_SMA", DataType = "REAL" },  // Bollinger Bands SMA
                new FieldDefinition { Name = "BB_UpperBand", DataType = "REAL" },  // Bollinger Bands Upper Band
                new FieldDefinition { Name = "BB_LowerBand", DataType = "REAL" },  // Bollinger Bands Lower Band
                new FieldDefinition { Name = "BB_PercentB", DataType = "REAL" },  // Bollinger Bands Percent B
                new FieldDefinition { Name = "BB_ZScore", DataType = "REAL" },  // Bollinger Bands Z Score
                new FieldDefinition { Name = "BB_Width", DataType = "REAL" },  // Bollinger Bands Width
                new FieldDefinition { Name = "Stoch_K", DataType = "REAL" },  // Stochastic Oscillator %K
                new FieldDefinition { Name = "Stoch_D", DataType = "REAL" },  // Stochastic Oscillator %D
                new FieldDefinition { Name = "MACD", DataType = "REAL" },  // MACD Line
                new FieldDefinition { Name = "MACD_Signal", DataType = "REAL" },  // MACD Signal Line
                new FieldDefinition { Name = "MACD_Histogram", DataType = "REAL" },  // MACD Histogram
                new FieldDefinition { Name = "SuperTrend", DataType = "REAL" },  // SuperTrend
                new FieldDefinition { Name = "WilliamsR", DataType = "REAL" },  // Williams %R
                new FieldDefinition { Name = "OBV", DataType = "REAL" },  // On-Balance Volume (OBV)
                new FieldDefinition { Name = "RollingMean", DataType = "REAL" },
                new FieldDefinition { Name = "RollingStdDev", DataType = "REAL" },
                new FieldDefinition { Name = "RollingVariance", DataType = "REAL" },
                new FieldDefinition { Name = "RollingSkewness", DataType = "REAL" },
                new FieldDefinition { Name = "RollingKurtosis", DataType = "REAL" },
                new FieldDefinition { Name = "ADL", DataType = "REAL" },  // Accumulation/Distribution Line
                new FieldDefinition { Name = "CMF", DataType = "REAL" },  // Chaikin Money Flow
                new FieldDefinition { Name = "CCI", DataType = "REAL" },  // Commodity Channel Index
                new FieldDefinition { Name = "Ichimoku_TenkanSen", DataType = "REAL" },  // Ichimoku Cloud - Tenkan Sen
                new FieldDefinition { Name = "Ichimoku_KijunSen", DataType = "REAL" },  // Ichimoku Cloud - Kijun Sen
                new FieldDefinition { Name = "Ichimoku_SenkouSpanA", DataType = "REAL" },  // Ichimoku Cloud - Senkou Span A
                new FieldDefinition { Name = "Ichimoku_SenkouSpanB", DataType = "REAL" },  // Ichimoku Cloud - Senkou Span B
                new FieldDefinition { Name = "Ichimoku_ChikouSpan", DataType = "REAL" },  // Ichimoku Cloud - Chikou Span
                new FieldDefinition { Name = "ParabolicSar", DataType = "REAL" },  // Parabolic SAR
                new FieldDefinition { Name = "PivotPoint", DataType = "REAL" },  // Pivot Point
                new FieldDefinition { Name = "Resistance1", DataType = "REAL" },  // Pivot Point - Resistance 1
                new FieldDefinition { Name = "Resistance2", DataType = "REAL" },  // Pivot Point - Resistance 2
                new FieldDefinition { Name = "Resistance3", DataType = "REAL" },  // Pivot Point - Resistance 3
                new FieldDefinition { Name = "Support1", DataType = "REAL" },  // Pivot Point - Support 1
                new FieldDefinition { Name = "Support2", DataType = "REAL" },  // Pivot Point - Support 2
                new FieldDefinition { Name = "Support3", DataType = "REAL" },  // Pivot Point - Support 3
                new FieldDefinition { Name = "FibRetracement_23_6", DataType = "REAL" },  // Fibonacci Retracement - 23.6%
                new FieldDefinition { Name = "FibRetracement_38_2", DataType = "REAL" },  // Fibonacci Retracement - 38.2%
                new FieldDefinition { Name = "FibRetracement_50", DataType = "REAL" },  // Fibonacci Retracement - 50%
                new FieldDefinition { Name = "FibRetracement_61_8", DataType = "REAL" },  // Fibonacci Retracement - 61.8%
                new FieldDefinition { Name = "FibRetracement_78_6", DataType = "REAL" },  // Fibonacci Retracement - 76.4%
                new FieldDefinition { Name = "Lagged_Close_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Close_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Close_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_RSI_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_RSI_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_RSI_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Return_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Return_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_Return_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_EMA_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_EMA_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_EMA_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_ATR_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_ATR_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_ATR_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_MACD_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_MACD_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_MACD_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerUpper_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerUpper_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerUpper_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerLower_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerLower_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerLower_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerPercentB_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerPercentB_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_BollingerPercentB_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochK_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochK_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochK_3", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochD_1", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochD_2", DataType = "REAL" },
                new FieldDefinition { Name = "Lagged_StochD_3", DataType = "REAL" }

            };



            var mappings = new List<CsvToDbMapping>
            {
                new CsvToDbMapping { CsvColumnIndex = 0, DbFieldName = "ProductId" },
                new CsvToDbMapping { CsvColumnIndex = 1, DbFieldName = "Granularity" },
                new CsvToDbMapping { CsvColumnIndex = 2, DbFieldName = "StartUnix" },
                new CsvToDbMapping { CsvColumnIndex = 3, DbFieldName = "StartDate" },
                new CsvToDbMapping { CsvColumnIndex = 4, DbFieldName = "Low" },
                new CsvToDbMapping { CsvColumnIndex = 5, DbFieldName = "High" },
                new CsvToDbMapping { CsvColumnIndex = 6, DbFieldName = "Open" },
                new CsvToDbMapping { CsvColumnIndex = 7, DbFieldName = "Close" },
                new CsvToDbMapping { CsvColumnIndex = 8, DbFieldName = "Volume" }
            };

            string dbFilePath = "candles_data.sqlite";
            string tableName = "Candles";
            DatabaseCreator.CreateDatabaseWithTable(dbFilePath, tableName, fields);

            var csvFilePaths = new List<string>
            {
                "C:\\Users\\DELL PC\\Desktop\\Candle Data\\BTC-USDC_FIFTEEN_MINUTE.csv",
                "C:\\Users\\DELL PC\\Desktop\\Candle Data\\BTC-USDC_FIVE_MINUTE.csv",
                "C:\\Users\\DELL PC\\Desktop\\Candle Data\\BTC-USDC_ONE_DAY.csv",
                "C:\\Users\\DELL PC\\Desktop\\Candle Data\\BTC-USDC_ONE_HOUR.csv"
            };

            foreach (var csvFilePath in csvFilePaths)
            {
                CsvImporter.ImportCsvToDatabase(csvFilePath, dbFilePath, tableName, mappings, true);
                Console.WriteLine($"CSV data from {csvFilePath} imported successfully.");
            }


            TechnicalAnalysis.CalculateIndicators(dbFilePath, tableName);
            Console.WriteLine("Indicators calculated successfully.");

            CsvExporter.ExportDatabaseToCsv(dbFilePath);
            Console.WriteLine("CSV data exported successfully.");


        }

        
    }
}
