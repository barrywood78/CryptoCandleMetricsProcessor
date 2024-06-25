using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using System.Globalization;
using System.IO;
using CryptoCandleMetricsProcessor.Analysis.Indicators;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis
{
    public static class TechnicalAnalysis
    {
        public static void CalculateIndicators(string dbFilePath, string tableName)
        {
            string connectionString = $"Data Source={Path.GetFullPath(dbFilePath)}";
            var groupedCandles = new Dictionary<(string ProductId, string Granularity), List<Quote>>();

            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();
                string selectQuery = $"SELECT ProductId, Granularity, StartUnix, StartDate, Open, High, Low, Close, Volume FROM {tableName} ORDER BY ProductId, Granularity, StartUnix";

                using (var command = new SqliteCommand(selectQuery, connection))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var key = (ProductId: reader.GetString(0), Granularity: reader.GetString(1));
                        if (!groupedCandles.ContainsKey(key))
                        {
                            groupedCandles[key] = new List<Quote>();
                        }

                        groupedCandles[key].Add(new Quote
                        {
                            Date = reader.GetDateTime(3),
                            Open = reader.GetDecimal(4),
                            High = reader.GetDecimal(5),
                            Low = reader.GetDecimal(6),
                            Close = reader.GetDecimal(7),
                            Volume = reader.GetDecimal(8)
                        });
                    }
                }

                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var group in groupedCandles)
                    {
                        var productId = group.Key.ProductId;
                        var granularity = group.Key.Granularity;
                        var candles = group.Value;

                        // Define custom periods for each indicator
                        var periods = GetPeriodsForGranularity(granularity);

                        // Call individual indicator calculations with custom periods
                        PriceUpIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);
                        SmaIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["SMA"]);
                        EmaIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["EMA"]);
                        AtrIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["ATR"]);
                        RsiIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["RSI"]);
                        AdxIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["ADX"]);
                        TemaIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["TEMA"]);
                        BollingerBandsIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["BollingerBands"]);
                        StochasticIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["StochasticOscillator"]);
                        MacdIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);
                        SupertrendIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["Supertrend"]);
                        WilliamsRIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["WilliamsR"]);
                        ObvIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);
                        StatisticalIndicators.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["SMA"]);
                        AdLineIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);
                        CmfIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["CMF"]);
                        CciIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, periods["CCI"]);
                        IchimokuIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);
                        ParabolicSarIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);
                        RollingPivotPointsIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, (int)PeriodSize.Day, (int)PivotPointType.Standard);
                        FibonacciRetracementIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);
                        RollingPivotPointsIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles, 20, 10, (int)PivotPointType.Standard); // Adjusted line
                        LaggedFeaturesIndicator.Calculate(connection, transaction, tableName, productId, granularity, candles);

                    }

                    transaction.Commit();
                }
            }
        }

        private static Dictionary<string, int> GetPeriodsForGranularity(string granularity)
        {
            // Customize periods for each indicator based on granularity
            return granularity switch
            {
                "FIVE_MINUTE" => new Dictionary<string, int>
                {
                    { "SMA", 9 },
                    { "EMA", 9 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 9 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                },
                "FIFTEEN_MINUTE" => new Dictionary<string, int>
                {
                    { "SMA", 14 },
                    { "EMA", 14 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 14 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                },
                "ONE_HOUR" => new Dictionary<string, int>
                {
                    { "SMA", 14 },
                    { "EMA", 14 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 14 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                },
                "ONE_DAY" => new Dictionary<string, int>
                {
                    { "SMA", 14 },
                    { "EMA", 14 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 14 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                },
                _ => new Dictionary<string, int>
                {
                    { "SMA", 14 },
                    { "EMA", 14 },
                    { "ATR", 14 },
                    { "RSI", 14 },
                    { "ADX", 14 },
                    { "TEMA", 14 },
                    { "BollingerBands", 20 },
                    { "StochasticOscillator", 14 },
                    { "MACD", 12 },
                    { "Supertrend", 14 },
                    { "WilliamsR", 14 },
                    { "CMF", 20 },
                    { "CCI", 20 }
                }
            };
        }
    }
}
