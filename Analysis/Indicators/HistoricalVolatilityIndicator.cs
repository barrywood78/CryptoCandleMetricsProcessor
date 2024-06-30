using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class HistoricalVolatilityIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 20)
        {
            for (int i = period; i < candles.Count; i++)
            {
                var subset = candles.Skip(i - period).Take(period).ToList();
                double[] returns = new double[period - 1];

                for (int j = 1; j < period; j++)
                {
                    returns[j - 1] = Math.Log((double)(subset[j].Close / subset[j - 1].Close));
                }

                double meanReturn = returns.Average();
                double sumSquaredDeviations = returns.Sum(r => Math.Pow(r - meanReturn, 2));
                double standardDeviation = Math.Sqrt(sumSquaredDeviations / (period - 1));
                double annualizedVolatility = standardDeviation * Math.Sqrt(252); // Assuming 252 trading days in a year

                string updateQuery = $@"
                    UPDATE {tableName}
                    SET HistoricalVolatility = @HistoricalVolatility
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@HistoricalVolatility", annualizedVolatility);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}