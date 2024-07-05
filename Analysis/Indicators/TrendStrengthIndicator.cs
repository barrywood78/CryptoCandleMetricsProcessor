using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class TrendStrengthIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int adxPeriod = 14, int smaPeriod = 50)
        {
            var adxResults = candles.GetAdx(adxPeriod).ToList();
            var smaResults = candles.GetSma(smaPeriod).ToList();

            var results = new List<(long DateTicks, decimal TrendStrength, int TrendDuration, bool IsUptrend)>();

            int trendDuration = 0;
            bool isUptrend = false;
            int startIndex = Math.Max(adxPeriod, smaPeriod) - 1;

            for (int i = startIndex; i < candles.Count; i++)
            {
                if (adxResults[i].Adx.HasValue && smaResults[i].Sma.HasValue)
                {
                    decimal adxValue = (decimal)(adxResults[i].Adx ?? 0);
                    decimal smaValue = (decimal)(smaResults[i].Sma ?? 1);
                    decimal priceToSma = candles[i].Close / smaValue;
                    decimal trendStrength = adxValue * (priceToSma > 1 ? priceToSma : 1 / priceToSma);

                    bool currentIsUptrend = candles[i].Close > smaValue;
                    if (currentIsUptrend != isUptrend)
                    {
                        trendDuration = 1;
                        isUptrend = currentIsUptrend;
                    }
                    else
                    {
                        trendDuration++;
                    }

                    results.Add((candles[i].Date.Ticks, trendStrength, trendDuration, isUptrend));
                }
            }

            UpdateTrendStrengthIndicators(connection, transaction, tableName, productId, granularity, results);
        }

        private static void UpdateTrendStrengthIndicators(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, decimal TrendStrength, int TrendDuration, bool IsUptrend)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET TrendStrength = @TrendStrength,
                    TrendDuration = @TrendDuration,
                    IsUptrend = @IsUptrend
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@TrendStrength", SqliteType.Real);
            command.Parameters.Add("@TrendDuration", SqliteType.Integer);
            command.Parameters.Add("@IsUptrend", SqliteType.Integer);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, trendStrength, trendDuration, isUptrend) in batch)
                {
                    command.Parameters["@TrendStrength"].Value = (double)trendStrength;
                    command.Parameters["@TrendDuration"].Value = trendDuration;
                    command.Parameters["@IsUptrend"].Value = isUptrend ? 1 : 0;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}