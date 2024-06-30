using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class OscillatorDivergencesIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int rsiPeriod = 14, int macdFastPeriod = 12, int macdSlowPeriod = 26, int macdSignalPeriod = 9)
        {
            var rsiResults = candles.GetRsi(rsiPeriod).ToList();
            var macdResults = candles.GetMacd(macdFastPeriod, macdSlowPeriod, macdSignalPeriod).ToList();

            for (int i = 1; i < candles.Count; i++)
            {
                bool rsiDivergence = false;
                bool macdDivergence = false;

                if (i >= rsiPeriod && rsiResults[i].Rsi.HasValue && rsiResults[i - 1].Rsi.HasValue)
                {
                    rsiDivergence = IsOscillatorDivergence(
                        candles[i].Close,
                        candles[i - 1].Close,
                        rsiResults[i].Rsi ?? 0,
                        rsiResults[i - 1].Rsi ?? 0);
                }

                if (i >= Math.Max(macdFastPeriod, macdSlowPeriod) && macdResults[i].Macd.HasValue && macdResults[i - 1].Macd.HasValue)
                {
                    macdDivergence = IsOscillatorDivergence(
                        candles[i].Close,
                        candles[i - 1].Close,
                        macdResults[i].Macd ?? 0,
                        macdResults[i - 1].Macd ?? 0);
                }


                string updateQuery = $@"
                    UPDATE {tableName}
                    SET RSIDivergence = @RSIDivergence,
                        MACDDivergence = @MACDDivergence
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@RSIDivergence", rsiDivergence);
                    command.Parameters.AddWithValue("@MACDDivergence", macdDivergence);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }

        private static bool IsOscillatorDivergence(decimal currentPrice, decimal previousPrice, double currentOscillator, double previousOscillator)
        {
            bool priceHigher = currentPrice > previousPrice;
            bool oscillatorHigher = currentOscillator > previousOscillator;

            return priceHigher != oscillatorHigher;
        }
    }
}