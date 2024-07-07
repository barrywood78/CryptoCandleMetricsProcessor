using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class OscillatorDivergencesIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int rsiPeriod = 14, int macdFastPeriod = 12, int macdSlowPeriod = 26, int macdSignalPeriod = 9)
        {
            var rsiResults = candles.GetRsi(rsiPeriod).ToList();
            var macdResults = candles.GetMacd(macdFastPeriod, macdSlowPeriod, macdSignalPeriod).ToList();

            var results = new List<(long DateTicks, bool RSIDivergence, bool MACDDivergence)>();

            int startIndex = Math.Max(rsiPeriod, Math.Max(macdFastPeriod, macdSlowPeriod));

            for (int i = startIndex; i < candles.Count; i++)
            {
                bool rsiDivergence = false;
                bool macdDivergence = false;

                if (rsiResults[i].Rsi.HasValue && rsiResults[i - 1].Rsi.HasValue)
                {
                    rsiDivergence = IsOscillatorDivergence(
                        candles[i].Close,
                        candles[i - 1].Close,
                        rsiResults[i].Rsi!.Value,
                        rsiResults[i - 1].Rsi!.Value);
                }

                if (macdResults[i].Macd.HasValue && macdResults[i - 1].Macd.HasValue)
                {
                    macdDivergence = IsOscillatorDivergence(
                        candles[i].Close,
                        candles[i - 1].Close,
                        macdResults[i].Macd!.Value,
                        macdResults[i - 1].Macd!.Value);
                }

                results.Add((candles[i].Date.Ticks, rsiDivergence, macdDivergence));
            }

            UpdateOscillatorDivergences(connection, transaction, tableName, productId, granularity, results);
        }

        private static bool IsOscillatorDivergence(decimal currentPrice, decimal previousPrice, double currentOscillator, double previousOscillator)
        {
            bool priceHigher = currentPrice > previousPrice;
            bool oscillatorHigher = currentOscillator > previousOscillator;
            return priceHigher != oscillatorHigher;
        }

        private static void UpdateOscillatorDivergences(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, bool RSIDivergence, bool MACDDivergence)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET RSIDivergence = @RSIDivergence,
                    MACDDivergence = @MACDDivergence
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@RSIDivergence", SqliteType.Integer);
            command.Parameters.Add("@MACDDivergence", SqliteType.Integer);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, rsiDivergence, macdDivergence) in batch)
                {
                    command.Parameters["@RSIDivergence"].Value = rsiDivergence ? 1 : 0;
                    command.Parameters["@MACDDivergence"].Value = macdDivergence ? 1 : 0;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}