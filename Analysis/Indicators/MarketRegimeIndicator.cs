using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class MarketRegimeIndicator
    {
        private const int BatchSize = 50000; // Optimal batch size for local SQLite operations

        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 20)
        {
            var atrResults = candles.GetAtr(period).ToList();
            var smaResults = candles.GetSma(period).ToList();

            var results = new List<(long DateTicks, string Regime, decimal Volatility)>();

            for (int i = period; i < candles.Count; i++)
            {
                if (atrResults[i].Atr.HasValue && smaResults[i].Sma.HasValue)
                {
                    decimal volatility = (decimal)(atrResults[i].Atr ?? 0) / (decimal)(smaResults[i].Sma ?? 1);
                    string regime = DetermineRegime(volatility, candles[i].Close, (decimal)(smaResults[i].Sma ?? 0));

                    results.Add((candles[i].Date.Ticks, regime, volatility));
                }
            }

            UpdateMarketRegimes(connection, transaction, tableName, productId, granularity, results);
        }

        private static string DetermineRegime(decimal volatility, decimal closePrice, decimal sma)
        {
            if (volatility > 0.02m) // High volatility
            {
                return closePrice > sma ? "Trending Up" : "Trending Down";
            }
            else if (volatility < 0.01m) // Low volatility
            {
                return "Ranging";
            }
            else // Medium volatility
            {
                return "Transitioning";
            }
        }

        private static void UpdateMarketRegimes(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<(long DateTicks, string Regime, decimal Volatility)> results)
        {
            string updateQuery = $@"
                UPDATE {tableName}
                SET MarketRegime = @MarketRegime,
                    MarketVolatility = @MarketVolatility
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

            using var command = new SqliteCommand(updateQuery, connection, transaction);
            command.Parameters.Add("@MarketRegime", SqliteType.Text);
            command.Parameters.Add("@MarketVolatility", SqliteType.Real);
            command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
            command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
            command.Parameters.Add("@StartDate", SqliteType.Integer);

            foreach (var batch in results.Chunk(BatchSize))
            {
                foreach (var (dateTicks, regime, volatility) in batch)
                {
                    command.Parameters["@MarketRegime"].Value = regime;
                    command.Parameters["@MarketVolatility"].Value = (double)volatility;
                    command.Parameters["@StartDate"].Value = dateTicks;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}