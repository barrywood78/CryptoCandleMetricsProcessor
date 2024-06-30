using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System.Globalization;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class MarketRegimeIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 20)
        {
            var atrResults = candles.GetAtr(period).ToList();
            var smaResults = candles.GetSma(period).ToList();

            for (int i = period; i < candles.Count; i++)
            {
                if (atrResults[i].Atr.HasValue && smaResults[i].Sma.HasValue)
                {
                    decimal volatility = (decimal)(atrResults[i].Atr ?? 0) / (decimal)(smaResults[i].Sma ?? 1);
                    string regime;

                    if (volatility > 0.02m) // High volatility
                    {
                        regime = candles[i].Close > (decimal)(smaResults[i].Sma ?? 0) ? "Trending Up" : "Trending Down";
                    }

                    else if (volatility < 0.01m) // Low volatility
                    {
                        regime = "Ranging";
                    }
                    else // Medium volatility
                    {
                        regime = "Transitioning";
                    }

                    string updateQuery = $@"
                    UPDATE {tableName}
                    SET MarketRegime = @MarketRegime,
                        MarketVolatility = @MarketVolatility
                    WHERE ProductId = @ProductId
                      AND Granularity = @Granularity
                      AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@MarketRegime", regime);
                        command.Parameters.AddWithValue("@MarketVolatility", volatility);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}