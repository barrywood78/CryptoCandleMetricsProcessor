using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System.Globalization;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class CrossoverFeaturesIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var macdResults = candles.GetMacd().ToList();
            var emaShortResults = candles.GetEma(12).ToList();
            var emaLongResults = candles.GetEma(26).ToList();

            for (int i = 1; i < candles.Count; i++)
            {
                bool macdCrossover = macdResults[i].Macd > macdResults[i].Signal && macdResults[i - 1].Macd <= macdResults[i - 1].Signal ||
                                     macdResults[i].Macd < macdResults[i].Signal && macdResults[i - 1].Macd >= macdResults[i - 1].Signal;

                bool emaCrossover = emaShortResults[i].Ema > emaLongResults[i].Ema && emaShortResults[i - 1].Ema <= emaLongResults[i - 1].Ema ||
                                    emaShortResults[i].Ema < emaLongResults[i].Ema && emaShortResults[i - 1].Ema >= emaLongResults[i - 1].Ema;

                string updateQuery = $@"
                UPDATE {tableName}
                SET MACDCrossover = @MACDCrossover,
                    EMACrossover = @EMACrossover
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@MACDCrossover", macdCrossover);
                    command.Parameters.AddWithValue("@EMACrossover", emaCrossover);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}