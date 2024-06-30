using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class PriceActionClassification
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 5)
        {
            for (int i = period; i < candles.Count; i++)
            {
                var recentCandles = candles.Skip(i - period).Take(period).ToList();
                string pattern = ClassifyPriceAction(recentCandles);

                string updateQuery = $@"
                UPDATE {tableName}
                SET PriceActionPattern = @PriceActionPattern
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@PriceActionPattern", pattern);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }

        private static string ClassifyPriceAction(List<Quote> candles)
        {
            bool isUptrend = candles.All(c => c.Close > c.Open);
            bool isDowntrend = candles.All(c => c.Close < c.Open);
            bool isRangebound = Math.Abs(candles.Last().Close - candles.First().Close) / candles.First().Close < 0.01m;

            if (isUptrend) return "Uptrend";
            if (isDowntrend) return "Downtrend";
            if (isRangebound) return "Rangebound";
            return "Mixed";
        }
    }
}
