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
    public static class RSIDivergenceStrengthIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            var rsiResults = candles.GetRsi(period).ToList();

            for (int i = period; i < candles.Count; i++)
            {
                decimal priceChange = (candles[i].Close - candles[i - period].Close) / candles[i - period].Close;
                double rsiChange = (rsiResults[i].Rsi ?? 0) - (rsiResults[i - period].Rsi ?? 0);


                decimal divergenceStrength = (decimal)(Math.Sign(priceChange) != Math.Sign(rsiChange) ? Math.Abs(priceChange - (decimal)rsiChange) : 0);

                string updateQuery = $@"
                UPDATE {tableName}
                SET RSIDivergenceStrength = @RSIDivergenceStrength
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@RSIDivergenceStrength", divergenceStrength);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
