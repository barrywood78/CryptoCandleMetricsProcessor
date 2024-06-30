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
    public static class CompositeMomentumIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int rsiPeriod = 14, int macdFastPeriod = 12, int macdSlowPeriod = 26, int macdSignalPeriod = 9)
        {
            var rsiResults = candles.GetRsi(rsiPeriod).ToList();
            var macdResults = candles.GetMacd(macdFastPeriod, macdSlowPeriod, macdSignalPeriod).ToList();
            var rocResults = candles.GetRoc(14).ToList();

            for (int i = Math.Max(rsiPeriod, Math.Max(macdSlowPeriod, 14)); i < candles.Count; i++)
            {
                decimal rsiComponent = (decimal)((rsiResults[i].Rsi ?? 0) - 50) / 50;
                decimal macdComponent = (decimal)(macdResults[i].Macd ?? 0) / (decimal)candles[i].Close;
                decimal rocComponent = (decimal)((rocResults[i].Roc ?? 0) / 100);

                decimal compositeMomentum = (rsiComponent + macdComponent + rocComponent) / 3;

                string updateQuery = $@"
                UPDATE {tableName}
                SET CompositeMomentum = @CompositeMomentum
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@CompositeMomentum", compositeMomentum);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
