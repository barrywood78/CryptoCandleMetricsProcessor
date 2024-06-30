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
    public static class VolatilityRegime
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 20)
        {
            var atrResults = candles.GetAtr(period).ToList();

            for (int i = period; i < candles.Count; i++)
            {
                var recentATRs = atrResults.Skip(i - period).Take(period).Select(r => r.Atr ?? 0).ToList();

                double meanATR = recentATRs.Average();
                double stdDevATR = Math.Sqrt(recentATRs.Select(x => Math.Pow(x - meanATR, 2)).Sum() / period);

                string regime;
                if ((atrResults[i].Atr ?? 0) > meanATR + stdDevATR) regime = "High";
                else if ((atrResults[i].Atr ?? 0) < meanATR - stdDevATR) regime = "Low";
                else regime = "Medium";


                string updateQuery = $@"
                UPDATE {tableName}
                SET VolatilityRegime = @VolatilityRegime
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@VolatilityRegime", regime);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
