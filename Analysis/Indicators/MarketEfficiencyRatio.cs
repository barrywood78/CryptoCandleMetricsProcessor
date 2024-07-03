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
    public static class MarketEfficiencyRatio
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            for (int i = period; i < candles.Count; i++)
            {
                decimal netPriceChange = Math.Abs(candles[i].Close - candles[i - period].Close);
                decimal sumPriceChanges = 0;
                for (int j = i - period + 1; j <= i; j++)
                {
                    sumPriceChanges += Math.Abs(candles[j].Close - candles[j - 1].Close);
                }

                decimal mer;
                if (sumPriceChanges == 0)
                {
                    // If sumPriceChanges is zero, it means all prices are identical
                    // In this case, we can consider the market to be perfectly efficient
                    mer = 1;
                }
                else
                {
                    mer = netPriceChange / sumPriceChanges;
                }

                string updateQuery = $@"
                UPDATE {tableName}
                SET MarketEfficiencyRatio = @MarketEfficiencyRatio
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@MarketEfficiencyRatio", mer);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}