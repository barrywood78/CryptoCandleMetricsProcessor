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
    public static class OrderFlowImbalanceIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period = 14)
        {
            for (int i = period; i < candles.Count; i++)
            {
                decimal buyingPressure = 0;
                decimal sellingPressure = 0;

                for (int j = i - period + 1; j <= i; j++)
                {
                    if (candles[j].Close > candles[j].Open)
                    {
                        buyingPressure += candles[j].Volume * (candles[j].Close - candles[j].Open) / candles[j].Open;
                    }
                    else if (candles[j].Close < candles[j].Open)
                    {
                        sellingPressure += candles[j].Volume * (candles[j].Open - candles[j].Close) / candles[j].Open;
                    }
                }

                decimal imbalance = (buyingPressure - sellingPressure) / (buyingPressure + sellingPressure);

                string updateQuery = $@"
                UPDATE {tableName}
                SET OrderFlowImbalance = @OrderFlowImbalance
                WHERE ProductId = @ProductId
                  AND Granularity = @Granularity
                  AND StartDate = @StartDate";

                using (var command = new SqliteCommand(updateQuery, connection, transaction))
                {
                    command.Parameters.AddWithValue("@OrderFlowImbalance", imbalance);
                    command.Parameters.AddWithValue("@ProductId", productId);
                    command.Parameters.AddWithValue("@Granularity", granularity);
                    command.Parameters.AddWithValue("@StartDate", candles[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
