using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class IchimokuIndicator
    {
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
        {
            var ichimokuResults = candles.GetIchimoku().ToList();

            for (int i = 0; i < ichimokuResults.Count; i++)
            {
                if (ichimokuResults[i] != null)
                {
                    string updateQuery = $@"
                        UPDATE {tableName}
                        SET Ichimoku_TenkanSen = @TenkanSen,
                            Ichimoku_KijunSen = @KijunSen,
                            Ichimoku_SenkouSpanA = @SenkouSpanA,
                            Ichimoku_SenkouSpanB = @SenkouSpanB,
                            Ichimoku_ChikouSpan = @ChikouSpan
                        WHERE ProductId = @ProductId
                          AND Granularity = @Granularity
                          AND StartDate = @StartDate";

                    using (var command = new SqliteCommand(updateQuery, connection, transaction))
                    {
                        command.Parameters.AddWithValue("@TenkanSen", ichimokuResults[i].TenkanSen ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@KijunSen", ichimokuResults[i].KijunSen ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@SenkouSpanA", ichimokuResults[i].SenkouSpanA ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@SenkouSpanB", ichimokuResults[i].SenkouSpanB ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@ChikouSpan", ichimokuResults[i].ChikouSpan ?? (object)DBNull.Value);
                        command.Parameters.AddWithValue("@ProductId", productId);
                        command.Parameters.AddWithValue("@Granularity", granularity);
                        command.Parameters.AddWithValue("@StartDate", ichimokuResults[i].Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                        command.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
