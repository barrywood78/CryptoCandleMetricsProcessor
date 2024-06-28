using Skender.Stock.Indicators;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;
using System.Linq;
using MathNet.Numerics.Statistics;

namespace CryptoCandleMetricsProcessor.Analysis.Indicators
{
    public static class StatisticalIndicators
    {
        /// <summary>
        /// Calculates various statistical indicators and updates the database.
        /// </summary>
        /// <param name="connection">The SQLite database connection.</param>
        /// <param name="transaction">The SQLite transaction for atomic updates.</param>
        /// <param name="tableName">The name of the table to update.</param>
        /// <param name="productId">The product ID to filter the data.</param>
        /// <param name="granularity">The granularity to filter the data.</param>
        /// <param name="candles">The list of candle data to process.</param>
        /// <param name="period">The period to use for the statistical calculations.</param>
        public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate Rolling Mean
            CalculateRollingMean(connection, transaction, tableName, productId, granularity, candles, period);

            // Calculate Rolling Standard Deviation
            CalculateRollingStdDev(connection, transaction, tableName, productId, granularity, candles, period);

            // Calculate Rolling Variance
            CalculateRollingVariance(connection, transaction, tableName, productId, granularity, candles, period);

            // Calculate Rolling Skewness
            CalculateRollingSkewness(connection, transaction, tableName, productId, granularity, candles, period);

            // Calculate Rolling Kurtosis
            CalculateRollingKurtosis(connection, transaction, tableName, productId, granularity, candles, period);
        }

        private static void CalculateRollingMean(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate rolling means over the specified period
            var rollingMeans = candles.Select((c, i) => new
            {
                c.Date,
                RollingMean = i >= period - 1 ? (decimal?)candles.Skip(i - period + 1).Take(period).Select(q => q.Close).Average() : null
            }).ToList();

            // Update the database with the rolling means
            UpdateDatabase(connection, transaction, tableName, productId, granularity, rollingMeans, "RollingMean");
        }

        private static void CalculateRollingStdDev(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate rolling standard deviations over the specified period
            var rollingStdDevs = candles.Select((c, i) => new
            {
                c.Date,
                RollingStdDev = i >= period - 1 ? (double?)candles.Skip(i - period + 1).Take(period).Select(q => (double)q.Close).StandardDeviation() : null
            }).ToList();

            // Update the database with the rolling standard deviations
            UpdateDatabase(connection, transaction, tableName, productId, granularity, rollingStdDevs, "RollingStdDev");
        }

        private static void CalculateRollingVariance(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate rolling variances over the specified period
            var rollingVariances = candles.Select((c, i) => new
            {
                c.Date,
                RollingVariance = i >= period - 1 ? (double?)candles.Skip(i - period + 1).Take(period).Select(q => (double)q.Close).Variance() : null
            }).ToList();

            // Update the database with the rolling variances
            UpdateDatabase(connection, transaction, tableName, productId, granularity, rollingVariances, "RollingVariance");
        }

        private static void CalculateRollingSkewness(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate rolling skewness over the specified period
            var rollingSkewness = candles.Select((c, i) => new
            {
                c.Date,
                RollingSkewness = i >= period - 1 ? (double?)candles.Skip(i - period + 1).Take(period).Select(q => (double)q.Close).Skewness() : null
            }).ToList();

            // Update the database with the rolling skewness
            UpdateDatabase(connection, transaction, tableName, productId, granularity, rollingSkewness, "RollingSkewness");
        }

        private static void CalculateRollingKurtosis(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles, int period)
        {
            // Calculate rolling kurtosis over the specified period
            var rollingKurtosis = candles.Select((c, i) => new
            {
                c.Date,
                RollingKurtosis = i >= period - 1 ? (double?)candles.Skip(i - period + 1).Take(period).Select(q => (double)q.Close).Kurtosis() : null
            }).ToList();

            // Update the database with the rolling kurtosis
            UpdateDatabase(connection, transaction, tableName, productId, granularity, rollingKurtosis, "RollingKurtosis");
        }

        private static void UpdateDatabase<T>(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<T> results, string columnName)
        {
            // Iterate through the results and update the database
            foreach (var result in results)
            {
                if (result != null)
                {
                    var resultType = result.GetType();
                    var date = (DateTime?)resultType.GetProperty("Date")?.GetValue(result, null) ?? default(DateTime);
                    var value = resultType.GetProperty(columnName)?.GetValue(result, null);

                    if (value != null)
                    {
                        bool isNaN = false;
                        if (value is double doubleValue)
                        {
                            isNaN = double.IsNaN(doubleValue);
                        }

                        if (!isNaN)
                        {
                            string updateQuery = $@"
                                UPDATE {tableName}
                                SET {columnName} = @{columnName}
                                WHERE ProductId = @ProductId
                                  AND Granularity = @Granularity
                                  AND StartDate = @StartDate";

                            using (var command = new SqliteCommand(updateQuery, connection, transaction))
                            {
                                // Add parameters to the update command
                                command.Parameters.AddWithValue($"@{columnName}", value);
                                command.Parameters.AddWithValue("@ProductId", productId);
                                command.Parameters.AddWithValue("@Granularity", granularity);
                                command.Parameters.AddWithValue("@StartDate", date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

                                // Execute the update command
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }
        }
    }
}
