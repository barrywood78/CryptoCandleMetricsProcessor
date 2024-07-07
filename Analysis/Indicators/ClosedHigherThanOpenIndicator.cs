using Microsoft.Data.Sqlite;
using Skender.Stock.Indicators;

public static class ClosedHigherThanOpenIndicator
{
    private const int BatchSize = 50000;

    /// <summary>
    /// Calculates whether the closing price is higher (1) or not (0) than the opening price for each candle and updates the database.
    /// </summary>
    /// <param name="connection">The SQLite database connection.</param>
    /// <param name="transaction">The SQLite transaction for atomic updates.</param>
    /// <param name="tableName">The name of the table to update.</param>
    /// <param name="productId">The product ID to filter the data.</param>
    /// <param name="granularity">The granularity to filter the data.</param>
    /// <param name="candles">The list of candle data to process.</param>
    public static void Calculate(SqliteConnection connection, SqliteTransaction transaction, string tableName, string productId, string granularity, List<Quote> candles)
    {
        var closedHigherThanOpenResults = new List<(long DateTicks, int ClosedHigherThanOpen)>();
        foreach (var candle in candles)
        {
            int closedHigherThanOpen = candle.Close > candle.Open ? 1 : 0;
            closedHigherThanOpenResults.Add((candle.Date.Ticks, closedHigherThanOpen));
        }

        string updateQuery = $@"
            UPDATE {tableName}
            SET ClosedHigherThanOpen = @ClosedHigherThanOpen
            WHERE ProductId = @ProductId
              AND Granularity = @Granularity
              AND StartDate = datetime(@StartDate / 10000000 - 62135596800, 'unixepoch')";

        using var command = new SqliteCommand(updateQuery, connection, transaction);
        command.Parameters.Add("@ClosedHigherThanOpen", SqliteType.Integer);
        command.Parameters.Add("@ProductId", SqliteType.Text).Value = productId;
        command.Parameters.Add("@Granularity", SqliteType.Text).Value = granularity;
        command.Parameters.Add("@StartDate", SqliteType.Integer);

        foreach (var batch in closedHigherThanOpenResults.Chunk(BatchSize))
        {
            foreach (var result in batch)
            {
                command.Parameters["@ClosedHigherThanOpen"].Value = result.ClosedHigherThanOpen;
                command.Parameters["@StartDate"].Value = result.DateTicks;
                command.ExecuteNonQuery();
            }
        }
    }
}