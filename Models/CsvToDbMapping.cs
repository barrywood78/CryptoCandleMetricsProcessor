namespace CryptoCandleMetricsProcessor.Models
{
    /// <summary>
    /// Represents a mapping from a CSV column to a database field.
    /// </summary>
    public class CsvToDbMapping
    {
        /// <summary>
        /// Gets or sets the index of the column in the CSV file.
        /// </summary>
        public int CsvColumnIndex { get; set; }

        /// <summary>
        /// Gets or sets the name of the corresponding field in the database.
        /// </summary>
        public string? DbFieldName { get; set; }
    }
}
