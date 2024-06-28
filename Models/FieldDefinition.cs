namespace CryptoCandleMetricsProcessor.Models
{
    /// <summary>
    /// Represents a definition for a field in a database table.
    /// </summary>
    public class FieldDefinition
    {
        /// <summary>
        /// Gets or sets the name of the field.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the data type of the field.
        /// </summary>
        public string? DataType { get; set; }
    }
}
