namespace CryptoCandleMetricsProcessor.Models
{
    public class CsvToDbMapping
    {
        public int CsvColumnIndex { get; set; }
        public string? DbFieldName { get; set; }
    }
}
