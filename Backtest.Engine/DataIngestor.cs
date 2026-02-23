using System.IO.Pipelines;
using Backtest.Shared;
using Sylvan.Data.Csv;
using System.IO;

public class DataIngestor
{
    public async Task<List<Candle>> ReadCsvAsync(string filePath)
    {
        var candles = new List<Candle>();

        // 1. Open the file as a Stream
        using var stream = File.OpenRead(filePath);
        
        // 2. Wrap the stream in a StreamReader (Sylvan needs a TextReader or a file path)
        using var reader = new StreamReader(stream);

        // 3. Create the CsvDataReader using the TextReader
        var csvOptions = new CsvDataReaderOptions { HasHeaders = true };
        using var csvReader = await CsvDataReader.CreateAsync(reader, csvOptions);

        while (await csvReader.ReadAsync())
        {
            // Sylvan is zero-allocation here, very fast!
            candles.Add(new Candle
            {
                DateTime = csvReader.GetDateTime(0),
                Open = csvReader.GetDecimal(1),
                High = csvReader.GetDecimal(2),
                Low = csvReader.GetDecimal(3),
                Close = csvReader.GetDecimal(4),
                Volume = csvReader.GetDecimal(5)
            });
        }

        return candles;
    }
}