using Backtest.Shared;
using Sylvan.Data.Csv;
using System.IO;

namespace Backtest.Engine;

public class DataIngestor
{
    public async Task<List<Candle>> ReadCsvAsync(string filePath, string targetSymbol, DateTime start, DateTime end)
    {
        var candles = new List<Candle>();

        if (!File.Exists(filePath))
        {
            Console.WriteLine($"[Error] Data file not found at: {filePath}");
            return candles;
        }

        using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream);
        
        var csvOptions = new CsvDataReaderOptions { HasHeaders = true };
        using var csvReader = await CsvDataReader.CreateAsync(reader, csvOptions);

        while (await csvReader.ReadAsync())
        {
            // Expected Format: Symbol, DateTime, Open, High, Low, Close, Volume
            string symbol = csvReader.GetString(0);
            DateTime date = csvReader.GetDateTime(1);

            // Only load data if it matches the job's Symbol AND falls within the job's date range
            if (symbol.Equals(targetSymbol, StringComparison.OrdinalIgnoreCase) && date >= start && date < end)
            {
                candles.Add(new Candle
                {
                    DateTime = date,
                    Open = csvReader.GetDecimal(2),
                    High = csvReader.GetDecimal(3),
                    Low = csvReader.GetDecimal(4),
                    Close = csvReader.GetDecimal(5),
                    Volume = csvReader.GetDecimal(6)
                });
            }
        }

        return candles;
    }
}