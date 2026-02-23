using Backtest.Shared;

public class SmaStrategy
{
    // High-performance tip: Use Span for window-based calculations
    // This allows us to look at a "slice" of data without copying it.
    public decimal CalculateSma(ReadOnlySpan<Candle> window)
    {
        decimal sum = 0;
        foreach (var candle in window)
        {
            sum += candle.Close;
        }
        return sum / window.Length;
    }

    public void Execute(List<Candle> data)
    {
        int period = 10;
        // Convert List to Span for faster access
        ReadOnlySpan<Candle> dataSpan = data.ToArray();

        Console.WriteLine($"Starting backtest on {dataSpan.Length} candles...");

        for (int i = period; i < dataSpan.Length; i++)
        {
            // Create a "slice" of the last 10 candles
            var window = dataSpan.Slice(i - period, period);
            decimal sma = CalculateSma(window);
            decimal currentPrice = dataSpan[i].Close;

            if (currentPrice > sma)
            {
                // Logic: Buy Signal (We'll just log for now)
                // Console.WriteLine($"[{dataSpan[i].DateTime}] BUY at {currentPrice}");
            }
        }
    }
}