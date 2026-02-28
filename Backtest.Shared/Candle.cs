namespace Backtest.Shared;

public struct Candle
{
    public DateTime DateTime { get; set; }
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
    public decimal Volume { get; set; }

    // Helper for APIs that return Unix seconds
    public static DateTime FromUnixTimestamp(long unixSeconds) => 
        DateTimeOffset.FromUnixTimeSeconds(unixSeconds).DateTime;
}