namespace Backtest.Shared;

// We use a 'struct' instead of a 'class' for performance. 
// Structs are stored on the Stack, which avoids Garbage Collection (GC) overhead.
public struct Candle
{
    public DateTime DateTime { get; set; }
    public decimal Open { get; set; }
    public decimal High { get; set; }
    public decimal Low { get; set; }
    public decimal Close { get; set; }
    public decimal Volume { get; set; }
}