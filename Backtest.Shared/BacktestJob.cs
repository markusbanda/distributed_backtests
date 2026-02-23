namespace Backtest.Shared;

public record BacktestJob(
    Guid BatchId, 
    Guid JobId, 
    string Symbol, 
    DateTime Start, 
    DateTime End
);

public record StrategyResult(
    Guid BatchId, 
    Guid JobId, 
    decimal TotalPnL, 
    int TradeCount
);