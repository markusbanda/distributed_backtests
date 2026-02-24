using StackExchange.Redis;
using Backtest.Shared;
using System.Text.Json;

namespace Backtest.Orchestrator;

public class ResultAggregator(IDatabase db)
{
    private readonly IDatabase _db = db;

    public async Task MonitorResultsAsync(Guid batchId, int totalJobs)
    {
        Console.WriteLine($"[Aggregator] Monitoring Batch {batchId}. Expecting {totalJobs} results...");

        int receivedCount = 0;
        decimal cumulativePnL = 0;

        while (receivedCount < totalJobs)
        {
            // Use RPop to get the oldest result first
            var resultData = await _db.ListRightPopAsync("results_queue");

            if (resultData.HasValue)
            {
                var result = JsonSerializer.Deserialize<StrategyResult>((string)resultData!);

                // Important: Only count results that belong to OUR current batch
                if (result != null && result.BatchId == batchId)
                {
                    receivedCount++;
                    cumulativePnL += result.TotalPnL;

                    Console.WriteLine($"[Progress] {receivedCount}/{totalJobs} received. (Latest PnL: ${result.TotalPnL})");

                    if (receivedCount == totalJobs)
                    {
                        PrintFinalReport(batchId, cumulativePnL, totalJobs);
                    }
                }
                else if (result != null)
                {
                    // If we find a result from an old batch, we don't want to lose it, 
                    // but for this simple version, we'll just ignore it or log it.
                    Console.WriteLine($"[Aggregator] Ignored result from different batch: {result.BatchId}");
                }
            }

            // Low-latency polling delay
            await Task.Delay(100);
        }
    }

    private void PrintFinalReport(Guid batchId, decimal totalPnL, int count)
    {
        Console.WriteLine("\n========================================");
        Console.WriteLine("        FINAL BACKTEST REPORT           ");
        Console.WriteLine("========================================");
        Console.WriteLine($" Batch ID:    {batchId}");
        Console.WriteLine($" Total Jobs:  {count}");
        Console.WriteLine($" Total PnL:   ${totalPnL:N2}");
        Console.WriteLine($" Avg PnL:     ${(totalPnL / count):N2}");
        Console.WriteLine("========================================\n");
    }
}