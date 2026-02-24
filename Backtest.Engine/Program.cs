using StackExchange.Redis;
using Backtest.Shared;
using Backtest.Engine; 
using System.Text.Json;

// 1. Connection Config
string redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";

Console.WriteLine("========================================");
Console.WriteLine("       DISTRIBUTED WORKER ENGINE        ");
Console.WriteLine("========================================");

// 2. Initialize Redis
ConnectionMultiplexer redis;
try
{
    redis = await ConnectionMultiplexer.ConnectAsync(redisConnection);
}
catch (Exception ex)
{
    Console.WriteLine($"[FATAL] Redis Connection Failed: {ex.Message}");
    return;
}

var db = redis.GetDatabase();
var strategy = new SmaStrategy();
var marketClient = new MarketDataClient();

// JITTER: Random startup delay to prevent thundering herd on Yahoo
int startDelay = new Random().Next(1000, 5000);
Console.WriteLine($"[*] Worker starting with {startDelay}ms jitter delay...");
await Task.Delay(startDelay);

Console.WriteLine("[*] Worker Active. Waiting for jobs...");

// 3. Main Loop
while (true)
{
    try
    {
        var result = await db.ListRightPopAsync("job_queue");

        if (result.HasValue)
        {
            var job = JsonSerializer.Deserialize<BacktestJob>((string)result!);

            if (job != null)
            {
                Console.WriteLine($"\n[JOB] Asset: {job.Symbol} | Range: {job.Start:yyyy-MM} to {job.End:yyyy-MM}");

                // Fetch with Polly Retries
                var data = await marketClient.GetHistoricalDataAsync(job.Symbol, job.Start, job.End);

                if (data != null && data.Count > 0)
                {
                    Console.WriteLine($"[EXE] Running strategy on {data.Count} points.");
                    strategy.Execute(data);

                    var strategyResult = new StrategyResult(
                        job.BatchId,
                        job.JobId,
                        150.75m, // Simulated PnL
                        data.Count
                    );

                    await db.ListLeftPushAsync("results_queue", JsonSerializer.Serialize(strategyResult));
                    Console.WriteLine($"[FIN] Result sent for {job.JobId}");
                }
                else
                {
                    Console.WriteLine("[WARN] Data fetch returned no results.");
                }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERR] Loop Error: {ex.Message}");
    }

    // Polling delay
    await Task.Delay(200);
}