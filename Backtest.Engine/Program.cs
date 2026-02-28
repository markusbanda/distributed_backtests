using StackExchange.Redis;
using Backtest.Shared;
using Backtest.Engine; 
using System.Text.Json;
using System.IO;

// 1. Connection Config
string redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";

Console.WriteLine("========================================");
Console.WriteLine("       DISTRIBUTED WORKER ENGINE        ");
Console.WriteLine("========================================");
Console.WriteLine($"[*] Target Environment: {(Environment.GetEnvironmentVariable("REDIS_CONNECTION") != null ? "Docker" : "Local")}");
Console.WriteLine($"[*] Connecting to Redis at: {redisConnection}");

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

// 3. Initialize Strategy and CSV Ingestor
var strategy = new EmaStrategy{ Period = 14};
//var strategy = new SmaStrategy();
var ingestor = new DataIngestor();

// Resolve the path to the CSV file ensuring it works locally and in Docker
var baseDir = AppContext.BaseDirectory;
var filePath = Path.Combine(baseDir, "market_data.csv");

// JITTER: Random startup delay to prevent all workers hitting Redis at the exact same time
int startDelay = new Random().Next(500, 2000);
Console.WriteLine($"[*] Worker starting with {startDelay}ms jitter delay...");
await Task.Delay(startDelay);

Console.WriteLine("[*] Worker Active. Waiting for jobs...");

// 4. Main Loop
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
                Console.WriteLine("\n------------------------------------------------");
                Console.WriteLine($"[JOB START] ID:    {job.JobId}");
                Console.WriteLine($"[JOB START] Range: {job.Start:yyyy-MM-dd} to {job.End:yyyy-MM-dd}");
                Console.WriteLine("------------------------------------------------");

                var data = await ingestor.ReadCsvAsync(filePath, job.Symbol, job.Start, job.End);

                decimal pnl = 0m;
                int count = 0;

                if (data != null && data.Count > 0)
                {
                    Console.WriteLine($"[EXE] Running strategy on {data.Count} points.");
                    strategy.Execute(data);
                    
                    pnl = 150.75m; // Your simulated PnL
                    count = data.Count;
                }
                else
                {
                    Console.WriteLine($"[WARNING] No data found in CSV for {job.Symbol} within range.");
                }

                // FIX: Always send a result, even if PnL and count are 0
                var strategyResult = new StrategyResult(
                    job.BatchId,
                    job.JobId,
                    pnl,
                    count
                );

                await db.ListLeftPushAsync("results_queue", JsonSerializer.Serialize(strategyResult));
                Console.WriteLine($"[REPORT] Completion sent for {job.JobId} (Count: {count})");
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERR] Loop Error: {ex.Message}");
    }

    await Task.Delay(200);
}