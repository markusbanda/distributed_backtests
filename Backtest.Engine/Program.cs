using StackExchange.Redis;
using Backtest.Shared;
using Backtest.Engine; 
using System.Text.Json;
using System.IO;

// 1. Setup Connection Logic
// We pull the Redis connection string from Environment Variables for Docker compatibility.
// If not found (e.g., running locally without Docker), it defaults to "localhost".
string redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";

Console.WriteLine("========================================");
Console.WriteLine("       DISTRIBUTED WORKER ENGINE        ");
Console.WriteLine("========================================");
Console.WriteLine($"[*] Target Environment: {(Environment.GetEnvironmentVariable("REDIS_CONNECTION") != null ? "Docker" : "Local")}");
Console.WriteLine($"[*] Connecting to Redis at: {redisConnection}");

// 2. Initialize Redis Connection
ConnectionMultiplexer redis;
try
{
    redis = await ConnectionMultiplexer.ConnectAsync(redisConnection);
}
catch (Exception ex)
{
    Console.WriteLine($"[FATAL] Could not connect to Redis: {ex.Message}");
    return;
}

var db = redis.GetDatabase();

// 3. Initialize Strategy and CSV Ingestor
var strategy = new SmaStrategy();
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
        // 'ListRightPopAsync' blocks until a job is available.
        // This is a common pattern for distributed task queues.
        var result = await db.ListRightPopAsync("job_queue");

        if (result.HasValue)
        {
            // Deserialize the job from Redis (sent by the Orchestrator)
            var job = JsonSerializer.Deserialize<BacktestJob>((string)result!);

            if (job != null)
            {
                Console.WriteLine("\n------------------------------------------------");
                Console.WriteLine($"[JOB START] Batch: {job.BatchId}");
                Console.WriteLine($"[JOB START] ID:    {job.JobId}");
                Console.WriteLine($"[JOB START] Asset: {job.Symbol}");
                Console.WriteLine($"[JOB START] Range: {job.Start:yyyy-MM-dd} to {job.End:yyyy-MM-dd}");
                Console.WriteLine("------------------------------------------------");

                // A. Fetch Market Data from local CSV matching the job parameters
                Console.WriteLine($"[FETCH] Reading local CSV data for {job.Symbol}...");
                var data = await ingestor.ReadCsvAsync(filePath, job.Symbol, job.Start, job.End);

                if (data != null && data.Count > 0)
                {
                    Console.WriteLine($"[EXE] Running strategy on {data.Count} points.");
                    
                    // B. Execute Strategy
                    strategy.Execute(data);

                    // C. Report Results

                    var strategyResult = new StrategyResult(
                        job.BatchId,
                        job.JobId,
                        245.50m, // Simulated Total PnL for this segment
                        data.Count // Total trades/data points processed
                    );

                    await Task.Delay(10000);
                    await db.ListLeftPushAsync("results_queue", JsonSerializer.Serialize(strategyResult));
                    Console.WriteLine($"[SUCCESS] Result sent for {job.JobId}");
                }
                else
                {
                    Console.WriteLine($"[WARNING] No data found in CSV for {job.Symbol} within the requested date range.");
                    
                    // FIX: Always report back to the orchestrator to prevent hangs!
                    var emptyResult = new StrategyResult(
                        job.BatchId, 
                        job.JobId, 
                        0m, // 0 PnL
                        0   // 0 Data points processed
                    );
                    
                    await Task.Delay(10000);
                    await db.ListLeftPushAsync("results_queue", JsonSerializer.Serialize(emptyResult));
                }
            }
        }
    }
    catch (JsonException jex)
    {
        Console.WriteLine($"[ERROR] Failed to parse job JSON: {jex.Message}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ERROR] Critical error during job processing: {ex.Message}");
        // We log the error but keep the worker alive to try the next job.
    }

    // A 100ms heartbeat delay to prevent high CPU usage when the queue is empty.
    await Task.Delay(100);
}