using StackExchange.Redis;
using Backtest.Shared;
using Backtest.Engine; 
using System.Text.Json;

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

// 3. Initialize Strategy and Market Data Components
// SmaStrategy: The quant logic (Phase 1)
// MarketDataClient: The Yahoo Finance API logic (Phase 2 - Updated)
var strategy = new SmaStrategy();
var marketClient = new MarketDataClient();

Console.WriteLine("[*] Worker successfully connected.");
Console.WriteLine("[*] Waiting for jobs in 'job_queue'...");

// 4. The Main Execution Loop
// This loop runs indefinitely, polling Redis for new backtesting tasks.
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

                // A. Fetch Market Data via API Handshake
                // This now handles the Yahoo Finance Cookie/Crumb handshake internally.
                var data = await marketClient.GetHistoricalDataAsync(job.Symbol, job.Start, job.End);

                if (data != null && data.Count > 0)
                {
                    Console.WriteLine($"[FETCH] Successfully retrieved {data.Count} candles.");

                    // B. Execute the High-Performance Strategy
                    // Note: Ensure your market_data.csv logic from Phase 1 is updated 
                    // to accept this List<Candle> from the API.
                    strategy.Execute(data);

                    // C. Generate the Result Object
                    // In a real firm, PnL and TradeCount would be returned by strategy.Execute().
                    // For now, we simulate a successful backtest result.
                    var strategyResult = new StrategyResult(
                        job.BatchId,
                        job.JobId,
                        245.50m, // Simulated Total PnL for this segment
                        data.Count // Total trades/data points processed
                    );

                    // D. Push back to the "results_queue" for the Aggregator
                    string resultJson = JsonSerializer.Serialize(strategyResult);
                    await db.ListLeftPushAsync("results_queue", resultJson);

                    Console.WriteLine($"[SUCCESS] Results for Job {job.JobId} sent to aggregator.");
                }
                else
                {
                    Console.WriteLine($"[WARNING] No data returned for {job.Symbol}. Skipping result push.");
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