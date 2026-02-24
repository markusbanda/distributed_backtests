using StackExchange.Redis;
using Backtest.Shared;
using System.Text.Json;

// 1. Setup Connection Logic
// We pull the Redis connection string from Environment Variables for Docker compatibility.
string redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";

Console.WriteLine("--- Worker Engine Starting ---");
Console.WriteLine($"Connecting to Redis at: {redisConnection}");

// 2. Initialize Redis Connection
var redis = await ConnectionMultiplexer.ConnectAsync(redisConnection);
var db = redis.GetDatabase();

// 3. Initialize Strategy Logic
var strategy = new SmaStrategy();
var ingestor = new DataIngestor();

// Determine the path for the data file relative to the app execution directory
var baseDir = AppContext.BaseDirectory;
var filePath = Path.Combine(baseDir, "market_data.csv");

Console.WriteLine("Worker successfully connected. Waiting for jobs in 'job_queue'...");

// 4. The Main Execution Loop
while (true)
{
    try
    {
        // 'ListRightPopAsync' blocks until a job is available (LIFO/FIFO depending on Orchestrator push)
        // We use the '!' null-forgiving operator because we check .HasValue immediately after.
        var result = await db.ListRightPopAsync("job_queue");

        if (result.HasValue)
        {
            // Cast RedisValue to string to resolve JSON deserialization ambiguity
            var job = JsonSerializer.Deserialize<BacktestJob>((string)result!);

            if (job != null)
            {
                Console.WriteLine($"[Received] Processing Batch: {job.BatchId} | Job: {job.JobId} for {job.Symbol}");

                // A. Ingest Market Data
                var data = await ingestor.ReadCsvAsync(filePath);

                // B. Execute Strategy
                // This is our high-performance logic using Spans/ReadOnlySpan
                strategy.Execute(data);

                // C. Generate and Report Result
                // In a production system, these metrics would come from the strategy.Execute return value.
                var strategyResult = new StrategyResult(
                    job.BatchId,
                    job.JobId,
                    150.00m, // Simulated Total PnL for this segment
                    5        // Simulated Trade Count for this segment
                );

                // D. Push back to the "results_queue" for the Aggregator to pick up
                string resultJson = JsonSerializer.Serialize(strategyResult);
                await db.ListLeftPushAsync("results_queue", resultJson);

                Console.WriteLine($"[Completed] Results for Job {job.JobId} sent to aggregator.");
            }
        }
    }
    catch (Exception ex)
    {
        // In a trading firm, you'd log this to a tool like Sentry or Datadog
        Console.WriteLine($"[Error] An error occurred while processing job: {ex.Message}");
    }

    // A small delay to prevent the loop from "spinning" and eating CPU cycles
    // during periods where the Redis queue is empty.
    await Task.Delay(100);
}