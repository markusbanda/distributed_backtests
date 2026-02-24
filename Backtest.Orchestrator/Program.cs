using StackExchange.Redis;
using Backtest.Shared;
using System.Text.Json;
using Backtest.Orchestrator;

// 1. Setup Connection Logic
string redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";
Console.WriteLine($"Connecting to Redis at: {redisConnection}");

var redis = await ConnectionMultiplexer.ConnectAsync(redisConnection);
var db = redis.GetDatabase();

// 2. Initialize our helper classes
var aggregator = new ResultAggregator(db);

// 3. User Input (or Environment Variable for Docker)
string symbol = Environment.GetEnvironmentVariable("TARGET_SYMBOL") ?? "CL"; // Default to Crude Oil futures if nothing is provided
if (string.IsNullOrEmpty(symbol))
{
    Console.WriteLine("Enter a symbol to backtest (e.g., BTCUSD):");
    symbol = Console.ReadLine() ?? "GENERIC";
}

var batchId = Guid.NewGuid();
int totalJobs = 10;

// 4. Distribution Phase
Console.WriteLine($"\n--- Starting Batch {batchId} for {symbol} ---");

for (int i = 0; i < totalJobs; i++)
{
    var job = new BacktestJob(
        batchId, 
        Guid.NewGuid(), 
        symbol, 
        DateTime.Now.AddMonths(-i), 
        DateTime.Now.AddMonths(-i + 1)
    );

    string message = JsonSerializer.Serialize(job);
    await db.ListLeftPushAsync("job_queue", message);
    
    Console.WriteLine($"[Sent] Job {i + 1}/{totalJobs} pushed to Redis.");
}

// 5. Aggregation Phase
// We 'await' this so the program stays alive until all results are in.
await aggregator.MonitorResultsAsync(batchId, totalJobs);

Console.WriteLine("Press any key to exit or wait for next manual run...");
// If running in Docker, the container will exit here after the report is printed.