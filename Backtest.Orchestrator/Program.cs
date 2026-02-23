using StackExchange.Redis;
using Backtest.Shared;
using System.Text.Json;

// 1. Connect to Redis (assuming it's running in Docker/Localhost)
var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
var db = redis.GetDatabase();

Console.WriteLine("--- Backtest Orchestrator ---");
Console.WriteLine("Enter a symbol to backtest (e.g., BTCUSD):");
var symbol = Console.ReadLine() ?? "GENERIC";

var batchId = Guid.NewGuid();

// 2. Create 10 "Jobs" (Simulating slicing 10 months of data)
for (int i = 0; i < 10; i++)
{
    var job = new BacktestJob(
        batchId, 
        Guid.NewGuid(), 
        symbol, 
        DateTime.Now.AddMonths(-i), 
        DateTime.Now.AddMonths(-i + 1)
    );

    // Serialize to JSON for transport
    string message = JsonSerializer.Serialize(job);

    // 3. Push to Redis List (The 'Work Queue')
    await db.ListLeftPushAsync("job_queue", message);
    
    Console.WriteLine($"[Sent] Job {job.JobId} for {symbol} pushed to queue.");
}

Console.WriteLine("\nAll jobs distributed. Waiting for workers to pick them up...");