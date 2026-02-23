using StackExchange.Redis;
using Backtest.Shared;
using System.Text.Json;

// 1. Connect to Redis (assuming it's running in Docker/Localhost)
// Get the connection string from an environment variable, 
// defaulting to "localhost" for local development.
string redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";

var redis = await ConnectionMultiplexer.ConnectAsync(redisConnection);
var db = redis.GetDatabase();

Console.WriteLine("--- Backtest Orchestrator ---");

/*** UNCOMMENT THIS CODE BELOW FOR MANUAL CONSOLE SYMBOL INPUT ***/
//Console.WriteLine("Enter a symbol to backtest (e.g., BTCUSD):");
//var symbol = Console.ReadLine() ?? "GENERIC";
/*** END OF MANUAL CONSOLE SYMBOL INPUT CODE ***/

// Check if a symbol was passed via Docker, otherwise ask for one
string symbol = Environment.GetEnvironmentVariable("TARGET_SYMBOL") ?? "CL"; // Default to Crude Oil if nothing is provided

Console.WriteLine($"Running backtest for: {symbol}");

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