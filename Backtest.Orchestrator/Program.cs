using StackExchange.Redis;
using Backtest.Shared;
using System.Text.Json;
using System.Diagnostics;

string redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost";
Console.WriteLine($"[*] Orchestrator connecting to Redis at: {redisConnection}");

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

// 1. Setup the Batch 
Guid batchId = Guid.NewGuid();
string symbol = "SPY";
int totalJobs = 10;
var pendingJobs = new HashSet<Guid>(); // Tracking exact Job Guids

Console.WriteLine($"\n--- Starting Batch {batchId} for {symbol} ---");

// 2. Generate and Push Jobs
DateTime startDate = new DateTime(2025, 05, 01);
for (int i = 0; i < totalJobs; i++)
{
    // Perfectly matches: BacktestJob(Guid, Guid, string, DateTime, DateTime)
    var job = new BacktestJob(
        batchId,
        Guid.NewGuid(),
        symbol,
        startDate.AddMonths(i),
        startDate.AddMonths(i + 1)
    );

    string json = JsonSerializer.Serialize(job);
    await db.ListLeftPushAsync("job_queue", json);
    
    // Add to our pending list
    pendingJobs.Add(job.JobId); 
    
    Console.WriteLine($"[Sent] Job {i + 1}/{totalJobs} pushed to Redis.");
}

Console.WriteLine($"[Aggregator] Monitoring Batch {batchId}. Expecting {totalJobs} results...");

// 3. Robust Aggregation Loop with Timeout
var timeout = TimeSpan.FromSeconds(30); 
var sw = Stopwatch.StartNew();
int receivedCount = 0;
decimal totalPnL = 0m;

// Loop continues while we still have pending jobs AND we haven't timed out
while (pendingJobs.Count > 0 && sw.Elapsed < timeout)
{
    var result = await db.ListRightPopAsync("results_queue");

    if (result.HasValue)
    {
        var stratResult = JsonSerializer.Deserialize<StrategyResult>((string)result!);
        
        if (stratResult != null)
        {
            // Cross the job off our pending list
            if (pendingJobs.Remove(stratResult.JobId))
            {
                receivedCount++;
                
                // Perfectly matches your TotalPnL property
                totalPnL += stratResult.TotalPnL; 
                Console.WriteLine($"[Progress] {receivedCount}/{totalJobs} received. (Latest PnL: ${stratResult.TotalPnL})");
            }
        }
    }
    else
    {
        // No messages right now, wait a tiny bit before checking again
        await Task.Delay(200);
    }
}

// 4. Batch Completion & DLQ Processing
sw.Stop();
Console.WriteLine("\n================================================");
Console.WriteLine($"BATCH COMPLETE in {sw.Elapsed.TotalSeconds:F2} seconds");
Console.WriteLine($"Total PnL Aggregated: ${totalPnL}");
Console.WriteLine($"Successful Jobs: {receivedCount} / {totalJobs}");

// If there's anything left in pendingJobs, it timed out/crashed
if (pendingJobs.Count > 0)
{
    Console.WriteLine($"\n[WARNING] {pendingJobs.Count} jobs timed out! Moving to Dead Letter Queue (DLQ).");
    foreach (var missingJobId in pendingJobs)
    {
        // Serialize the Guid back to string for Redis storage
        await db.ListLeftPushAsync("dlq_queue", missingJobId.ToString());
        Console.WriteLine($"[DLQ] Orphaned Job ID moved to DLQ: {missingJobId}");
    }
}
Console.WriteLine("================================================\n");