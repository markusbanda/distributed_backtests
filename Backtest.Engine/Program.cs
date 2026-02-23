using StackExchange.Redis;
using Backtest.Shared;
using System.Text.Json;

// 1. Connect to Redis
var redis = await ConnectionMultiplexer.ConnectAsync("localhost");
var db = redis.GetDatabase();

// These classes should be in your Backtest.Engine project
var baseDir = AppContext.BaseDirectory;
var filePath = Path.Combine(baseDir, "market_data.csv");

var strategy = new SmaStrategy();
var ingestor = new DataIngestor();

Console.WriteLine("--- Worker Engine Started ---");
Console.WriteLine("Waiting for jobs from Redis...");

while (true)
{
    // 2. 'ListRightPopAsync' returns a RedisValue
    var result = await db.ListRightPopAsync("job_queue");

    if (result.HasValue)
    {
        // FIX: Cast result to (string) or (string?) to resolve the ambiguity
        var job = JsonSerializer.Deserialize<BacktestJob>((string)result!);
        
        if (job != null)
        {
            Console.WriteLine($"[Received] Processing {job.Symbol} | Job: {job.JobId}");

            // 3. Run the Engine Logic
            var data = await ingestor.ReadCsvAsync(filePath);
            strategy.Execute(data);

            Console.WriteLine($"[Completed] Job {job.JobId} finished.");
        }
    }
    
    await Task.Delay(100); 
}