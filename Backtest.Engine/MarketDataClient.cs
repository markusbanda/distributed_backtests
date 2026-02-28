using Backtest.Shared;
using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using Polly;
using Polly.Retry;

namespace Backtest.Engine;

public class MarketDataClient
{
    private readonly HttpClient _httpClient;
    private readonly CookieContainer _cookieContainer = new();
    private string? _crumb;
    private readonly AsyncRetryPolicy _retryPolicy;

    public MarketDataClient()
    {
        var handler = new SocketsHttpHandler
        {
            CookieContainer = _cookieContainer,
            UseCookies = true,
            AllowAutoRedirect = true
        };

        _httpClient = new HttpClient(handler);
        
        // Headers must be very specific to look like a modern browser
        _httpClient.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36");
        _httpClient.DefaultRequestHeaders.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/ *;q=0.8");
        _httpClient.DefaultRequestHeaders.Add("Accept-Language", "en-US,en;q=0.5");

        //Default retry policy
        _retryPolicy = Policy
            .Handle<HttpRequestException>(ex => ex.StatusCode == HttpStatusCode.Unauthorized || ex.StatusCode == HttpStatusCode.Forbidden)
            .Or<Exception>()
            .WaitAndRetryAsync(3, retryAttempt => 
                TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)) + TimeSpan.FromMilliseconds(new Random().Next(0, 1000)),
                (exception, timeSpan, count, context) =>
                {
                    Console.WriteLine($"[Retry] Attempt {count} failed. Retrying in {timeSpan.TotalSeconds:N2}s... Error: {exception.Message}");
                    _crumb = null; // Clear crumb to force a new handshake on retry
                });
    }

    private async Task EnsureHandshakeAsync(string symbol)
    {
        if (_crumb != null) return;

        // Simulate human delay so we don't look like a bot
        await Task.Delay(new Random().Next(500, 1500));

        Console.WriteLine($"[Handshake] establishing session for {symbol}...");

        string quoteUrl = $"https://finance.yahoo.com/quote/{symbol}";
        
        // 1. Get the page to set the session cookie
        var response = await _httpClient.GetAsync(quoteUrl);
        response.EnsureSuccessStatusCode();
        var html = await response.Content.ReadAsStringAsync();

        // 2. Extract Crumb from the HTML source using Regex. 
        // Yahoo stores the crumb in a JSON object in the source: "CrumbStore":{"crumb":"XYZ"}
        var match = Regex.Match(html, @"""crumb"":""([^""]+)""");
        if (match.Success)
        {
            _crumb = match.Groups[1].Value;
            // Decode potential unicode escapes like \u002F
            _crumb = Regex.Unescape(_crumb);
        }
        else
        {
            // Fallback to the test endpoint if regex fails
            _crumb = await _httpClient.GetStringAsync("https://query1.finance.yahoo.com/v1/test/getcrumb");
        }

        if (string.IsNullOrEmpty(_crumb))
            throw new Exception("Could not find Crumb in Yahoo response.");

        Console.WriteLine($"[Handshake] Success. Crumb: {_crumb}");
    }

    public async Task<List<Candle>> GetHistoricalDataAsync(string symbol, DateTime start, DateTime end)
    {
        // Wrap the entire fetch in the retry policy
        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var candles = new List<Candle>();

            await EnsureHandshakeAsync(symbol);

            long period1 = ((DateTimeOffset)start).ToUnixTimeSeconds();
            long period2 = ((DateTimeOffset)end).ToUnixTimeSeconds();

            string csvUrl = $"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true&crumb={_crumb}";

            var request = new HttpRequestMessage(HttpMethod.Get, csvUrl);
            request.Headers.Add("Referer", $"https://finance.yahoo.com/quote/{symbol}/history");

            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync();
            var lines = content.Split('\n');

            for (int i = 1; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i])) continue;
                var cols = lines[i].Split(',');
                
                if (cols.Length >= 7 && DateTime.TryParse(cols[0], out DateTime date))
                {
                    candles.Add(new Candle
                    {
                        DateTime = date,
                        Open = ParseDecimal(cols[1]),
                        High = ParseDecimal(cols[2]),
                        Low = ParseDecimal(cols[3]),
                        Close = ParseDecimal(cols[4]),
                        Volume = ParseDecimal(cols[6])
                    });
                }
            }

            return candles;
        });
    }

    private decimal ParseDecimal(string val) => 
        decimal.TryParse(val, CultureInfo.InvariantCulture, out decimal res) ? res : 0;
}