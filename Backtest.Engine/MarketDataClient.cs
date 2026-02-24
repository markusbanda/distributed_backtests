using Backtest.Shared;
using System.Globalization;
using System.Net;

namespace Backtest.Engine;

public class MarketDataClient
{
    private readonly HttpClient _httpClient;
    private readonly CookieContainer _cookieContainer = new();
    private string? _crumb;

    public MarketDataClient()
    {
        var handler = new SocketsHttpHandler
        {
            CookieContainer = _cookieContainer,
            UseCookies = true,
            AllowAutoRedirect = true
        };

        _httpClient = new HttpClient(handler);
        
        // Essential headers to avoid immediate 403s
        _httpClient.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
        _httpClient.DefaultRequestHeaders.Add("Accept", "*/*");
		_httpClient.DefaultRequestHeaders.Add("Connection", "keep-alive");
    }

    private async Task EnsureHandshakeAsync(string symbol)
    {
        if (_crumb != null) return;

        try
        {
            string quoteUrl = $"https://finance.yahoo.com/quote/{symbol}";
            var quoteResponse = await _httpClient.GetAsync(quoteUrl);
            quoteResponse.EnsureSuccessStatusCode();

            using var request = new HttpRequestMessage(HttpMethod.Get, "https://query1.finance.yahoo.com/v1/test/getcrumb");
            request.Headers.Add("Referer", quoteUrl);

            var crumbResponse = await _httpClient.SendAsync(request);
            crumbResponse.EnsureSuccessStatusCode();

            _crumb = await crumbResponse.Content.ReadAsStringAsync();
            Console.WriteLine($"[Handshake] Success. Crumb: {_crumb}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] Handshake failed: {ex.Message}");
            throw;
        }
    }

    public async Task<List<Candle>> GetHistoricalDataAsync(string symbol, DateTime start, DateTime end)
    {
        var candles = new List<Candle>();

        try
        {
            await EnsureHandshakeAsync(symbol);

            long startUnix = ((DateTimeOffset)start).ToUnixTimeSeconds();
            long endUnix = ((DateTimeOffset)end).ToUnixTimeSeconds();

            string csvUrl = $"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={startUnix}&period2={endUnix}&interval=1d&events=history&includeAdjustedClose=true&crumb={_crumb}";

            using var request = new HttpRequestMessage(HttpMethod.Get, csvUrl);
            request.Headers.Add("Referer", $"https://finance.yahoo.com/quote/{symbol}/history");

            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var csvContent = await response.Content.ReadAsStringAsync();
            var rows = csvContent.Split('\n');

            for (int i = 1; i < rows.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(rows[i])) continue;
                var columns = rows[i].Split(',');
                
                if (columns.Length >= 7 && DateTime.TryParse(columns[0], out DateTime date))
                {
                    candles.Add(new Candle
                    {
                        DateTime = date,
                        Open = ParseDecimal(columns[1]),
                        High = ParseDecimal(columns[2]),
                        Low = ParseDecimal(columns[3]),
                        Close = ParseDecimal(columns[4]),
                        Volume = ParseDecimal(columns[6])
                    });
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] Data fetch failed: {ex.Message}");
            _crumb = null; // Reset to try again on next job
        }

        return candles;
    }

    private decimal ParseDecimal(string value) => 
        decimal.TryParse(value, CultureInfo.InvariantCulture, out decimal result) ? result : 0;
}