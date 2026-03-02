using System;
using System.Collections.Generic;
using System.Linq;
using Backtest.Shared;

namespace Backtest.Engine
{
    public class EmaStrategy
    {
        public int Period { get; set; } = 14;

        /// <summary>
        /// Executes the EMA strategy. 
        /// Logic: If Price > EMA, it's a BUY signal. 
        /// If Price < EMA, it's a SELL signal (or exit).
        /// </summary>
        public void Execute(List<Candle> data)
        {
            if (data == null || data.Count < Period)
            {
                return;
            }

            // EMA Formula: [Closing Price - EMA (previous day)] x multiplier + EMA (previous day)
            // Multiplier: 2 / (Period + 1)
            decimal multiplier = 2.0m / (Period + 1);
            
            // Start the first EMA value as a simple average of the first 'Period' points
            decimal currentEma = data.Take(Period).Average(p => p.Close);

            // Process the data starting from the point after the initial SMA seed
            for (int i = Period; i < data.Count; i++)
            {
                decimal close = data[i].Close;
                
                // Calculate the new EMA value
                currentEma = (close - currentEma) * multiplier + currentEma;

                // Simple Signal Logic
                if (close > currentEma)
                {
                    //Console.WriteLine($"[{data[i].DateTime:MM/dd/yyyy HH:mm:ss}] EMA BUY Signal at {close:F2} (EMA: {currentEma:F2})");
                }
                else if (close < currentEma)
                {
                    //Console.WriteLine($"[{data[i].DateTime:MM/dd/yyyy HH:mm:ss}] EMA SELL Signal at {close:F2} (EMA: {currentEma:F2})");
                }
            }
        }
    }
}