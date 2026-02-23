# Build Stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy the project files specifically to restore dependencies
# These paths are relative to the ROOT folder (where docker-compose.yml is)
COPY ["Backtest.Shared/Backtest.Shared.csproj", "Backtest.Shared/"]
COPY ["Backtest.Engine/Backtest.Engine.csproj", "Backtest.Engine/"]

# Restore only the Engine (Shared is restored automatically as a dependency)
RUN dotnet restore "Backtest.Engine/Backtest.Engine.csproj"

# Copy the rest of the source code
COPY . .

# Move into the Engine folder to publish
WORKDIR "/src/Backtest.Engine"
RUN dotnet publish -c Release -o /app/publish

# Runtime Stage
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish .

# The market_data.csv must be in the bin folder (managed by our .csproj fix earlier)
ENTRYPOINT ["dotnet", "Backtest.Engine.dll"]