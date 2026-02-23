FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["Backtest.Shared/Backtest.Shared.csproj", "Backtest.Shared/"]
COPY ["Backtest.Orchestrator/Backtest.Orchestrator.csproj", "Backtest.Orchestrator/"]

RUN dotnet restore "Backtest.Orchestrator/Backtest.Orchestrator.csproj"

COPY . .
WORKDIR "/src/Backtest.Orchestrator"
RUN dotnet publish -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Backtest.Orchestrator.dll"]