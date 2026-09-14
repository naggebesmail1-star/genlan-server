# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ["GenLAN.Server.csproj", "./"]
RUN dotnet restore "./GenLAN.Server.csproj"

COPY . .
RUN dotnet publish "GenLAN.Server.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

ENV PORT=10000
EXPOSE 10000

ENTRYPOINT ["dotnet", "GenLAN.Server.dll"]
