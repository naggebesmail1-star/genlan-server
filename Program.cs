using GenLAN.Server.Signaling;
using GenLAN.Server.Relay;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("logs/server-.log", rollingInterval: RollingInterval.Day)
    .CreateLogger();

var builder = WebApplication.CreateBuilder(args);
builder.Host.UseSerilog();

builder.Services.AddSingleton<RoomManager>();
builder.Services.AddSingleton<RelayServer>();

var app = builder.Build();

// Health check endpoint
app.MapGet("/", () => new { status = "GEN LAN Signal Server", version = "1.0.0" });
app.MapGet("/health", () => Results.Ok(new { status = "ok", time = DateTime.UtcNow }));

// WebSocket for signaling
app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(30)
});

app.MapGet("/ws", async (HttpContext ctx, RoomManager rooms) =>
{
    if (ctx.WebSockets.IsWebSocketRequest)
    {
        using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
        var handler = new SignalingHandler(ws, rooms, ctx.Connection.RemoteIpAddress?.ToString() ?? "unknown");
        await handler.HandleAsync();
    }
    else
    {
        ctx.Response.StatusCode = 400;
    }
});

// Relay server (UDP) runs as background service
var relayServer = app.Services.GetRequiredService<RelayServer>();
_ = relayServer.StartAsync();

var envPort = Environment.GetEnvironmentVariable("PORT");
var port = !string.IsNullOrEmpty(envPort) && int.TryParse(envPort, out var p) ? p :
           (args.Contains("--port") ? int.Parse(args[args.ToList().IndexOf("--port") + 1]) : 7777);

Log.Information("GEN LAN Signal Server starting on port {Port}", port);
app.Run($"http://0.0.0.0:{port}");
