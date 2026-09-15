using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Serilog;

namespace GenLAN.Server.Signaling;

/// <summary>
/// Handles one WebSocket connection.
/// Protocol (client → server): { type, data?, correlationId? }
/// Server → client push: { type, data?, correlationId? }
///
/// Supported types (client sends):
///   CREATE_ROOM                      → ROOM_CREATED { roomId }
///   JOIN_ROOM  { roomId }            → ROOM_JOINED { roomId } | ROOM_NOT_FOUND
///   RELAY_PACKET { roomId, payload } → forwarded to other participant
///
/// Push from server:
///   PEER_JOINED { roomId }           → to host when client joins
///   PEER_LEFT                        → to remaining participant when other disconnects
///   RELAY_PACKET { payload }         → forwarded relay packet
/// </summary>
public class SignalingHandler
{
    private readonly WebSocket _ws;
    private readonly RoomManager _rooms;
    private readonly string _remoteIp;
    private readonly string _connectionId;

    public SignalingHandler(WebSocket ws, RoomManager rooms, string remoteIp)
    {
        _ws = ws;
        _rooms = rooms;
        _remoteIp = remoteIp;
        _connectionId = Guid.NewGuid().ToString("N")[..12];
    }

    public async Task HandleAsync()
    {
        Log.Information("[Signal] Connected: {ConnId} from {IP}", _connectionId, _remoteIp);
        _rooms.RegisterSocket(_connectionId, _ws);

        var buffer = new byte[65536];
        try
        {
            while (_ws.State == WebSocketState.Open)
            {
                using var ms = new System.IO.MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _ws.ReceiveAsync(buffer, CancellationToken.None);
                    if (result.MessageType == WebSocketMessageType.Close) goto done;
                    ms.Write(buffer, 0, result.Count);
                } while (!result.EndOfMessage);

                var json = Encoding.UTF8.GetString(ms.ToArray());
                await ProcessMessageAsync(json);
            }
        }
        catch (Exception ex)
        {
            Log.Debug(ex, "[Signal] Connection error for {ConnId}", _connectionId);
        }

        done:
        _rooms.UnregisterSocket(_connectionId);

        var room = _rooms.FindRoomByConnection(_connectionId);
        if (room != null)
        {
            var peerId = room.Host?.ConnectionId == _connectionId
                ? room.Client?.ConnectionId
                : room.Host?.ConnectionId;

            if (!string.IsNullOrEmpty(peerId))
                await _rooms.SendToConnectionAsync(peerId, new { type = "PEER_LEFT" });

            _rooms.CloseRoom(room.Id);
        }

        Log.Information("[Signal] Disconnected: {ConnId}", _connectionId);
    }

    private async Task ProcessMessageAsync(string json)
    {
        JsonDocument? doc = null;
        try
        {
            doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var type = root.GetProperty("type").GetString() ?? "";
            var corrId = root.TryGetProperty("correlationId", out var cEl) ? cEl.GetString() : null;
            var data = root.TryGetProperty("data", out var dEl) ? dEl : default;

            Log.Debug("[Signal] {ConnId} -> {Type}", _connectionId, type);

            switch (type)
            {
                case "CREATE_ROOM":
                    await HandleCreateRoomAsync(corrId);
                    break;

                case "JOIN_ROOM":
                    var roomId = data.GetProperty("roomId").GetString()!.Trim().ToUpperInvariant();
                    await HandleJoinRoomAsync(roomId, corrId);
                    break;

                case "RELAY_PACKET":
                    var rRoomId = data.TryGetProperty("roomId", out var rEl) ? rEl.GetString() : null;
                    var rPayload = data.TryGetProperty("payload", out var pEl) ? pEl.GetString() : null;
                    if (!string.IsNullOrEmpty(rRoomId) && !string.IsNullOrEmpty(rPayload))
                    {
                        await _rooms.ForwardToOtherParticipantAsync(_connectionId, rRoomId,
                            new { type = "RELAY_PACKET", data = new { payload = rPayload } });
                    }
                    break;

                default:
                    Log.Warning("[Signal] Unknown message: {Type} from {ConnId}", type, _connectionId);
                    break;
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "[Signal] Error processing message from {ConnId}", _connectionId);
        }
        finally
        {
            doc?.Dispose();
        }
    }

    private async Task HandleCreateRoomAsync(string? corrId)
    {
        var room = _rooms.CreateRoom(_connectionId);

        await SendAsync(new
        {
            type = "ROOM_CREATED",
            correlationId = corrId,
            data = new { roomId = room.Id }
        });

        Log.Information("[Signal] Room {RoomId} created by {ConnId}", room.Id, _connectionId);
    }

    private async Task HandleJoinRoomAsync(string roomId, string? corrId)
    {
        if (!_rooms.TryJoinRoom(roomId, _connectionId, out var room) || room == null)
        {
            await SendAsync(new
            {
                type = "ROOM_NOT_FOUND",
                correlationId = corrId,
                data = new { roomId }
            });
            Log.Warning("[Signal] Room {RoomId} not found for {ConnId}", roomId, _connectionId);
            return;
        }

        // Confirm to joining client
        await SendAsync(new
        {
            type = "ROOM_JOINED",
            correlationId = corrId,
            data = new { roomId = room.Id }
        });

        // Immediately notify host that a client joined
        if (!string.IsNullOrEmpty(room.Host?.ConnectionId))
        {
            await _rooms.SendToConnectionAsync(room.Host.ConnectionId, new
            {
                type = "PEER_JOINED",
                data = new { roomId = room.Id }
            });
        }

        Log.Information("[Signal] Client {ConnId} joined room {RoomId}", _connectionId, roomId);
    }

    private async Task SendAsync(object message)
    {
        if (_ws.State != WebSocketState.Open) return;

        var json = JsonSerializer.Serialize(message);
        var bytes = Encoding.UTF8.GetBytes(json);

        try
        {
            await _ws.SendAsync(
                new ArraySegment<byte>(bytes),
                WebSocketMessageType.Text,
                endOfMessage: true,
                cancellationToken: CancellationToken.None);
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[Signal] Failed to send to {ConnId}", _connectionId);
        }
    }
}
