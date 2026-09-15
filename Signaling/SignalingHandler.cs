using System;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace GenLAN.Server.Signaling;

/// <summary>
/// Handles one WebSocket connection for the 16-player virtual LAN.
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
            var left = room.RemoveParticipant(_connectionId);
            if (left != null)
            {
                Log.Information("[Signal] Player {ConnId} (IP: {Ip}) left room {RoomId}. Remaining: {Count}",
                    _connectionId, left.VirtualIp, room.Id, room.PlayerCount);

                if (room.PlayerCount > 0)
                {
                    await _rooms.BroadcastToRoomOthersAsync(_connectionId, room.Id, new
                    {
                        type = "PEER_LEFT",
                        data = new { virtualIp = left.VirtualIp, totalPlayers = room.PlayerCount }
                    });
                }
                else
                {
                    _rooms.CloseRoom(room.Id);
                }
            }
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
                        await _rooms.BroadcastToRoomOthersAsync(_connectionId, rRoomId,
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
        var host = _rooms.CreateRoom(_connectionId, out var room);

        await SendAsync(new
        {
            type = "ROOM_CREATED",
            correlationId = corrId,
            data = new
            {
                roomId = room.Id,
                virtualIp = host.VirtualIp,
                totalPlayers = room.PlayerCount
            }
        });

        Log.Information("[Signal] Room {RoomId} created by {ConnId} (IP: {Ip})", room.Id, _connectionId, host.VirtualIp);
    }

    private async Task HandleJoinRoomAsync(string roomId, string? corrId)
    {
        if (!_rooms.TryJoinRoom(roomId, _connectionId, out var room, out var participant) || room == null || participant == null)
        {
            await SendAsync(new
            {
                type = "ROOM_NOT_FOUND",
                correlationId = corrId,
                data = new { roomId }
            });
            Log.Warning("[Signal] Room {RoomId} not found/full for {ConnId}", roomId, _connectionId);
            return;
        }

        // 1. Confirm to the joining player
        await SendAsync(new
        {
            type = "ROOM_JOINED",
            correlationId = corrId,
            data = new
            {
                roomId = room.Id,
                virtualIp = participant.VirtualIp,
                totalPlayers = room.PlayerCount
            }
        });

        // 2. Notify all existing participants in the room
        await _rooms.BroadcastToRoomOthersAsync(_connectionId, room.Id, new
        {
            type = "PEER_JOINED",
            data = new
            {
                roomId = room.Id,
                virtualIp = participant.VirtualIp,
                totalPlayers = room.PlayerCount
            }
        });

        Log.Information("[Signal] Player {ConnId} joined room {RoomId} (Assigned IP: {Ip}, Total: {Count}/16)",
            _connectionId, roomId, participant.VirtualIp, room.PlayerCount);
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
