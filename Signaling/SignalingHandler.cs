using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Serilog;

namespace GenLAN.Server.Signaling;

/// <summary>
/// Handles one WebSocket connection from a client.
/// Processes signaling messages: CREATE_ROOM, JOIN_ROOM, SEND_ENDPOINT, GET_RELAY.
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
        Log.Information("[Signal] Client connected: {ConnId} from {IP}", _connectionId, _remoteIp);
        _rooms.RegisterSocket(_connectionId, _ws);

        var buffer = new byte[8192];
        try
        {
            while (_ws.State == WebSocketState.Open)
            {
                var result = await _ws.ReceiveAsync(buffer, CancellationToken.None);

                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                await ProcessMessageAsync(json);
            }
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[Signal] Connection error for {ConnId}", _connectionId);
        }
        finally
        {
            _rooms.UnregisterSocket(_connectionId);

            // Cleanup: close room if this was the host
            var room = _rooms.FindRoomByConnection(_connectionId);
            if (room != null)
            {
                _rooms.CloseRoom(room.Id);
            }

            Log.Information("[Signal] Client disconnected: {ConnId}", _connectionId);
        }
    }

    private async Task ProcessMessageAsync(string json)
    {
        JsonDocument? doc = null;
        try
        {
            doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var type = root.GetProperty("type").GetString();
            var correlationId = root.TryGetProperty("correlationId", out var corrId)
                ? corrId.GetString() : null;
            var data = root.TryGetProperty("data", out var d) ? d : default;

            Log.Debug("[Signal] {ConnId} → {Type}", _connectionId, type);

            switch (type)
            {
                case "CREATE_ROOM":
                    await HandleCreateRoomAsync(correlationId);
                    break;

                case "JOIN_ROOM":
                    var roomId = data.GetProperty("roomId").GetString()!;
                    await HandleJoinRoomAsync(roomId, correlationId);
                    break;

                case "SEND_ENDPOINT":
                    await HandleSendEndpointAsync(data, correlationId);
                    break;

                case "GET_RELAY":
                    var relayRoomId = data.GetProperty("roomId").GetString()!;
                    await HandleGetRelayAsync(relayRoomId, correlationId);
                    break;

                case "RELAY_PACKET":
                    var rRoomId = data.TryGetProperty("roomId", out var rEl) ? rEl.GetString() : null;
                    var rPayload = data.TryGetProperty("payload", out var pEl) ? pEl.GetString() : null;
                    if (!string.IsNullOrEmpty(rRoomId) && !string.IsNullOrEmpty(rPayload))
                    {
                        await _rooms.ForwardToOtherParticipantAsync(_connectionId, rRoomId, new
                        {
                            type = "RELAY_PACKET",
                            data = new { payload = rPayload }
                        });
                    }
                    break;

                default:
                    Log.Warning("[Signal] Unknown message type: {Type}", type);
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

    private async Task HandleCreateRoomAsync(string? correlationId)
    {
        var room = _rooms.CreateRoom(_connectionId);

        // Assign a local port for host UDP
        var port = GetRandomPort();

        await SendAsync(new
        {
            type = "ROOM_CREATED",
            correlationId,
            data = new
            {
                roomId = room.Id,
                port,
                expiresInMinutes = 30
            }
        });

        Log.Information("[Signal] Room {RoomId} created", room.Id);
    }

    private async Task HandleJoinRoomAsync(string roomId, string? correlationId)
    {
        if (!_rooms.TryJoinRoom(roomId, _connectionId, out var room) || room == null)
        {
            await SendAsync(new
            {
                type = "ROOM_NOT_FOUND",
                correlationId,
                data = new { roomId, reason = "Room not found or expired" }
            });
            return;
        }

        // Send room info to joining client
        await SendAsync(new
        {
            type = "ROOM_JOINED",
            correlationId,
            data = new
            {
                roomId = room.Id,
                hostEndpoint = new
                {
                    publicEndpoint = room.Host?.PublicEndpoint ?? "",
                    privateEndpoint = room.Host?.PrivateEndpoint ?? "",
                    publicKey = room.Host?.PublicKey ?? ""
                },
                virtualIp = "10.77.0.2" // Client always gets .2
            }
        });
    }

    private async Task HandleSendEndpointAsync(JsonElement data, string? correlationId)
    {
        var roomId = data.GetProperty("roomId").GetString()!;
        var room = _rooms.GetRoom(roomId);
        if (room == null) return;

        // Determine if this is host or client
        var isHost = room.Host?.ConnectionId == _connectionId;

        // Extract endpoint info
        var endpoint = data.GetProperty("endpoint");
        var publicEp = endpoint.TryGetProperty("publicEndpoint", out var pub) ? pub.GetString() : _remoteIp;
        var privateEp = endpoint.TryGetProperty("privateEndpoint", out var priv) ? priv.GetString() : "";
        var pubKey = endpoint.TryGetProperty("publicKey", out var key) ? key.GetString() : "";

        if (isHost && room.Host != null)
        {
            room.Host.PublicEndpoint = publicEp;
            room.Host.PrivateEndpoint = privateEp;
            room.Host.PublicKey = pubKey;
        }
        else if (!isHost && room.Client != null)
        {
            room.Client.PublicEndpoint = publicEp;
            room.Client.PrivateEndpoint = privateEp;
            room.Client.PublicKey = pubKey;

            // Now that both have sent endpoints, notify host of client's real endpoint
            await NotifyHostAsync(room, new
            {
                type = $"PEER_JOINED_{roomId}",
                data = new
                {
                    peerEndpoint = new
                    {
                        publicEndpoint = publicEp,
                        privateEndpoint = privateEp,
                        publicKey = pubKey
                    }
                }
            });

            // Also send host endpoint to client (updated)
            await SendAsync(new
            {
                type = "HOST_ENDPOINT_UPDATE",
                data = new
                {
                    hostEndpoint = new
                    {
                        publicEndpoint = room.Host?.PublicEndpoint,
                        privateEndpoint = room.Host?.PrivateEndpoint,
                        publicKey = room.Host?.PublicKey
                    }
                }
            });
        }

        await SendAsync(new { type = "ENDPOINT_RECEIVED", correlationId });
    }

    private async Task HandleGetRelayAsync(string roomId, string? correlationId)
    {
        // Return relay server UDP endpoint
        // In production, this is the deployed relay server address
        var relayEndpoint = "relay.genlan.aboadnan.net:7778";

        await SendAsync(new
        {
            type = "RELAY_INFO",
            correlationId,
            data = new { relayEndpoint, sessionId = Guid.NewGuid().ToString("N") }
        });
    }

    private async Task NotifyHostAsync(Room room, object message)
    {
        if (!string.IsNullOrEmpty(room.Host?.ConnectionId))
        {
            await _rooms.SendToConnectionAsync(room.Host.ConnectionId, message);
            Log.Information("[Signal] Notified host {HostConn} of room {RoomId}", room.Host.ConnectionId, room.Id);
        }
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

    private static int GetRandomPort()
    {
        // Return a port in range 47000-48000 for UDP hole punching
        return Random.Shared.Next(47000, 48000);
    }
}
