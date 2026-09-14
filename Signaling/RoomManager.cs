using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Serilog;

namespace GenLAN.Server.Signaling;

public class Room
{
    public string Id { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime ExpiresAt { get; set; } = DateTime.UtcNow.AddMinutes(30);
    public RoomParticipant? Host { get; set; }
    public RoomParticipant? Client { get; set; }
    public RoomState State { get; set; } = RoomState.WaitingForClient;

    public bool IsExpired => DateTime.UtcNow > ExpiresAt;
    public bool IsFull => Host != null && Client != null;
}

public class RoomParticipant
{
    public string ConnectionId { get; set; } = "";
    public string? PublicEndpoint { get; set; }
    public string? PrivateEndpoint { get; set; }
    public string? PublicKey { get; set; }
    public string? RelaySessionId { get; set; }
}

public enum RoomState
{
    WaitingForClient,
    NegotiatingP2P,
    Active,
    Closed
}

/// <summary>
/// In-memory room manager. Handles room lifecycle and cleanup.
/// </summary>
public class RoomManager
{
    private readonly ConcurrentDictionary<string, Room> _rooms = new();
    private readonly ConcurrentDictionary<string, WebSocket> _sockets = new();
    private readonly Timer _cleanupTimer;

    public void RegisterSocket(string connectionId, WebSocket ws) => _sockets[connectionId] = ws;
    public void UnregisterSocket(string connectionId) => _sockets.TryRemove(connectionId, out _);

    public async Task SendToConnectionAsync(string connectionId, object message)
    {
        if (_sockets.TryGetValue(connectionId, out var ws) && ws.State == WebSocketState.Open)
        {
            var json = JsonSerializer.Serialize(message);
            var bytes = Encoding.UTF8.GetBytes(json);
            try
            {
                await ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "[RoomManager] Failed to send to {ConnId}", connectionId);
            }
        }
    }

    public async Task ForwardToOtherParticipantAsync(string currentConnectionId, string roomId, object message)
    {
        var room = GetRoom(roomId);
        if (room == null) return;
        var targetId = room.Host?.ConnectionId == currentConnectionId ? room.Client?.ConnectionId : room.Host?.ConnectionId;
        if (!string.IsNullOrEmpty(targetId))
        {
            await SendToConnectionAsync(targetId, message);
        }
    }

    // Valid characters for room codes (excluding confusable characters: 0/O, 1/I/L)
    private const string CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    private const int CODE_LENGTH = 5;

    public RoomManager()
    {
        // Cleanup expired rooms every 5 minutes
        _cleanupTimer = new Timer(_ => CleanupExpiredRooms(), null,
            TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    public Room CreateRoom(string connectionId)
    {
        var id = GenerateRoomId();
        var room = new Room
        {
            Id = id,
            Host = new RoomParticipant { ConnectionId = connectionId }
        };

        _rooms[id] = room;
        Log.Information("[RoomManager] Created room {RoomId} for {ConnectionId}", id, connectionId);
        return room;
    }

    public Room? GetRoom(string roomId)
    {
        _rooms.TryGetValue(roomId.ToUpperInvariant(), out var room);
        if (room?.IsExpired == true)
        {
            _rooms.TryRemove(roomId.ToUpperInvariant(), out _);
            return null;
        }
        return room;
    }

    public bool TryJoinRoom(string roomId, string connectionId, out Room? room)
    {
        room = GetRoom(roomId.ToUpperInvariant());
        if (room == null)
        {
            Log.Warning("[RoomManager] Room {RoomId} not found", roomId);
            return false;
        }

        if (room.IsFull)
        {
            Log.Warning("[RoomManager] Room {RoomId} is full", roomId);
            return false;
        }

        room.Client = new RoomParticipant { ConnectionId = connectionId };
        room.State = RoomState.NegotiatingP2P;
        Log.Information("[RoomManager] Client joined room {RoomId}", roomId);
        return true;
    }

    public void CloseRoom(string roomId)
    {
        _rooms.TryRemove(roomId.ToUpperInvariant(), out _);
        Log.Information("[RoomManager] Room {RoomId} closed", roomId);
    }

    public Room? FindRoomByConnection(string connectionId)
    {
        return _rooms.Values.FirstOrDefault(r =>
            r.Host?.ConnectionId == connectionId ||
            r.Client?.ConnectionId == connectionId);
    }

    private static string GenerateRoomId()
    {
        var chars = new char[CODE_LENGTH];
        var bytes = new byte[CODE_LENGTH];
        System.Security.Cryptography.RandomNumberGenerator.Fill(bytes);

        for (int i = 0; i < CODE_LENGTH; i++)
            chars[i] = CODE_CHARS[bytes[i] % CODE_CHARS.Length];

        return "GEN-" + new string(chars);
    }

    private void CleanupExpiredRooms()
    {
        var expired = _rooms.Where(kv => kv.Value.IsExpired).Select(kv => kv.Key).ToList();
        foreach (var key in expired)
        {
            _rooms.TryRemove(key, out _);
            Log.Debug("[RoomManager] Expired room {RoomId} cleaned up", key);
        }
    }
}
