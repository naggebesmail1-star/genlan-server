using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace GenLAN.Server.Signaling;

public class RoomParticipant
{
    public string ConnectionId { get; set; } = "";
    public string VirtualIp { get; set; } = "";
    public int IpSlot { get; set; }
    public bool IsHost { get; set; }
    public DateTime JoinedAt { get; set; } = DateTime.UtcNow;
}

public class Room
{
    public const int MAX_PLAYERS = 16;

    public string Id { get; set; } = "";
    public string HostConnectionId { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime ExpiresAt { get; set; } = DateTime.UtcNow.AddHours(2);
    public ConcurrentDictionary<string, RoomParticipant> Participants { get; } = new();
    private readonly HashSet<int> _usedSlots = new();
    private readonly object _lock = new();

    public bool IsExpired => DateTime.UtcNow > ExpiresAt;
    public bool IsFull => Participants.Count >= MAX_PLAYERS;
    public int PlayerCount => Participants.Count;

    public RoomParticipant? AddParticipant(string connectionId, bool isHost)
    {
        lock (_lock)
        {
            if (IsFull && !isHost) return null;

            int slot;
            if (isHost)
            {
                slot = 1;
                _usedSlots.Add(1);
            }
            else
            {
                slot = -1;
                for (int i = 2; i <= MAX_PLAYERS; i++)
                {
                    if (!_usedSlots.Contains(i))
                    {
                        slot = i;
                        _usedSlots.Add(i);
                        break;
                    }
                }
                if (slot == -1) return null;
            }

            var participant = new RoomParticipant
            {
                ConnectionId = connectionId,
                VirtualIp = $"10.77.0.{slot}",
                IpSlot = slot,
                IsHost = isHost
            };

            Participants[connectionId] = participant;
            return participant;
        }
    }

    public RoomParticipant? RemoveParticipant(string connectionId)
    {
        lock (_lock)
        {
            if (Participants.TryRemove(connectionId, out var p))
            {
                _usedSlots.Remove(p.IpSlot);
                return p;
            }
            return null;
        }
    }
}

/// <summary>
/// In-memory room manager supporting up to 16 players per room.
/// </summary>
public class RoomManager
{
    private readonly ConcurrentDictionary<string, Room> _rooms = new();
    private readonly ConcurrentDictionary<string, WebSocket> _sockets = new();
    private readonly Timer _cleanupTimer;

    private const string CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    private const int CODE_LENGTH = 5;

    public RoomManager()
    {
        _cleanupTimer = new Timer(_ => CleanupExpiredRooms(), null,
            TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

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

    public async Task BroadcastToRoomOthersAsync(string senderConnectionId, string roomId, object message)
    {
        var room = GetRoom(roomId);
        if (room == null) return;

        foreach (var p in room.Participants.Values)
        {
            if (p.ConnectionId != senderConnectionId)
            {
                await SendToConnectionAsync(p.ConnectionId, message);
            }
        }
    }

    public async Task BroadcastToAllInRoomAsync(string roomId, object message)
    {
        var room = GetRoom(roomId);
        if (room == null) return;

        foreach (var p in room.Participants.Values)
        {
            await SendToConnectionAsync(p.ConnectionId, message);
        }
    }

    public RoomParticipant CreateRoom(string connectionId, out Room room)
    {
        var id = GenerateRoomId();
        room = new Room
        {
            Id = id,
            HostConnectionId = connectionId
        };

        var host = room.AddParticipant(connectionId, isHost: true)!;
        _rooms[id] = room;
        Log.Information("[RoomManager] Created room {RoomId} for host {ConnectionId} (IP: {Ip})",
            id, connectionId, host.VirtualIp);
        return host;
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

    public bool TryJoinRoom(string roomId, string connectionId, out Room? room, out RoomParticipant? participant)
    {
        room = GetRoom(roomId.ToUpperInvariant());
        participant = null;

        if (room == null)
        {
            Log.Warning("[RoomManager] Room {RoomId} not found", roomId);
            return false;
        }

        if (room.IsFull)
        {
            Log.Warning("[RoomManager] Room {RoomId} is full (16 players)", roomId);
            return false;
        }

        participant = room.AddParticipant(connectionId, isHost: false);
        if (participant == null) return false;

        Log.Information("[RoomManager] Player {ConnectionId} joined room {RoomId} (Assigned IP: {Ip}, Total: {Count}/16)",
            connectionId, roomId, participant.VirtualIp, room.PlayerCount);
        return true;
    }

    public void CloseRoom(string roomId)
    {
        _rooms.TryRemove(roomId.ToUpperInvariant(), out _);
        Log.Information("[RoomManager] Room {RoomId} closed", roomId);
    }

    public Room? FindRoomByConnection(string connectionId)
    {
        return _rooms.Values.FirstOrDefault(r => r.Participants.ContainsKey(connectionId));
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
