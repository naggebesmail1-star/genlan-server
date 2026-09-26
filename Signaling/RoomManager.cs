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
    // Hard ceiling of the 10.77.0.0/24 virtual subnet: slots 1..254
    // (1 = host, 2..254 = the remaining /24 hosts; .255 is the broadcast address).
    public const int MAX_PLAYERS = 254;

    public string Id { get; set; } = "";
    public string HostConnectionId { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public ConcurrentDictionary<string, RoomParticipant> Participants { get; } = new();
    private readonly HashSet<int> _usedSlots = new();
    private readonly object _lock = new();

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
/// In-memory room manager. Rooms live until their host disconnects (or the host
/// closes the app): there is no time-based expiry.
/// </summary>
public class RoomManager
{
    private readonly ConcurrentDictionary<string, Room> _rooms = new();
    private readonly ConcurrentDictionary<string, WebSocket> _sockets = new();

    private const string CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    private const int CODE_LENGTH = 5;

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
            Log.Warning("[RoomManager] Room {RoomId} is full ({MaxPlayers} players)", roomId, Room.MAX_PLAYERS);
            return false;
        }

        participant = room.AddParticipant(connectionId, isHost: false);
        if (participant == null) return false;

        Log.Information("[RoomManager] Player {ConnectionId} joined room {RoomId} (Assigned IP: {Ip}, Total: {Count}/{MaxPlayers})",
            connectionId, roomId, participant.VirtualIp, room.PlayerCount, Room.MAX_PLAYERS);
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
}
