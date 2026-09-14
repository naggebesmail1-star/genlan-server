using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Serilog;

namespace GenLAN.Server.Relay;

/// <summary>
/// UDP relay server - forwards encrypted packets between peers when P2P hole punching fails.
/// Does NOT decrypt packets - maintains end-to-end encryption.
/// 
/// Protocol:
///   [4 bytes session_id] [4 bytes sender_peer_id] [remaining: encrypted tunnel packet]
/// 
/// Usage:
///   1. Both peers connect to relay UDP endpoint
///   2. Send HELLO packet with session_id
///   3. Relay maps session_id → two endpoints
///   4. Relay forwards all subsequent packets to the OTHER peer in the session
/// </summary>
public class RelayServer
{
    private const int RELAY_PORT = 7778;
    private const int SESSION_TIMEOUT_MINUTES = 60;

    private readonly ConcurrentDictionary<uint, RelaySession> _sessions = new();
    private UdpClient? _udp;
    private bool _running;

    public async Task StartAsync()
    {
        _udp = new UdpClient(RELAY_PORT);
        _running = true;

        Log.Information("[Relay] UDP relay server started on port {Port}", RELAY_PORT);

        // Session cleanup timer
        _ = Task.Run(async () =>
        {
            while (_running)
            {
                await Task.Delay(TimeSpan.FromMinutes(5));
                CleanupExpiredSessions();
            }
        });

        await ReceiveLoopAsync();
    }

    private async Task ReceiveLoopAsync()
    {
        while (_running)
        {
            try
            {
                var result = await _udp!.ReceiveAsync();
                _ = ProcessPacketAsync(result.Buffer, result.RemoteEndPoint);
            }
            catch (Exception ex) when (_running)
            {
                Log.Warning(ex, "[Relay] Receive error");
            }
        }
    }

    private async Task ProcessPacketAsync(byte[] data, IPEndPoint senderEp)
    {
        if (data.Length < 8) return; // Minimum: 4 session + 4 peer

        var sessionId = BitConverter.ToUInt32(data, 0);
        var peerId    = BitConverter.ToUInt32(data, 4);
        var payload   = data.AsSpan(8).ToArray();

        // HELLO packet: peer registers with session
        if (payload.Length == 4 && payload[0] == 'H' && payload[1] == 'E')
        {
            RegisterPeer(sessionId, peerId, senderEp);
            return;
        }

        // Forward to other peer in session
        if (_sessions.TryGetValue(sessionId, out var session))
        {
            session.LastActivity = DateTime.UtcNow;
            var targetEp = session.GetOtherPeer(peerId);

            if (targetEp != null)
            {
                // Forward the entire original packet (keep session + peer header for debugging)
                await _udp!.SendAsync(data, targetEp);
            }
        }
    }

    private void RegisterPeer(uint sessionId, uint peerId, IPEndPoint ep)
    {
        var session = _sessions.GetOrAdd(sessionId, _ => new RelaySession(sessionId));
        session.RegisterPeer(peerId, ep);
        Log.Debug("[Relay] Peer {PeerId} registered in session {SessionId} from {Ep}", peerId, sessionId, ep);
    }

    private void CleanupExpiredSessions()
    {
        var cutoff = DateTime.UtcNow.AddMinutes(-SESSION_TIMEOUT_MINUTES);
        var expired = _sessions.Where(kv => kv.Value.LastActivity < cutoff).Select(kv => kv.Key).ToList();
        foreach (var id in expired)
        {
            _sessions.TryRemove(id, out _);
            Log.Debug("[Relay] Session {Id} expired", id);
        }
    }
}

public class RelaySession
{
    public uint SessionId { get; }
    public DateTime CreatedAt { get; } = DateTime.UtcNow;
    public DateTime LastActivity { get; set; } = DateTime.UtcNow;

    private readonly Dictionary<uint, IPEndPoint> _peers = new();
    private readonly object _lock = new();

    public RelaySession(uint sessionId) => SessionId = sessionId;

    public void RegisterPeer(uint peerId, IPEndPoint ep)
    {
        lock (_lock) { _peers[peerId] = ep; }
    }

    public IPEndPoint? GetOtherPeer(uint senderId)
    {
        lock (_lock)
        {
            return _peers.Where(kv => kv.Key != senderId).Select(kv => kv.Value).FirstOrDefault();
        }
    }
}
