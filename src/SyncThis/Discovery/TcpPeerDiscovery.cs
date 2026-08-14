using System.Net;
using System.Net.Sockets;
using MessagePack;
using SyncThis.Core;

namespace SyncThis.Discovery;

public class TcpPeerDiscovery : IPeerDiscovery
{
    private const int MaxFrameSize = 16 * 1024 * 1024;

    private readonly SyncConfig _config;
    private readonly Guid _nodeId;
    private readonly PeerRegistry _registry;
    private readonly MessagePackSerializerOptions _options;
    private TcpListener? _listener;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private Task? _refreshTask;
    private bool _disposed;

    public event Action<NodeInfo>? PeerDiscovered;
    public event Action<NodeInfo>? PeerLost;

    public TcpPeerDiscovery(SyncConfig config, Guid nodeId)
    {
        _config = config;
        _nodeId = nodeId;
        _registry = new PeerRegistry(TimeSpan.FromMilliseconds(config.PeerTimeoutMs));
        _registry.PeerDiscovered += n => PeerDiscovered?.Invoke(n);
        _registry.PeerLost += n => PeerLost?.Invoke(n);
        _options = MessagePack.Resolvers.ContractlessStandardResolver.Options;
    }

    public Result<IReadOnlyCollection<NodeInfo>> GetPeers()
    {
        try
        {
            return Result<IReadOnlyCollection<NodeInfo>>.Success(_registry.GetActivePeers());
        }
        catch (Exception ex)
        {
            return Result<IReadOnlyCollection<NodeInfo>>.Failure("PEER_GET_FAILED", ex.Message);
        }
    }

    public Result Start()
    {
        try
        {
            _listener = new TcpListener(IPAddress.Any, _config.ListenPort);
            _listener.Start();
            _cts = new CancellationTokenSource();
            _acceptTask = AcceptLoop(_cts.Token);
            _refreshTask = RefreshLoop(_cts.Token);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("DISCOVERY_START_FAILED", ex.Message);
        }
    }

    public Result Stop()
    {
        try
        {
            _cts?.Cancel();
            _acceptTask?.GetAwaiter().GetResult();
            _refreshTask?.GetAwaiter().GetResult();
            _listener?.Stop();
            _registry.Clear();
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("DISCOVERY_STOP_FAILED", ex.Message);
        }
    }

    private NodeInfo BuildHello() => new()
    {
        NodeId = _nodeId,
        HostName = Environment.MachineName,
        ListenPort = _config.ListenPort,
        Role = _config.Role
    };

    private async Task AcceptLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var client = await _listener!.AcceptTcpClientAsync(ct);
                _ = HandleIncoming(client, ct);
            }
            catch (OperationCanceledException) { break; }
            catch { }
        }
    }

    private async Task HandleIncoming(TcpClient client, CancellationToken ct)
    {
        try
        {
            using (client)
            using (var stream = client.GetStream())
            {
                var hello = await ReadMessage<NodeInfo>(stream, ct);
                if (hello is null || hello.NodeId == _nodeId)
                    return;

                if (client.Client.RemoteEndPoint is IPEndPoint remote)
                    hello.IPAddress = PeerAddressResolver.Normalize(remote.Address);
                _registry.AddOrUpdate(hello);

                var reply = new PeerList { Peers = _registry.GetActivePeers().Where(p => p.NodeId != _nodeId).ToList() };
                await WriteMessage(stream, reply, ct);
            }
        }
        catch { }
    }

    private async Task RefreshLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await DialAll(ct);
            }
            catch { }
            try
            {
                await Task.Delay(_config.HeartbeatIntervalMs, ct);
            }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task DialAll(CancellationToken ct)
    {
        var targets = ResolveTargets();
        foreach (var target in targets)
        {
            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync(target, ct);
                using var stream = client.GetStream();
                await WriteMessage(stream, BuildHello(), ct);
                var reply = await ReadMessage<PeerList>(stream, ct);
                if (reply is null) continue;

                foreach (var peer in reply.Peers)
                {
                    if (peer.NodeId == _nodeId) continue;
                    peer.IPAddress ??= ResolveAddress(peer.HostName);
                    _registry.AddOrUpdate(peer);
                }
            }
            catch { }
        }
    }

    private IEnumerable<IPEndPoint> ResolveTargets()
    {
        var targets = new List<IPEndPoint>();

        foreach (var seed in _config.SeedPeers)
        {
            var endpoint = ParseSeed(seed);
            if (endpoint is not null)
                targets.Add(endpoint);
        }

        foreach (var peer in _registry.GetActivePeers())
        {
            if (peer.IPAddress is not null)
                targets.Add(new IPEndPoint(peer.IPAddress, peer.ListenPort));
            else
            {
                var address = ResolveAddress(peer.HostName);
                if (address is not null)
                    targets.Add(new IPEndPoint(address, peer.ListenPort));
            }
        }

        return targets;
    }

    private static IPEndPoint? ParseSeed(string hostPort)
    {
        var parts = hostPort.Split(':');
        if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
            return null;
        return ResolveEndpoint(parts[0], port);
    }

    private static IPEndPoint? ResolveEndpoint(string host, int port)
    {
        if (IPAddress.TryParse(host, out var ip))
            return new IPEndPoint(ip, port);
        return ResolveAddress(host) is { } address ? new IPEndPoint(address, port) : null;
    }

    private static IPAddress? ResolveAddress(string host)
    {
        try
        {
            var addresses = Dns.GetHostAddresses(host);
            return addresses.FirstOrDefault();
        }
        catch
        {
            return null;
        }
    }

    private static async Task WriteMessage<T>(Stream stream, T message, CancellationToken ct)
    {
        var payload = MessagePack.MessagePackSerializer.Serialize(message);
        var header = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(payload.Length));
        await stream.WriteAsync(header, ct);
        await stream.WriteAsync(payload, ct);
        await stream.FlushAsync(ct);
    }

    private static async Task<T?> ReadMessage<T>(Stream stream, CancellationToken ct)
    {
        var header = new byte[4];
        await ReadExactly(stream, header, 4, ct);
        var length = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(header));
        if (length <= 0 || length > MaxFrameSize)
            return default;

        var payload = new byte[length];
        await ReadExactly(stream, payload, length, ct);
        return MessagePack.MessagePackSerializer.Deserialize<T>(payload);
    }

    private static async Task ReadExactly(Stream stream, byte[] buffer, int count, CancellationToken ct)
    {
        var read = 0;
        while (read < count)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(read, count - read), ct);
            if (n == 0) throw new EndOfStreamException();
            read += n;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Stop();
        _listener = null;
        _cts?.Dispose();
    }
}