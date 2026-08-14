using System.Net;
using System.Net.Sockets;
using MessagePack;
using SyncThis.Core;

namespace SyncThis.Discovery;

public class UdpBroadcastDiscovery : IPeerDiscovery
{
    private readonly SyncConfig _config;
    private readonly Guid _nodeId;
    private readonly PeerRegistry _registry;
    private readonly MessagePackSerializerOptions _serializerOptions;
    private UdpClient? _udpClient;
    private CancellationTokenSource? _cts;
    private Task? _listenTask;
    private bool _disposed;

    public event Action<NodeInfo>? PeerDiscovered;
    public event Action<NodeInfo>? PeerLost;

    public UdpBroadcastDiscovery(SyncConfig config, Guid nodeId)
    {
        _config = config;
        _nodeId = nodeId;
        _registry = new PeerRegistry(TimeSpan.FromMilliseconds(config.PeerTimeoutMs));
        _registry.PeerDiscovered += n => PeerDiscovered?.Invoke(n);
        _registry.PeerLost += n => PeerLost?.Invoke(n);
        _serializerOptions = MessagePack.Resolvers.ContractlessStandardResolver.Options;
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
            _udpClient = new UdpClient(AddressFamily.InterNetwork);
            _udpClient.ExclusiveAddressUse = false;
            _udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _udpClient.Client.Bind(new IPEndPoint(IPAddress.Any, _config.MulticastPort));

            _cts = new CancellationTokenSource();
            _listenTask = ListenLoop(_cts.Token);
            BroadcastHeartbeat();
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
            _listenTask?.GetAwaiter().GetResult();
            _udpClient?.Close();
            _registry.Clear();
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("DISCOVERY_STOP_FAILED", ex.Message);
        }
    }

    private void BroadcastHeartbeat()
    {
        var heartbeat = new NodeInfo
        {
            NodeId = _nodeId,
            HostName = Environment.MachineName,
            ListenPort = _config.ListenPort,
            Role = _config.Role
        };
        var bytes = MessagePack.MessagePackSerializer.Serialize(heartbeat, _serializerOptions);
        _ = Task.Run(async () =>
        {
            while (_cts?.IsCancellationRequested == false)
            {
                try
                {
                    await _udpClient!.SendAsync(bytes, bytes.Length, new IPEndPoint(_config.BroadcastAddress!, _config.MulticastPort));
                    await Task.Delay(_config.HeartbeatIntervalMs, _cts.Token);
                }
                catch
                {
                    break;
                }
            }
        });
    }

    private async Task ListenLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await _udpClient!.ReceiveAsync(ct);
                var node = MessagePack.MessagePackSerializer.Deserialize<NodeInfo>(result.Buffer, _serializerOptions);
                node.IPAddress = PeerAddressResolver.Normalize(result.RemoteEndPoint.Address);
                if (node.NodeId != _nodeId)
                    _registry.AddOrUpdate(node);
            }
            catch (OperationCanceledException) { break; }
            catch { }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Stop();
        _udpClient?.Dispose();
        _cts?.Dispose();
    }
}
