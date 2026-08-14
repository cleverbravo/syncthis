using System.Net;
using System.Net.Sockets;
using MessagePack;
using SyncThis.Core;

namespace SyncThis.Transport;

public class UdpUnicastTransport : ITransport
{
    private readonly SyncConfig _config;
    private readonly Guid _nodeId;
    private readonly Func<IReadOnlyCollection<NodeInfo>> _getPeers;
    private readonly MessagePackSerializerOptions _options;
    private UdpClient? _udpClient;
    private CancellationTokenSource? _cts;
    private Task? _listenTask;
    private bool _disposed;

    public event Action<Message>? MessageReceived;

    public UdpUnicastTransport(SyncConfig config, Guid nodeId, Func<IReadOnlyCollection<NodeInfo>> getPeers)
    {
        _config = config;
        _nodeId = nodeId;
        _getPeers = getPeers;
        _options = MessagePack.Resolvers.ContractlessStandardResolver.Options;
    }

    public Result Start()
    {
        try
        {
            _udpClient = new UdpClient(_config.ListenPort + 1);
            _cts = new CancellationTokenSource();
            _listenTask = ListenLoop(_cts.Token);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("TRANSPORT_START_FAILED", ex.Message);
        }
    }

    public Result Stop()
    {
        try
        {
            _cts?.Cancel();
            _listenTask?.GetAwaiter().GetResult();
            _udpClient?.Close();
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("TRANSPORT_STOP_FAILED", ex.Message);
        }
    }

    public Result Send(Message message, NodeInfo recipient)
    {
        if (recipient.IPAddress is null)
            return Result.Failure("TRANSPORT_SEND_FAILED", $"Peer {recipient.NodeId} has no known address.");
        return SendInternal(message, new IPEndPoint(recipient.IPAddress, recipient.ListenPort + 1));
    }

    public Result Broadcast(Message message)
    {
        var peers = _getPeers();
        if (peers.Count == 0)
            return Result.Failure("NO_PEERS", "No known peers to broadcast to.");
        foreach (var peer in peers)
        {
            var result = Send(message, peer);
            if (result.IsFailure)
                return result;
        }
        return Result.Success();
    }

    private Result SendInternal(Message message, IPEndPoint endpoint)
    {
        try
        {
            message.SenderId = _nodeId;
            var bytes = MessagePack.MessagePackSerializer.Serialize(message, _options);
            _udpClient?.Send(bytes, bytes.Length, endpoint);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("TRANSPORT_SEND_FAILED", ex.Message);
        }
    }

    private async Task ListenLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await _udpClient!.ReceiveAsync(ct);
                var message = MessagePack.MessagePackSerializer.Deserialize<Message>(result.Buffer, _options);
                if (message.SenderId != _nodeId)
                    MessageReceived?.Invoke(message);
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