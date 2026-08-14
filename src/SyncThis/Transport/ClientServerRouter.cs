using SyncThis.Core;
using SyncThis.Discovery;

namespace SyncThis.Transport;

public class ClientServerRouter : IMessageRouter
{
    private readonly ITransport _transport;
    private readonly IPeerDiscovery _discovery;
    private readonly SyncConfig _config;
    private readonly Guid _nodeId;
    private Action<Message>? _handler;

    public bool NeedsRelay => _config.Role == SyncRole.Server;

    public ClientServerRouter(ITransport transport, IPeerDiscovery discovery, SyncConfig config, Guid nodeId)
    {
        _transport = transport;
        _discovery = discovery;
        _config = config;
        _nodeId = nodeId;
        _transport.MessageReceived += OnTransportMessage;
    }

    public Result Route(Message message)
    {
        if (_config.Role == SyncRole.Server)
            return SendToClients(message, null);

        var server = FindServer();
        if (server is null)
            return Result.Failure("SERVER_NOT_FOUND", "No server discovered on the LAN.");
        return _transport.Send(message, server);
    }

    public Result Relay(Message message)
    {
        if (_config.Role != SyncRole.Server)
            return Result.Success();

        var originalSender = message.SenderId;
        return SendToClients(message, originalSender);
    }

    public void Accept(Action<Message> handler) => _handler = handler;

    public void Dispose()
    {
        _transport.MessageReceived -= OnTransportMessage;
    }

    private void OnTransportMessage(Message msg)
    {
        if (_config.Role == SyncRole.Client)
        {
            var server = FindServer();
            if (server is null || msg.SenderId != server.NodeId)
                return;
        }
        _handler?.Invoke(msg);
    }

    private NodeInfo? FindServer()
    {
        var peers = _discovery.GetPeers();
        if (peers.IsFailure)
            return null;
        return peers.Value.FirstOrDefault(p => p.Role == SyncRole.Server);
    }

    private Result SendToClients(Message message, Guid? exceptNodeId)
    {
        var peers = _discovery.GetPeers();
        if (peers.IsFailure)
            return Result.Failure(peers.Error);

        foreach (var peer in peers.Value)
        {
            if (peer.Role != SyncRole.Client) continue;
            if (peer.NodeId == _nodeId) continue;
            if (exceptNodeId.HasValue && peer.NodeId == exceptNodeId.Value) continue;

            var sendResult = _transport.Send(message, peer);
            if (sendResult.IsFailure)
                return sendResult;
        }
        return Result.Success();
    }
}