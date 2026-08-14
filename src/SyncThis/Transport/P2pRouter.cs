using SyncThis.Core;

namespace SyncThis.Transport;

public class P2pRouter : IMessageRouter
{
    private readonly ITransport _transport;

    public bool NeedsRelay => false;

    public P2pRouter(ITransport transport)
    {
        _transport = transport;
    }

    public Result Route(Message message) => _transport.Broadcast(message);

    public Result Relay(Message message) => Result.Success();

    public void Accept(Action<Message> handler) => _transport.MessageReceived += handler;

    public void Dispose() { }
}