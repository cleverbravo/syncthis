using SyncThis.Core;

namespace SyncThis.Transport;

public interface ITransport : IDisposable
{
    event Action<Message>? MessageReceived;
    Result Start();
    Result Stop();
    Result Send(Message message, NodeInfo recipient);
    Result Broadcast(Message message);
}
