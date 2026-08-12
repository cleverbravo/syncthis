using SyncThis.Core;

namespace SyncThis.Transport;

public interface IMessageRouter : IDisposable
{
    bool NeedsRelay { get; }
    Result Route(Message message);
    Result Relay(Message message);
    void Accept(Action<Message> handler);
}
