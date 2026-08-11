using SyncThis.Core;

namespace SyncThis.Discovery;

public interface IPeerDiscovery : IDisposable
{
    event Action<NodeInfo>? PeerDiscovered;
    event Action<NodeInfo>? PeerLost;
    Result<IReadOnlyCollection<NodeInfo>> GetPeers();
    Result Start();
    Result Stop();
}
