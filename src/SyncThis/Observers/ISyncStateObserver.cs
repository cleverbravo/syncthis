using SyncThis.Core;

namespace SyncThis.Observers;

public interface ISyncStateObserver
{
    void OnStateChanged<T>(T updatedObject) where T : ISyncable;
    void OnPeerDiscovered(NodeInfo peer);
    void OnPeerLost(NodeInfo peer);
}
