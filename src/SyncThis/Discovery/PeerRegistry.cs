using System.Collections.Concurrent;
using SyncThis.Core;

namespace SyncThis.Discovery;

public class PeerRegistry
{
    private readonly ConcurrentDictionary<Guid, NodeInfo> _peers = new();
    private readonly TimeSpan _timeout;

    public event Action<NodeInfo>? PeerDiscovered;
    public event Action<NodeInfo>? PeerLost;

    public PeerRegistry(TimeSpan timeout)
    {
        _timeout = timeout;
    }

    public void AddOrUpdate(NodeInfo node)
    {
        node.LastSeen = DateTime.UtcNow;
        var isNew = !_peers.ContainsKey(node.NodeId);
        _peers[node.NodeId] = node;
        if (isNew)
            PeerDiscovered?.Invoke(node);
    }

    public void Remove(Guid nodeId)
    {
        if (_peers.TryRemove(nodeId, out var removed))
            PeerLost?.Invoke(removed);
    }

    public IReadOnlyCollection<NodeInfo> GetActivePeers()
    {
        var now = DateTime.UtcNow;
        var expired = _peers.Values.Where(p => now - p.LastSeen > _timeout).ToList();
        foreach (var e in expired)
            Remove(e.NodeId);
        return _peers.Values.ToList().AsReadOnly();
    }

    public NodeInfo? Get(Guid nodeId) =>
        _peers.TryGetValue(nodeId, out var node) ? node : null;

    public void Clear()
    {
        var keys = _peers.Keys.ToList();
        foreach (var k in keys)
            Remove(k);
    }
}