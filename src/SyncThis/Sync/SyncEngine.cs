using System.Collections.Concurrent;
using SyncThis.ConflictResolution;
using SyncThis.Core;
using SyncThis.Discovery;
using SyncThis.Serialization;
using SyncThis.Transport;

namespace SyncThis.Sync;

public class SyncEngine : IDisposable
{
    private static readonly Lazy<SyncEngine> _instance = new(() => new SyncEngine());
    public static SyncEngine Instance => _instance.Value;

    private readonly ConcurrentDictionary<Guid, SyncedObject> _tracked = new();
    private readonly ConcurrentDictionary<Guid, List<Delegate>> _observers = new();
    private readonly ConcurrentDictionary<Guid, ConcurrentDictionary<Guid, int>> _clientDelivery = new();
    private readonly SyncConfig _config;
    private readonly IPeerDiscovery _discovery;
    private readonly ITransport _transport;
    private readonly IMessageRouter _router;
    private readonly IDelta _deltaTracker;
    private readonly IConflictResolver _conflictResolver;
    private readonly Guid _nodeId;
    private CancellationTokenSource? _cts;
    private Task? _pollTask;
    private bool _started;
    private bool _disposed;

    public SyncEngine() : this(new SyncConfig()) { }

    public SyncEngine(SyncConfig config)
    {
        _nodeId = Guid.NewGuid();
        _config = config;
        switch (config.Topology)
        {
            case SyncTopology.P2P:
                _discovery = new TcpPeerDiscovery(_config, _nodeId);
                _transport = new UdpUnicastTransport(_config, _nodeId, () =>
                {
                    var peers = _discovery.GetPeers();
                    return peers.IsSuccess ? peers.Value : [];
                });
                break;
            case SyncTopology.P2PBroadcast:
                _discovery = new UdpBroadcastDiscovery(_config, _nodeId);
                _transport = new UdpBroadcastTransport(_config, _nodeId);
                break;
            case SyncTopology.ClientServer:
                _discovery = new UdpMulticastDiscovery(_config, _nodeId);
                _transport = new TcpUnicastTransport(_config, _nodeId);
                break;
            case SyncTopology.P2PMulticast:
                _discovery = new UdpMulticastDiscovery(_config, _nodeId);
                _transport = new UdpMulticastTransport(_config, _nodeId);
                break;
        }
        _router = config.Topology == SyncTopology.ClientServer
            ? new ClientServerRouter(_transport, _discovery, _config, _nodeId)
            : new P2pRouter(_transport);
        _deltaTracker = new MessagePackDeltaSerializer();
        _conflictResolver = new LastWriteWinsResolver();
        _router.Accept(OnMessageReceived);
    }

    public Result Start()
    {
        if (_started) return Result.Success();

        var discResult = _discovery.Start();
        if (discResult.IsFailure) return discResult;

        var transResult = _transport.Start();
        if (transResult.IsFailure)
        {
            _discovery.Stop();
            return transResult;
        }

        _started = true;
        _cts = new CancellationTokenSource();
        _pollTask = PollForChanges(_cts.Token);
        return Result.Success();
    }

    public Result Stop()
    {
        if (!_started) return Result.Success();
        _cts?.Cancel();
        _pollTask?.GetAwaiter().GetResult();
        _transport.Stop();
        _discovery.Stop();
        _tracked.Clear();
        _observers.Clear();
        _clientDelivery.Clear();
        _started = false;
        return Result.Success();
    }

    public Result Register(object obj)
    {
        if (obj is not ISyncable)
            return Result.Failure("NOT_SYNCABLE", $"Type {obj.GetType().Name} does not implement ISyncable.");

        var synced = new SyncedObject(obj);
        synced.SetSnapshot(Clone(obj));
        if (_tracked.TryAdd(synced.SyncId, synced))
        {
            var initialResult = SendFullSnapshot(synced, synced.Version);
            if (initialResult.IsSuccess)
                synced.InitialSyncSent = true;
        }
        return Result.Success();
    }

    public Result Unregister(Guid syncId)
    {
        _tracked.TryRemove(syncId, out _);
        _observers.TryRemove(syncId, out _);
        foreach (var perClient in _clientDelivery.Values)
            perClient.TryRemove(syncId, out _);
        return Result.Success();
    }

    public void Observe<T>(Action<T> handler)
    {
        var key = typeof(T).GUID;
        _observers.AddOrUpdate(key,
            _ => [handler],
            (_, list) => { list.Add(handler); return list; });
    }

    private void OnMessageReceived(Message msg)
    {
        if (!_tracked.TryGetValue(msg.SyncId, out var local))
        {
            if (msg.Type == MessageType.FullSnapshot)
            {
                var type = ResolveType(msg.TypeName);
                if (type is null)
                    return;

                var restored = _deltaTracker.FromSnapshot(msg.Payload, type);
                if (restored.IsSuccess && restored.Value is ISyncable syncable)
                {
                    var synced = new SyncedObject(restored.Value);
                    synced.Version = msg.SyncVersion;
                    synced.InitialSyncSent = true;
                    synced.SetSnapshot(Clone(restored.Value));
                    _tracked.TryAdd(msg.SyncId, synced);
                    NotifyObservers(restored.Value);
                    RelayRemoteMessage(msg);
                }
            }
            return;
        }

        if (msg.Type == MessageType.Delta)
        {
            var resolved = _conflictResolver.Resolve(local.Version, msg.SyncVersion,
                () => _deltaTracker.ApplyDelta(local.Instance, msg.Payload, local.Type));
            if (resolved.IsSuccess)
            {
                local.Version = msg.SyncVersion;
                local.SetSnapshot(Clone(local.Instance));
                if (local.Instance is ISyncable s)
                    s.SyncVersion = msg.SyncVersion;
                NotifyObservers(local.Instance);
                RelayRemoteMessage(msg);
            }
        }
        else if (msg.Type == MessageType.FullSnapshot)
        {
            var restored = _deltaTracker.FromSnapshot(msg.Payload, local.Type);
            if (restored.IsSuccess)
            {
                foreach (var prop in local.Type.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                             .Where(p => p.CanWrite))
                {
                    var val = prop.GetValue(restored.Value);
                    prop.SetValue(local.Instance, val);
                }
                local.Version = msg.SyncVersion;
                local.SetSnapshot(Clone(local.Instance));
                if (local.Instance is ISyncable s)
                    s.SyncVersion = msg.SyncVersion;
                NotifyObservers(local.Instance);
                RelayRemoteMessage(msg);
            }
        }
    }

    private void RelayRemoteMessage(Message msg)
    {
        if (_router.NeedsRelay)
            _router.Relay(msg);
    }

    private async Task PollForChanges(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            foreach (var (_, synced) in _tracked)
            {
                if (!synced.InitialSyncSent)
                {
                    if (SendFullSnapshot(synced, synced.Version).IsSuccess)
                        synced.InitialSyncSent = true;
                    continue;
                }

                if (!synced.HasChanges()) continue;

                var previous = synced.GetSnapshot<object>();
                var delta = _deltaTracker.ComputeDelta(synced.Instance, previous);
                if (delta.IsFailure) continue;

                synced.Version++;
                if (synced.Instance is ISyncable s)
                    s.SyncVersion = synced.Version;
                synced.SetSnapshot(Clone(synced.Instance));

                var msgType = synced.Version % _config.FullSnapshotEveryNVersions == 0
                    ? MessageType.FullSnapshot
                    : MessageType.Delta;

                var msg = new Message
                {
                    SyncId = synced.SyncId,
                    SyncVersion = synced.Version,
                    Type = msgType,
                    TypeName = synced.Type.AssemblyQualifiedName,
                    Payload = msgType == MessageType.FullSnapshot
                        ? _deltaTracker.TakeSnapshot(synced.Instance).Value
                        : delta.Value
                };
                _router.Route(msg);
            }

            ReconcileWithClients();

            try
            {
                await Task.Delay(500, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private void ReconcileWithClients()
    {
        if (_config.Role != SyncRole.Server)
            return;

        var peers = _discovery.GetPeers();
        if (peers.IsFailure)
            return;

        var clients = peers.Value
            .Where(p => p.Role == SyncRole.Client && p.NodeId != _nodeId)
            .ToList();

        foreach (var staleId in _clientDelivery.Keys.Where(k => clients.All(c => c.NodeId != k)).ToList())
            _clientDelivery.TryRemove(staleId, out _);

        foreach (var (_, synced) in _tracked)
        {
            foreach (var client in clients)
            {
                var delivered = GetDeliveredVersion(client.NodeId, synced.SyncId);
                if (delivered >= synced.Version)
                    continue;

                if (SendFullSnapshot(synced, synced.Version, client).IsSuccess)
                    SetDeliveredVersion(client.NodeId, synced.SyncId, synced.Version);
            }
        }
    }

    private int GetDeliveredVersion(Guid clientId, Guid syncId)
    {
        return _clientDelivery.TryGetValue(clientId, out var perClient)
            && perClient.TryGetValue(syncId, out var version)
            ? version
            : -1;
    }

    private void SetDeliveredVersion(Guid clientId, Guid syncId, int version)
    {
        var perClient = _clientDelivery.GetOrAdd(clientId, _ => new ConcurrentDictionary<Guid, int>());
        perClient[syncId] = version;
    }

    private Result SendFullSnapshot(SyncedObject synced, int version, NodeInfo? target = null)
    {
        var snapshot = _deltaTracker.TakeSnapshot(synced.Instance);
        if (snapshot.IsFailure)
            return Result.Failure(snapshot.Error);

        var msg = new Message
        {
            SyncId = synced.SyncId,
            SyncVersion = version,
            Type = MessageType.FullSnapshot,
            Payload = snapshot.Value,
            TypeName = synced.Type.AssemblyQualifiedName
        };
        return target is null
            ? _router.Route(msg)
            : _transport.Send(msg, target);
    }

    private void NotifyObservers(object obj)
    {
        var type = obj.GetType();
        if (_observers.TryGetValue(type.GUID, out var handlers))
        {
            foreach (var handler in handlers)
                (handler as Delegate)?.DynamicInvoke(obj);
        }
    }

    private object Clone(object obj)
    {
        var snapshot = _deltaTracker.TakeSnapshot(obj);
        if (snapshot.IsFailure) return obj;
        var restored = _deltaTracker.FromSnapshot(snapshot.Value, obj.GetType());
        return restored.IsSuccess ? restored.Value : obj;
    }

    private static Type? ResolveType(string? typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return null;

        var type = Type.GetType(typeName);
        if (type is not null)
            return type;

        var commaIndex = typeName.IndexOf(',');
        var simpleTypeName = commaIndex > 0 ? typeName[..commaIndex] : typeName;

        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            type = asm.GetType(simpleTypeName);
            if (type is not null)
                return type;
        }

        return null;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Stop();
        _transport.Dispose();
        _discovery.Dispose();
        _router.Dispose();
        _cts?.Dispose();
    }
}