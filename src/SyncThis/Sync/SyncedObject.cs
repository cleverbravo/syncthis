using SyncThis.Core;

namespace SyncThis.Sync;

public class SyncedObject
{
    private readonly object _lock = new();
    private object? _snapshot;

    public Guid SyncId { get; }
    public object Instance { get; }
    public Type Type { get; }
    public int Version { get; set; }
    public bool InitialSyncSent { get; set; }

    public SyncedObject(object instance)
    {
        Instance = instance;
        Type = instance.GetType();
        SyncId = instance is ISyncable s ? s.SyncId : Guid.NewGuid();
        Version = instance is ISyncable sv ? sv.SyncVersion : 0;
    }

    public T GetSnapshot<T>()
    {
        lock (_lock) { return (T)_snapshot!; }
    }

    public void SetSnapshot(object snapshot)
    {
        lock (_lock) { _snapshot = snapshot; }
    }

    public bool HasChanges()
    {
        lock (_lock)
        {
            if (_snapshot is null) return true;
            var props = Type.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                .Where(p => p.CanRead && p.Name != nameof(ISyncable.SyncVersion));
            foreach (var prop in props)
            {
                var cur = prop.GetValue(Instance);
                var prev = prop.GetValue(_snapshot);
                if (!Equals(cur, prev)) return true;
            }
            return false;
        }
    }
}
