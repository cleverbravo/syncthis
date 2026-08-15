using SyncThis.Core;

namespace SyncThis.Sync;

public class Sync
{
    private readonly SyncEngine _engine;

    public Sync() : this(SyncEngine.Instance) { }

    public Sync(SyncEngine engine)
    {
        _engine = engine;
        _engine.Start();
    }

    public Result SyncThis(ISyncable obj)
    {
        return _engine.Register(obj);
    }

    public Result StopSync(Guid syncId)
    {
        return _engine.Unregister(syncId);
    }

    public void OnUpdate<T>(Action<T> handler) where T : ISyncable
    {
        _engine.Observe(handler);
    }

    public Result Start() => _engine.Start();
    public Result Stop() => _engine.Stop();
}