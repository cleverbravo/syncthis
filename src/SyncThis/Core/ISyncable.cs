namespace SyncThis.Core;

public abstract class Syncable : ISyncable
{
    public Guid SyncId { get; set; } = Guid.NewGuid();
    public int SyncVersion { get; set; }
    public virtual string SyncGroup => "default";
}

public interface ISyncable
{
    Guid SyncId { get; }
    int SyncVersion { get; set; }
    string SyncGroup { get; }
}
