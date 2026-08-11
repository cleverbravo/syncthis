using SyncThis.Core;

namespace SyncThis.Serialization;

public interface IDelta
{
    Result<byte[]> ComputeDelta(object current, object? previous);
    Result<object> ApplyDelta(object target, byte[] delta, Type type);
    Result<byte[]> TakeSnapshot(object obj);
    Result<object> FromSnapshot(byte[] snapshot, Type type);
}
