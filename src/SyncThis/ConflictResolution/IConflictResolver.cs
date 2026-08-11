using SyncThis.Core;

namespace SyncThis.ConflictResolution;

public interface IConflictResolver
{
    Result<object> Resolve(int localVersion, int remoteVersion, Func<Result<object>> applyRemote);
}
