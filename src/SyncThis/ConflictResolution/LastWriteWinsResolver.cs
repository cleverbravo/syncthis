using SyncThis.Core;

namespace SyncThis.ConflictResolution;

public class LastWriteWinsResolver : IConflictResolver
{
    public Result<object> Resolve(int localVersion, int remoteVersion, Func<Result<object>> applyRemote)
    {
        if (remoteVersion >= localVersion)
            return applyRemote();
        return Result<object>.Failure("STALE_DELTA", $"Remote version {remoteVersion} is older than local version {localVersion}.");
    }
}