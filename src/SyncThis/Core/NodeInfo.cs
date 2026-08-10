using System.Net;
using MessagePack;

namespace SyncThis.Core;

[MessagePackObject]
public class NodeInfo
{
    [Key(0)]
    public Guid NodeId { get; set; }

    [Key(1)]
    public string HostName { get; set; } = string.Empty;

    [Key(2)]
    public int ListenPort { get; set; }

    [Key(3)]
    public SyncRole Role { get; set; } = SyncRole.Peer;

    [IgnoreMember]
    public DateTime LastSeen { get; set; } = DateTime.UtcNow;

    [IgnoreMember]
    public IPAddress? IPAddress { get; set; }

    public bool IsExpired(TimeSpan timeout) =>
        DateTime.UtcNow - LastSeen > timeout;
}
