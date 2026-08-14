using MessagePack;
using SyncThis.Core;

namespace SyncThis.Discovery;

[MessagePackObject]
public class PeerList
{
    [Key(0)]
    public List<NodeInfo> Peers { get; set; } = [];
}