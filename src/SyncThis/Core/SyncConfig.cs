using System.Net;

namespace SyncThis.Core;

public class SyncConfig
{
    public IPAddress MulticastAddress { get; set; } = IPAddress.Parse("239.255.0.1");
    public int MulticastPort { get; set; } = 42000;
    public int HeartbeatIntervalMs { get; set; } = 3000;
    public int PeerTimeoutMs { get; set; } = 12000;
    public int FullSnapshotEveryNVersions { get; set; } = 10;
    public int TransportBacklogSize { get; set; } = 100;

    public SyncTopology Topology { get; set; } = SyncTopology.P2PMulticast;
    public SyncRole Role { get; set; } = SyncRole.Peer;

    public int ListenPort { get; set; } = 42000;
    public List<string> SeedPeers { get; set; } = [];
}
