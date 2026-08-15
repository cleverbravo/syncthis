using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace SyncThis.Core;

public class SyncConfig
{
    public IPAddress MulticastAddress { get; set; } = IPAddress.Parse("239.255.0.1");
    public int MulticastPort { get; set; } = 42000;
    public IPAddress? BroadcastAddress { get; set; } = NetworkInterface
        .GetAllNetworkInterfaces()
        .Where(ni => ni.OperationalStatus == OperationalStatus.Up)
        .SelectMany(ni => ni.GetIPProperties().UnicastAddresses)
        .Where(ua => ua.Address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(ua.Address))
        .Select(ua =>
        {
            var ip = ua.Address.GetAddressBytes();
            var mask = ua.IPv4Mask.GetAddressBytes();
            var broadcast = new byte[4];
            for (int i = 0; i < 4; i++)
                broadcast[i] = (byte)(ip[i] | ~mask[i]);
            return new IPAddress(broadcast);
        })
        .FirstOrDefault();
    public int HeartbeatIntervalMs { get; set; } = 3000;
    public int PeerTimeoutMs { get; set; } = 12000;
    public int FullSnapshotEveryNVersions { get; set; } = 10;
    public int TransportBacklogSize { get; set; } = 100;

    public SyncTopology Topology { get; set; } = SyncTopology.P2PMulticast;
    public SyncRole Role { get; set; } = SyncRole.Peer;

    public int ListenPort { get; set; } = 42000;
    public List<string> SeedPeers { get; set; } = [];
}
