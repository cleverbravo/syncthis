namespace SyncThis.Core;

public static class SyncConfigFactory
{
    public static SyncConfig Default() => new();

    public static SyncConfig Multicast(int? heartbeatMs = null, int? peerTimeoutMs = null) => new()
    {
        Topology = SyncTopology.P2PMulticast,
        HeartbeatIntervalMs = heartbeatMs ?? 3000,
        PeerTimeoutMs = peerTimeoutMs ?? 12000
    };

    public static SyncConfig P2P(int port, params string[] seeds) => new()
    {
        Topology = SyncTopology.P2P,
        ListenPort = port,
        SeedPeers = seeds.ToList(),
        HeartbeatIntervalMs = 3000,
        PeerTimeoutMs = 12000
    };

    public static SyncConfig Broadcast(int port, int? heartbeatMs = null, int? peerTimeoutMs = null) => new()
    {
        Topology = SyncTopology.P2PBroadcast,
        ListenPort = port,
        HeartbeatIntervalMs = heartbeatMs ?? 3000,
        PeerTimeoutMs = peerTimeoutMs ?? 12000
    };

    public static SyncConfig Broadcast(int port) => new()
    {
        Topology = SyncTopology.P2PBroadcast,
        ListenPort = port,
        HeartbeatIntervalMs = 200,
        PeerTimeoutMs = 2000
    };

    public static SyncConfig ClientServer(int port,SyncRole role, int? heartbeatMs = null, int? peerTimeoutMs = null) => new()
    {
        Topology = SyncTopology.ClientServer,
        Role = role,
        ListenPort = port,
        HeartbeatIntervalMs = heartbeatMs ?? 3000,
        PeerTimeoutMs = peerTimeoutMs ?? 12000
    };

    public static SyncConfig ClientServer(int port,SyncRole role) => new()
    {
        Topology = SyncTopology.ClientServer,
        Role = role,
        ListenPort = port,
        HeartbeatIntervalMs = 200,
        PeerTimeoutMs = 2000
    };
    public static SyncConfigBuilder Builder() => new();

    public class SyncConfigBuilder
    {
        private readonly SyncConfig _config = new();

        public SyncConfigBuilder WithTopology(SyncTopology topology)
        {
            _config.Topology = topology;
            return this;
        }

        public SyncConfigBuilder WithRole(SyncRole role)
        {
            _config.Role = role;
            return this;
        }

        public SyncConfigBuilder WithListenPort(int port)
        {
            _config.ListenPort = port;
            return this;
        }

        public SyncConfigBuilder WithSeedPeers(params string[] seeds)
        {
            _config.SeedPeers = seeds.ToList();
            return this;
        }

        public SyncConfigBuilder WithHeartbeatInterval(int ms)
        {
            _config.HeartbeatIntervalMs = ms;
            return this;
        }

        public SyncConfigBuilder WithPeerTimeout(int ms)
        {
            _config.PeerTimeoutMs = ms;
            return this;
        }

        public SyncConfigBuilder WithMulticastAddress(string address)
        {
            _config.MulticastAddress = System.Net.IPAddress.Parse(address);
            return this;
        }

        public SyncConfigBuilder WithMulticastPort(int port)
        {
            _config.MulticastPort = port;
            return this;
        }

        public SyncConfigBuilder WithFullSnapshotEveryN(int n)
        {
            _config.FullSnapshotEveryNVersions = n;
            return this;
        }

        public SyncConfigBuilder WithTransportBacklogSize(int size)
        {
            _config.TransportBacklogSize = size;
            return this;
        }

        public SyncConfigBuilder ForTest()
        {
            _config.HeartbeatIntervalMs = 200;
            _config.PeerTimeoutMs = 2000;
            return this;
        }

        public SyncConfig Build() => _config;
    }
}
