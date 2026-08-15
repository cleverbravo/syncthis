namespace SyncThis.Core;

public enum SyncTopology
{
    P2PMulticast,
    ClientServer,
    P2P,// lets start with UDP but TCP must be available in some point
    P2PBroadcast
}

public enum SyncRole
{
    Peer,
    Server,
    Client
}
