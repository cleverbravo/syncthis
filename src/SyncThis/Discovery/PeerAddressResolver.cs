using System.Net;
using System.Net.NetworkInformation;

namespace SyncThis.Discovery;

internal static class PeerAddressResolver
{
    public static IPAddress Normalize(IPAddress address)
    {
        if (IPAddress.IsLoopback(address) || IsLocalInterface(address))
            return IPAddress.Loopback;
        return address;
    }

    private static bool IsLocalInterface(IPAddress address)
    {
        foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
        {
            foreach (var unicast in networkInterface.GetIPProperties().UnicastAddresses)
            {
                if (unicast.Address.Equals(address))
                    return true;
            }
        }
        return false;
    }
}
