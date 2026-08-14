using System.Net;
using System.Net.Sockets;
using MessagePack;
using SyncThis.Core;

namespace SyncThis.Transport;

public class UdpBroadcastTransport : ITransport
{
    private readonly SyncConfig _config;
    private readonly Guid _nodeId;
    private readonly MessagePackSerializerOptions _serializerOptions;
    private UdpClient? _udpClient;
    private CancellationTokenSource? _cts;
    private Task? _listenTask;
    private bool _disposed;

    public event Action<Message>? MessageReceived;

    public UdpBroadcastTransport(SyncConfig config, Guid nodeId)
    {
        _config = config;
        _nodeId = nodeId;
        _serializerOptions = MessagePack.Resolvers.ContractlessStandardResolver.Options;
    }

    public Result Start()
    {
        try
        {
            _udpClient = new UdpClient(AddressFamily.InterNetwork);
            _udpClient.ExclusiveAddressUse = false;
            _udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Broadcast, true);
            _udpClient.Client.Bind(new IPEndPoint(IPAddress.Any, _config.MulticastPort + 1));

            _cts = new CancellationTokenSource();
            _listenTask = ListenLoop(_cts.Token);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("TRANSPORT_START_FAILED", ex.Message);
        }
    }

    public Result Stop()
    {
        try
        {
            _cts?.Cancel();
            _listenTask?.GetAwaiter().GetResult();
            _udpClient?.Close();
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("TRANSPORT_STOP_FAILED", ex.Message);
        }
    }

    public Result Send(Message message, NodeInfo recipient)
    {
        var address = recipient.IPAddress;
        if (address is null)
        {
            try
            {
                var addresses = Dns.GetHostAddresses(recipient.HostName);
                if (addresses.Length == 0)
                    return Result.Failure("TRANSPORT_SEND_FAILED", $"No address for host {recipient.HostName}.");
                address = addresses[0];
            }
            catch (Exception ex)
            {
                return Result.Failure("TRANSPORT_SEND_FAILED", $"Could not resolve host {recipient.HostName}: {ex.Message}");
            }
        }
        return SendInternal(message, new IPEndPoint(address, recipient.ListenPort + 1));
    }

    public Result Broadcast(Message message)
    {
        return SendInternal(message, new IPEndPoint(_config.BroadcastAddress!, _config.MulticastPort + 1));
    }

    private Result SendInternal(Message message, IPEndPoint endpoint)
    {
        try
        {
            message.SenderId = _nodeId;
            var bytes = MessagePack.MessagePackSerializer.Serialize(message, _serializerOptions);
            _udpClient?.Send(bytes, bytes.Length, endpoint);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("TRANSPORT_SEND_FAILED", ex.Message);
        }
    }

    private async Task ListenLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await _udpClient!.ReceiveAsync(ct);
                var message = MessagePack.MessagePackSerializer.Deserialize<Message>(result.Buffer, _serializerOptions);
                if (message.SenderId != _nodeId)
                    MessageReceived?.Invoke(message);
            }
            catch (OperationCanceledException) { break; }
            catch { }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Stop();
        _udpClient?.Dispose();
        _cts?.Dispose();
    }
}
