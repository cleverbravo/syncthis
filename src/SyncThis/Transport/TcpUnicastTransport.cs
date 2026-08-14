using System.Net;
using System.Net.Sockets;
using MessagePack;
using SyncThis.Core;

namespace SyncThis.Transport;

public class TcpUnicastTransport : ITransport
{
    private const int MaxFrameSize = 16 * 1024 * 1024;

    private readonly SyncConfig _config;
    private readonly Guid _nodeId;
    private readonly MessagePackSerializerOptions _options;
    private TcpListener? _listener;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private bool _disposed;

    public event Action<Message>? MessageReceived;

    public TcpUnicastTransport(SyncConfig config, Guid nodeId)
    {
        _config = config;
        _nodeId = nodeId;
        _options = MessagePack.Resolvers.ContractlessStandardResolver.Options;
    }

    public Result Start()
    {
        try
        {
            _listener = new TcpListener(IPAddress.Any, _config.ListenPort);
            _listener.Start();
            _cts = new CancellationTokenSource();
            _acceptTask = AcceptLoop(_cts.Token);
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
            _acceptTask?.GetAwaiter().GetResult();
            _listener?.Stop();
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("TRANSPORT_STOP_FAILED", ex.Message);
        }
    }

    public Result Send(Message message, NodeInfo recipient)
    {
        if (recipient.IPAddress is null)
            return Result.Failure("TRANSPORT_SEND_FAILED", $"Peer {recipient.NodeId} has no known address.");

        try
        {
            message.SenderId = _nodeId;
            using var client = new TcpClient();
            client.Connect(recipient.IPAddress, recipient.ListenPort);
            using var stream = client.GetStream();
            WriteMessage(stream, message);
            return Result.Success();
        }
        catch (Exception ex)
        {
            return Result.Failure("TRANSPORT_SEND_FAILED", ex.Message);
        }
    }

    public Result Broadcast(Message message)
    {
        return Result.Failure("NOT_SUPPORTED", "TCP transport does not support broadcast. Use Send instead.");
    }

    private async Task AcceptLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var client = await _listener!.AcceptTcpClientAsync(ct);
                _ = HandleIncoming(client, ct);
            }
            catch (OperationCanceledException) { break; }
            catch { }
        }
    }

    private async Task HandleIncoming(TcpClient client, CancellationToken ct)
    {
        try
        {
            using (client)
            using (var stream = client.GetStream())
            {
                var message = await ReadMessage(stream, ct);
                if (message is not null && message.SenderId != _nodeId)
                    MessageReceived?.Invoke(message);
            }
        }
        catch { }
    }

    private void WriteMessage(Stream stream, Message message)
    {
        var payload = MessagePack.MessagePackSerializer.Serialize(message, _options);
        var header = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(payload.Length));
        stream.Write(header, 0, header.Length);
        stream.Write(payload, 0, payload.Length);
        stream.Flush();
    }

    private async Task<Message?> ReadMessage(Stream stream, CancellationToken ct)
    {
        var header = new byte[4];
        await ReadExactly(stream, header, 4, ct);
        var length = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(header));
        if (length <= 0 || length > MaxFrameSize)
            return null;

        var payload = new byte[length];
        await ReadExactly(stream, payload, length, ct);
        return MessagePack.MessagePackSerializer.Deserialize<Message>(payload, _options);
    }

    private static async Task ReadExactly(Stream stream, byte[] buffer, int count, CancellationToken ct)
    {
        var read = 0;
        while (read < count)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(read, count - read), ct);
            if (n == 0) throw new EndOfStreamException();
            read += n;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Stop();
        _listener = null;
        _cts?.Dispose();
    }
}
