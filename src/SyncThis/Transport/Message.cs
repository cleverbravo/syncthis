using MessagePack;

namespace SyncThis.Transport;

public enum MessageType
{
    Delta,
    FullSnapshot,
    SnapshotRequest
}

[MessagePackObject]
public class Message
{
    [Key(0)]
    public Guid SenderId { get; set; }

    [Key(1)]
    public Guid SyncId { get; set; }

    [Key(2)]
    public int SyncVersion { get; set; }

    [Key(3)]
    public MessageType Type { get; set; }

    [Key(4)]
    public byte[] Payload { get; set; } = [];

    [Key(5)]
    public string? TypeName { get; set; }
}