# SyncThis
**SyncThis** is a dotnet experimental library to synchronize the state of an object across the Local Area Network-LAN, It is built for socket-based experimentation and proof of concept (POC) projects.


## How to use:

### 1. Define a data class:

```csharp
class Data:Syncable
{
    public int X { get; set; }
}
```

### 2. Define a 'reader' 

```csharp 
var hostB = new Sync(new SyncEngine(SyncConfigFactory.Broadcast(42400)));

hostB.OnUpdate<Data>(p => { 
    Console.WriteLine($"new value for p.X={p.X}");
});
```


### 3. Define the 'writer'

```csharp 
var hostA = new Sync(new SyncEngine(SyncConfigFactory.Broadcast(42400)));
var data = new Data { X = 30 };

hostA.SyncThis(data);
Console.WriteLine("HostA sent initial snapshot (X=30)");

//Modify the state of Data
```


Also supports **single-writer/multiwriter** options.
Support protocols: **UDP/TCP** 
Cast supported: **Unicast/Broadcast/Multicast**
Architectures: **P2P/Client-Server**