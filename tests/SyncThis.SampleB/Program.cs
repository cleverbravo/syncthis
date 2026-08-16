using SyncThis.Core;
using SyncThis.Sync;

Console.WriteLine("Starting B");
var hostB = new Sync(new SyncEngine(SyncConfigFactory.Broadcast(42400)));

hostB.OnUpdate<Data>(p => { 
    Console.WriteLine($"new value for p.X={p.X}");
});

Console.ReadKey();

return 0;


class Data:Syncable
{
    public int X { get; set; }
}

