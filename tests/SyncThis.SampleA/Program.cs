using SyncThis.Core;
using SyncThis.Sync;

Console.WriteLine("Starting A");

var hostA = new Sync(new SyncEngine(SyncConfigFactory.Broadcast(42400)));
var data = new Data { X = 30 };
hostA.SyncThis(data);
Console.WriteLine("  HostA sent initial snapshot (X=30)");

do
{
    Console.Write($"Enter the new value of int X(0 ends the program)=");
}while((data.X=int.Parse(Console.ReadLine()))!=0);//possible exception but is just a sample

hostA.Stop();


class Data:Syncable
{
    public int X { get; set; }
}