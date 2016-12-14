using System;
using System.Collections.Generic;
using System.Linq;

using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;

using Dd;

public class DictionaryServiceImpl : DictionaryService.DictionaryServiceBase {

    public static uint HashString(string str) {
        var md5 = System.Security.Cryptography.MD5.Create();
        var inputBytes = System.Text.Encoding.ASCII.GetBytes(str);
        var hash = md5.ComputeHash(inputBytes);
        return BitConverter.ToUInt32(hash, 0);
    }

    public DictionaryServiceImpl(string port, string nextServerAddress) {
        _myHash = HashString(port);
        _nextHash = HashString(nextServerAddress.Split(':')[1]);
        Console.WriteLine($"Connecting to next server {nextServerAddress}");
        var channel = new Channel(nextServerAddress, ChannelCredentials.Insecure);
        _nextClient = new DictionaryService.DictionaryServiceClient(channel);
        _myAddr = port;
    }

    // Fields
    private static Dictionary<string, string> _dict = new Dictionary<string, string>(10000);
    private uint _myHash;
    private uint _nextHash;
    private DictionaryService.DictionaryServiceClient _nextClient;
    private string _myAddr;

    // Methods
    public override Task<GetResponse> Get(GetRequest request, ServerCallContext context) {

        // Try to find the key locally
        string value;
        if(_dict.TryGetValue(request.Key, out value)) {
            return Task.FromResult(new GetResponse { Value = value, Node = "current", Found = true });
        }
        return Task.FromResult(new GetResponse { Found = false });
    }

    public override Task<SetResponse> Set(SetRequest request, ServerCallContext context) {
        _dict[request.Key] = request.Value;
        return Task.FromResult(new SetResponse { Success = true });
    }

    public override Task<ListResponse> ListKeys(ListRequest request, ServerCallContext context) {
        var response = new ListResponse();
        response.Keys.AddRange(_dict.Keys);
        return Task.FromResult(response);
    }

    public override Task<FindResponse> Find(FindRequest request, ServerCallContext context) {
        var keyHash = HashString(request.Key);
        var crossingZero = _nextHash < _myHash;
        
        if(crossingZero) {
            if(keyHash >= _myHash || keyHash < _nextHash) {
                Console.WriteLine($"SERVING KEY");
                return Task.FromResult(new FindResponse { Node = _myAddr });
            } else {
                Console.WriteLine($"FORWARDING REQUEST TO {_nextHash}");
                var callOptions = new CallOptions(deadline: DateTime.UtcNow.Add(TimeSpan.FromSeconds(3)));
                var findRequest = new FindRequest { Key = request.Key };
                return Task.FromResult(_nextClient.Find(findRequest, callOptions));
            }
        } else if(keyHash >= _myHash && keyHash < _nextHash) {
            Console.WriteLine($"SERVING KEY");
            return Task.FromResult(new FindResponse { Node = _myAddr });
        } else {

            Console.WriteLine($"FORWARDING REQUEST TO {_nextHash}");
            var callOptions = new CallOptions(deadline: DateTime.UtcNow.Add(TimeSpan.FromSeconds(3)));
            var findRequest = new FindRequest { Key = request.Key };
            return Task.FromResult(_nextClient.Find(findRequest, callOptions));
        }
    }
}

public class Program {
    private static AutoResetEvent autoEvent = new AutoResetEvent(false);

    public static int Main(string[] args) {

        Console.WriteLine($"HOST IP: {args[0]}");
        // Validate arguments
        if(args.Length < 1) {
            Console.WriteLine("Usage: dotnet server.dll {PORT}");
            return -1;
        }
        int port;
        if(!int.TryParse(args[1], out port)) {
            Console.WriteLine("invalid format for port, use a number");
            return -1;
        }

        // Build client to next server in line
        var myHash = DictionaryServiceImpl.HashString(port.ToString());
        var otherPorts = args
            .Skip(2)
            .Select(p => new { Hash = DictionaryServiceImpl.HashString(p), Port = p })
            .OrderBy(x => x.Hash);
        var nextPort = otherPorts.First();
        foreach(var otherPort in otherPorts) {
            if(otherPort.Hash > myHash) {
                nextPort = otherPort;
                break;
            }
        }
        Console.WriteLine($"{port} ({myHash}) => {nextPort.Port} ({nextPort.Hash})");


        var nextClientAddress = $"{args[0]}:{nextPort.Port}";
    
    //    var callOptions = new CallOptions(deadline: DateTime.UtcNow.Add(TimeSpan.FromSeconds(3)));

            // Set key
          /*  var setRequest = new SetRequest { Key = "foo", Value = "bar" };
            var setResponse = client.Set(setRequest, callOptions);
            Console.WriteLine($"setRequest={setRequest}");
            Console.WriteLine($"setResponse={setResponse}");
*/

        // Build the gRPC server
        Console.WriteLine("Firing off GRPC server");
        var server = new Server {
            Services = { DictionaryService.BindService(new DictionaryServiceImpl(port.ToString(), nextClientAddress)) },
            Ports = { new ServerPort("*", port, ServerCredentials.Insecure) }
        };

        // Launch the gRPC server
        ThreadPool.QueueUserWorkItem(_ => {
                server.Start();
                Console.WriteLine($"GRPC server listening on port {port}");
            }
        );
        autoEvent.WaitOne();
        server.ShutdownAsync().Wait();
        Console.WriteLine("Shutting down ...");
        return 0;
    }
}
