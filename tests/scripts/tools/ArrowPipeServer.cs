#!/usr/bin/env dotnet
#:property TargetFramework=net10.0

using System.IO.Pipes;

// A validator-only stand-in for the server role dtpipe's own arrow: adapter never takes (it is
// client-only by design - see DtPipe.Adapters/Adapters/Arrow/ArrowPipeLocation.cs): the real server
// is DtPipe.PipelineNode, out of DtPipe.sln and unavailable here. This is the minimum needed to
// drive the real dtpipe binary's arrow:pipe://<name> client through a full connection.
//
// Usage: ArrowPipeServer.cs <pipe-name> <relay-in|relay-out> <file-path> [connect-timeout-seconds]
//   relay-in  - the server reads whatever arrives on the pipe and writes it to <file-path>
//               (drives a dtpipe writer's arrow:pipe:// client side).
//   relay-out - the server writes the bytes of <file-path> onto the pipe
//               (drives a dtpipe reader's arrow:pipe:// client side).

if (args.Length < 3)
{
    Console.Error.WriteLine("Usage: ArrowPipeServer.cs <pipe-name> <relay-in|relay-out> <file-path> [connect-timeout-seconds]");
    Environment.Exit(2);
    return;
}

var pipeName = args[0];
var mode = args[1];
var filePath = args[2];
var timeoutSeconds = args.Length > 3 ? int.Parse(args[3]) : 15;

var direction = mode switch
{
    "relay-in" => PipeDirection.In,
    "relay-out" => PipeDirection.Out,
    _ => throw new ArgumentException($"unknown mode '{mode}', expected relay-in or relay-out"),
};

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
using var server = new NamedPipeServerStream(pipeName, direction, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);

await server.WaitForConnectionAsync(cts.Token);

if (mode == "relay-in")
{
    using var output = File.Create(filePath);
    await server.CopyToAsync(output, cts.Token);
}
else
{
    using var input = File.OpenRead(filePath);
    await input.CopyToAsync(server, cts.Token);
}

Console.WriteLine("done");
