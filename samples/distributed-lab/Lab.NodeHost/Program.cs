using System.Runtime.InteropServices;
using DtPipe.Lab.NodeHost;

var options = NodeHostOptions.Parse(args);
var config = NodeConfig.Load(options.ConfigPath);

// Held until the host has disposed its fragments: an orderly stop must not leave a dtpipe child behind.
using var shutdown = new CancellationTokenSource();
void Stop(PosixSignalContext context) { context.Cancel = true; shutdown.Cancel(); }
using var sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, Stop);
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, Stop);

await using var host = new NodeHost(options, config);
try
{
    await host.RunAsync(shutdown.Token);
}
catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { }
