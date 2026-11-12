using ArrowSocketSource;
using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// A service offers the rows of an external API, after its business rules, to the dtpipe node it hosts itself.
//
//   probe --dtpipe <executable>
//       Step one, no coordinator: dtpipe reads the service's sockets directly; prints a verdict per case.
//   node --coordinator <url> --name <node> --key-file <file> --work <dir> --report-dir <dir>
//        [--dtpipe <DtPipe.dll or executable>] [--socket-dir <dir>]
//       Step two: the service as a node. Its bricks are the orders source (served here, on an Arrow socket), a standard
//       `generate:1000` source, and two CSV sinks.
if (args.Length == 0 || args[0] is not ("probe" or "node"))
{
    Console.Error.WriteLine("Usage: probe --dtpipe <executable> | node --coordinator <url> --name <node> --key-file <file> --work <dir> --report-dir <dir>");
    return 2;
}

var mode = args[0];
var options = new ConfigurationBuilder().AddCommandLine(args[1..]).Build();
string Required(string key) => options[key] ?? throw new ArgumentException($"Missing --{key}.");

var api = new FakeOrdersApi(
    totalOrders: int.Parse(options["orders"] ?? "20000"),
    pageSize: 500,
    pageDelay: TimeSpan.FromMilliseconds(int.Parse(options["page-delay-ms"] ?? "20")));

var builder = Host.CreateApplicationBuilder();
builder.Logging.ClearProviders().AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
builder.Logging.AddFilter("Microsoft", LogLevel.Warning).AddFilter("TransportR", LogLevel.Warning);

const string OrdersTitle = "Orders of the shop API";
const string OrdersDescription = "Orders after the business rules: personal data dropped, test orders filtered, total computed";

if (mode == "probe")
{
    // A Unix socket path is at most 103 characters on macOS: the probe's sockets stay short.
    var socketDir = options["socket-dir"] ?? "/tmp";
    var dtpipe = Path.GetFullPath(Required("dtpipe"));
    // The sources need a node to be registered on. This one has no coordinator to reach: it retries in silence, and the probe
    // uses its sockets only.
    builder.Logging.SetMinimumLevel(LogLevel.Warning).AddFilter("DtPipe.PipelineNode", LogLevel.None);
    var probeNode = builder.Services.AddDtPipeNode(o =>
    {
        o.Name = "probe";
        o.CoordinatorUrl = "http://127.0.0.1:9";
        o.Oidc.ClientSecret = "unused";
        o.StateDirectory = Path.Combine(Path.GetTempPath(), "arrow-socket-source-probe");
        o.DtPipe = dtpipe;
    });
    probeNode.AddArrowSource("orders", OrdersTitle, OrdersTransform.Schema, (_, ct) => OrdersTransform.ReadAsync(api, ct),
        OrdersDescription, socketDir);
    // The external API fails after three pages.
    probeNode.AddArrowSource("failing", OrdersTitle, OrdersTransform.Schema, (_, ct) => OrdersTransform.ReadThenFailAsync(api, 3, ct),
        socketDirectory: socketDir);
    using var host = builder.Build();
    await host.StartAsync();
    var verdict = await new Probe(dtpipe, ArrowSourceExtensions.SocketPath("orders", socketDir),
        ArrowSourceExtensions.SocketPath("failing", socketDir), ArrowSourceExtensions.SocketPath("nobody", socketDir), api).RunAsync();
    await host.StopAsync();
    return verdict;
}

var reports = Path.GetFullPath(Required("report-dir"));
var node = builder.Services.AddDtPipeNode(o =>
{
    o.Name = Required("name");
    o.CoordinatorUrl = Required("coordinator");
    o.Description = "Sample node: a .NET source served on an Arrow socket, and a standard dtpipe source";
    o.StateDirectory = Path.GetFullPath(Required("work"));
    // The node's key, in a file: read once, never in an argument.
    o.Oidc.ClientSecret = "@" + Path.GetFullPath(Required("key-file"));
    // Left out, the host runs the dtpipe tool package its build downloaded (PackageDownload in the csproj).
    if (options["dtpipe"] is { Length: > 0 } dtpipe) o.DtPipe = Path.GetFullPath(dtpipe);
});

// A source of this service: the producer is plain .NET, the schema fixed.
node.AddArrowSource("orders", OrdersTitle, OrdersTransform.Schema, (_, ct) => OrdersTransform.ReadAsync(api, ct),
    OrdersDescription, options["socket-dir"]);

// A standard dtpipe source: a branch of the pipeline grammar, run by the dtpipe child.
node.AddBrick("numbers", BrickKind.Source, "Generated numbers", """{"input":"generate:1000"}""",
    "A thousand rows from dtpipe's own generate source");

node.AddBrick("report", BrickKind.Sink, "Orders report", $$"""{"output":"csv:{{Path.Combine(reports, "report.csv")}}"}""");
node.AddBrick("numbers-report", BrickKind.Sink, "Numbers report", $$"""{"output":"csv:{{Path.Combine(reports, "numbers-report.csv")}}"}""");

await builder.Build().RunAsync();
return 0;
