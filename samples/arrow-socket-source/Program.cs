using ArrowSocketSource;
using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// A service offers the rows of an external API, after its business rules, to the dtpipe node it hosts itself.
//
//   node --coordinator <url> --name <node> --key-file <file> --work <dir> --report-dir <dir>
//        [--dtpipe <DtPipe.dll or executable>] [--socket-dir <dir>]
//       The service as a node. Its bricks are the orders source (served here, on an Arrow socket), a standard
//       `generate:1000` source, and two CSV sinks.
if (args.Length == 0 || args[0] != "node")
{
    Console.Error.WriteLine("Usage: node --coordinator <url> --name <node> --key-file <file> --work <dir> --report-dir <dir>");
    return 2;
}

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
node.AddBrick("numbers", BrickKind.Source, "Generated numbers", "input: generate:1000",
    "A thousand rows from dtpipe's own generate source");

node.AddBrick("report", BrickKind.Sink, "Orders report", $"output: csv:{Path.Combine(reports, "report.csv")}");
node.AddBrick("numbers-report", BrickKind.Sink, "Numbers report", $"output: csv:{Path.Combine(reports, "numbers-report.csv")}");

await builder.Build().RunAsync();
return 0;
