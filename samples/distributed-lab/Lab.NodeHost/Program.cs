using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// One lab data node: a plain host that adds a dtpipe node to itself, read from nodes/<node>.json.
//
//   Lab.NodeHost --config <node.json> --state <dir> --coordinator <url> --dtpipe <path>
//
// The node's databases live under <state>/<node>/ and reach its fragments as ${{VARIABLE}}. Its
// identity is the coordinator's IdP client named in the file. LAB_NODE_SANDBOX=1 (or true) in this
// process's environment lets it run any reader or writer, not only its bricks: the switch is the
// host's own, never something the coordinator sends.
var arguments = new ConfigurationBuilder().AddCommandLine(args).Build();
string Required(string key) => arguments[key]
    ?? throw new ArgumentException($"Missing --{key}. Usage: --config <node.json> --state <dir> --coordinator <url> --dtpipe <path>");

var file = new ConfigurationBuilder().AddJsonFile(Path.GetFullPath(Required("config")), optional: false).Build();
var name = file["name"] ?? throw new InvalidOperationException("The node file names no node.");
var state = Path.GetFullPath(Required("state"));
var coordinator = Required("coordinator").TrimEnd('/');
var sandbox = Environment.GetEnvironmentVariable("LAB_NODE_SANDBOX") is "1" or "true" or "TRUE" or "True";

var builder = Host.CreateApplicationBuilder();
builder.Logging.ClearProviders().AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
builder.Logging.AddFilter("TransportR", LogLevel.Warning).AddFilter("Microsoft", LogLevel.Warning);

var node = builder.Services.AddDtPipeNode(o =>
{
    o.Name = name;
    o.Description = file["description"] ?? "";
    o.CoordinatorUrl = coordinator;
    o.DtPipe = Path.GetFullPath(Required("dtpipe"));
    o.StateDirectory = Path.Combine(state, name);
    o.Policy = sandbox ? FragmentPolicy.AnyReadOrWrite : FragmentPolicy.Bricks;
    // The coordinator's embedded IdP issues the lab's tokens.
    o.Oidc.Authority = coordinator;
    o.Oidc.ClientId = file["clientId"] ?? name;
    o.Oidc.ClientSecret = file["secret"] ?? "";
});

// A database is a dataset the coordinator may open itself (the query panel, split): the lab runs on
// one machine, so it announces the file's path.
foreach (var dataset in file.GetSection("datasets").GetChildren())
{
    node.WithVariable(dataset["variable"]!, Path.Combine(state, name, dataset["file"]!), dataset["description"], dataset["engine"]);
}
node.AddBricks(file.GetSection("bricks"));

await builder.Build().RunAsync();
