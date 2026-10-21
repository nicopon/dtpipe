using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;
using Lab.NodeHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// One lab data node: a plain host that adds a dtpipe node to itself, read from nodes/<node>.json.
//
//   Lab.NodeHost --config <node.json> --state <dir> --coordinator <url> --idp <url> --dtpipe <path>
//
// The node's databases live under <state>/<node>/ and reach its fragments as ${{VARIABLE}}. Its
// identity is the IdP client named in the file. LAB_NODE_SANDBOX=1 (or true) in this
// process's environment lets it run any reader or writer, not only its bricks: the switch is the
// host's own, never something the coordinator sends.
var arguments = new ConfigurationBuilder().AddCommandLine(args).Build();
string Required(string key) => arguments[key]
    ?? throw new ArgumentException($"Missing --{key}. Usage: --config <node.json> --state <dir> --coordinator <url> --idp <url> --dtpipe <path>");

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
    o.Policy = FragmentPolicy.Bricks;
    // A bench's key, derived from the node's name so its replicas share it: not a secret.
    o.BrickKey = file["brickKey"] ?? $"lab-brick-key-of-{name}";
    // The coordinator's lab mode ends a fragment's child on request, to inject a fault; a host answers that only when its own
    // configuration enables it.
    o.FaultInjection = true;
    o.Oidc.Authority = Required("idp");
    o.Oidc.ClientId = file["clientId"] ?? name;
    o.Oidc.ClientSecret = file["secret"] ?? "";
});

// A database reaches the node's dtpipe children as ${{VARIABLE}}: the file under the node's own
// state directory. The coordinator learns its name, never where it is.
foreach (var dataset in file.GetSection("datasets").GetChildren())
{
    node.WithVariable(dataset["variable"]!, Path.Combine(state, name, dataset["file"]!), dataset["description"]);
}
node.AddBricks(file.GetSection("bricks"));

// The coordinator's lab mode cuts a branch where its data is: every lab node answers it, strict or sandbox.
node.WithRequest("lab.split", new LabRequests(name).SplitAsync);

// Sandbox mode lets a lab node run a fragment that reads or writes through any reader or writer, not only
// through its bricks: its own switch, and the one the Lab view's manual planning needs.
if (sandbox) node.WithFragmentGate((fragment, bricks) => BrickRules.FragmentRefusal(fragment.Yaml ?? "", bricks, requireBricks: false));

await builder.Build().RunAsync();
