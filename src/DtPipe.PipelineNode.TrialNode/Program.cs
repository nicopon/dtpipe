using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// The data node of the trial recipe, not for production: a plain host that adds a dtpipe node to itself, as a team's own
// service would. The node's settings are DtPipeNode__*, from the environment first; the bricks it offers are
// DtPipeNode:Bricks, from the file DtPipeNode__BricksFile names. A node that serves real data declares its own bricks.
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { Args = args, ContentRootPath = AppContext.BaseDirectory });
if (builder.Configuration["DtPipeNode:BricksFile"] is { Length: > 0 } bricksFile)
    builder.Configuration.AddJsonFile(Path.GetFullPath(bricksFile), optional: false);
builder.Services.AddDtPipeNode(builder.Configuration.GetSection("DtPipeNode"))
    .AddBricks(builder.Configuration.GetSection("DtPipeNode:Bricks"));
await builder.Build().RunAsync();
