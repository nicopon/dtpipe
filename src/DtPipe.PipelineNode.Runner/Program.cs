using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Hosting;

// The official runner: a node that hosts no data and runs every fragment the coordinator places on
// it. Everything it needs comes from configuration, the environment first (DtPipeNode__Name,
// DtPipeNode__CoordinatorUrl, DtPipeNode__Oidc__Authority, …). The generic host stops it on SIGTERM
// or Ctrl+C, and stopping disposes every fragment before the process exits.
// appsettings.json is read beside the binary, whatever directory the runner starts in.
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.Services.AddDtPipeNode(builder.Configuration.GetSection("DtPipeNode")).RunAnyFragment();
await builder.Build().RunAsync();
