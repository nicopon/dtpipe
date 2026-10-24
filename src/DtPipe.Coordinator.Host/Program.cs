using DtPipe.Coordinator.Server;

// The coordinator's server as the product ships it. Its settings are the Coordinator configuration section, from the
// environment first (Coordinator__Authority, Coordinator__StateDirectory, …); the pages are served from the wwwroot
// published beside the binary, whatever directory the server starts in.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.AddCoordinatorServer();
var app = builder.Build();
await app.MapCoordinatorServerAsync();
app.Run();
