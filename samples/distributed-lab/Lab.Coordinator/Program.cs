using DtPipe.Coordinator.Server;
using DtPipe.Lab.Coordinator;

// The lab's coordinator: the product's server, plus the Lab view under /lab (its pages, for admins), its scenario catalog, its
// planning by hand and fault injection, mapped on the server's admin group. Its settings are the server's Coordinator section,
// and Lab:Scenarios, the directory of the catalog's job files. The pages, the server's and the Lab view's, are served from the
// wwwroot published beside the binary, whatever directory it starts in.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.AddCoordinatorServer(new AdminView("/lab", "the Lab view"));
builder.Services.AddSingleton(new PipelineCatalog(builder.Configuration["Lab:Scenarios"] is { Length: > 0 } scenarios ? Path.GetFullPath(scenarios) : null));
var app = builder.Build();
var groups = await app.MapCoordinatorServerAsync();
groups.Admin.MapLabMode();
app.Run();
