using DtPipe.Coordinator;
using TransportR.Hub.SignalR;

var builder = WebApplication.CreateBuilder(args);

// Dev mode only, and no default groups: nothing has an authentication or authorization story yet,
// so every Connect that does not supply groups fails, and the empty matrix refuses every edge. Not
// exercised by any test as shipped - fill both in for an actual deployment.
builder.Services
    .AddCoordinatorHub(configureFlowControl: _ => { })
    .UseDevMode(options => options.AllowAnonymous = true)
    .Build();

var app = builder.Build();

app.MapDataHub(requireAuth: false);

app.Run();
