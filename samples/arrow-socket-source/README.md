# A service that hosts a dtpipe node and serves its own source

A .NET service that reads an external API, applies its business rules, and offers the result to dtpipe as
an Apache Arrow IPC stream on a socket. It hosts a dtpipe node in its own process (`AddDtPipeNode`), so a
coordinator sees an ordinary data node with four bricks:

| Brick | Kind | What it is |
|:---|:---|:---|
| `orders` | source | A .NET source: the service serves it on an Arrow socket, dtpipe reads it as `arrow:pipe://…` |
| `numbers` | source | A standard dtpipe source, `generate:1000`, run by the dtpipe child |
| `report`, `numbers-report` | sinks | CSV files |

`Program.cs` is the whole registration:

```csharp
var node = builder.Services.AddDtPipeNode(o => { /* name, coordinator, key file */ });
node.AddArrowSource("orders", "Orders of the shop API", OrdersTransform.Schema, (ctx, ct) => OrdersTransform.ReadAsync(api, ct));
node.AddBrick("numbers", BrickKind.Source, "Generated numbers", "input: generate:1000");
```

A brick's branch is YAML text, the branch a job file would hold; a JSON object is YAML too, and reads the same.

## The packages

The service references the node library, `DtPipe.PipelineNode.Hosting`, as a package: it carries the node,
its contracts and the job reader, and depends on TransportR and Apache.Arrow. `AddArrowSource` is in it.

Each run of a fragment is a `dtpipe` child process. The csproj gets the program from the `dtpipe` tool package:

```xml
<PackageReference Include="DtPipe.PipelineNode.Hosting" Version="1.16.1-beta.1" />
<PackageDownload Include="dtpipe" Version="[1.16.0]" />
```

A tool package cannot be a `PackageReference`, so it is downloaded; the node library writes its folder into the
host's `runtimeconfig.json` at build, and a host that names no `DtPipe` of its own runs that package's
`DtPipe.dll` through the dotnet host. The `PackageDownload` line stays in the service's project, since a package's
props are read only after the first restore. The library and the tool package are each named by their own version.
The machine needs the .NET and ASP.NET Core runtimes. A published copy of the service needs the package's folder
beside it, under `dtpipe/`; nothing here copies it.

The csproj takes the library from `dist/node-host` as well as from nuget.org, and `-p:NodeHostVersion=…` names
the version: `spike-node.sh` packs the library there under a version of its own before it builds.

| File | What it does |
|:---|:---|
| `Program.cs` | The registration above, behind the `node` command |
| `OrdersTransform.cs`, `FakeOrdersApi.cs` | The business rules and the fixed Arrow schema; a stand-in external API |
| `spike-node.sh` | Coordinator, runner, service node, previews, two pipelines, three rounds |

## Run it

It uses the `dtpipe` and the coordinator of `dist/release/` (`./build.sh`), and needs `dotnet`, `curl` and `jq`.
The first build downloads the dtpipe package (168 MB).

```bash
samples/arrow-socket-source/spike-node.sh     # port 18080 by default
```

`DTPIPE_DLL=src/DtPipe/bin/Debug/net10.0/DtPipe.dll` makes the node run a local build instead of the package.
The published 1.16.0 does not refuse a stream that ends before its schema in words: use a local build to see
the case read `is truncated: it ends before its schema`.

## What `AddArrowSource` holds

How a real `dtpipe` reads such a source, a failing producer and a reader that leaves included, is held by
`tests/DtPipe.PipelineNode.Tests/ArrowSourceReadTests.cs`.

- **A connection is a request to read.** The socket listens from the server's constructor on, and a host builds
  every hosted service before it starts any, so the node never announces the brick before the socket exists.
- **The socket's path is a variable of the node, never part of the brick.** The brick reads
  `arrow:pipe://${{DTPIPE_SRC_ORDERS}}`: its text is its version, which stays the same when the service restarts
  under another process id, and the coordinator learns the variable's name and nothing of the path.
- **The schema goes first**, before the producer runs, and never depends on the data: the source brick's contract
  is pinned when a pipeline is armed. A batch of another schema ends the stream.
- **The end-of-stream marker means success.** A producer that throws (the external API fails, its call times out)
  ends the stream without it, and the dtpipe run fails.
- **A reader can leave early** (`--limit`, a preview, a sample): the next write fails, and the producer is
  disposed, so it stops calling the external API.
- **Only the current user reaches the socket.** It lives in a folder of the process that no one else can open.
  The path is at most 103 characters on macOS; the registration refuses a longer one.

## Not covered

- Parameters per run (a cursor, a filter): the socket carries no request, and the stream is always the whole data set.
- Copying the dtpipe package beside a published service.
- A node that restarts while a run is in flight, and a service killed without running its shutdown (a run in flight
  leaves its child).
- A reader that leaves while the producer is waiting on the external API: the producer finds out at its next write.
- Windows: the code takes a pipe name there, and has not been run on it.
