# TransportR DLLs

Built `Release`/`net10.0` from `github.com/nicopon/transportr`,
branch `investigation/arrow-serialization`, commit `b02cea4`. Refreshing these is a port, not a
copy: rebuild from a later commit only after checking what changed against
`DtPipe.Coordinator`'s use of it.

| File | sha256 | Used by |
|---|---|---|
| `TransportR.dll` | `d31ad2a5dc84d7528a38b9648188a71b1f13945d607854865229de3cae01dc44` | core interfaces and models; every project below references it |
| `TransportR.Hub.SignalR.dll` | `5f5879d75241c8f6106f051c97a625feda4c2cdfd4af17fd7f7060aaf255ba77` | `DtPipe.Coordinator` — hosts `CommandHub`, `AddDataHub()`/`UseHub<THub>()` |
| `TransportR.FlowControl.dll` | `7dcdc3104077344c0871cfceff721dc45edc6dcde0fea26b89e5fcceccb2dc1d` | `DtPipe.Coordinator` — `DefaultFlowControlService`, the plan's edge check |
| `TransportR.Client.SignalR.dll` | `153acb9f794428b385dccd9be48263dd8b1409384dadfb7e1071ef100f59ba13` | `DtPipe.Coordinator.Tests`, standing in for pipeline nodes; `DtPipe.PipelineNode` — the real client, one per fragment |
| `TransportR.Serialization.MessagePack.dll` | `fe2fb8f357a0e78a862499bdfbfed18a526b692084baa5e353489e2046cdd254` | `DtPipe.Coordinator.Tests` and `DtPipe.PipelineNode`, paired with the client above |

Not carried: `TransportR.Auth.ClientCredentials.dll` and `TransportR.Serialization.Arrow.dll`,
unreferenced by anything on this branch — the coordinator relays no application data and has no
authentication story yet.

## History

| Commit | Date | Why |
|---|---|---|
| `4b7de44` | 2026-09-24 | C3 — first refresh, DLL tracking starts here |
| `37c0a88` | 2026-09-24 | Package upgrade on the TransportR side: OpenTelemetry 1.14.0 → 1.19.1 (clears `GHSA-g94r-2vxg-569j`), MessagePack 3.1.4 → 3.1.10 (clears 3 `High` + 9 `Moderate` advisories), `net8.0` support dropped (`net10.0` only — no effect here, already the only TFM used), `Microsoft.Extensions.*` and `Microsoft.AspNetCore.SignalR.Client` → `10.0.12`. `DtPipe.Coordinator.csproj`'s `OpenTelemetry` pin and `NuGetAuditSuppress` entry, and the test project's `MessagePack`/`SignalR.Client`/`TestHost` pins, bumped to match |
| `b02cea4` | 2026-09-24 | `AddDataHub()` now registers a default no-op `IHubProgressMonitor` (`TryAddSingleton`) when the host registers none of its own. Closes the point recorded during C3 — a host that forgot the registration got a hub that accepted `Connect` and opened transfers, then answered 500 on the first data-plane request. `DtPipe.Coordinator` keeps registering its own `LoggingHubProgressMonitor` (real logging, not a no-op) before calling `AddDataHub()`, so it still wins over the new default — no source change needed on this side |
