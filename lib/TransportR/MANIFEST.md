# TransportR DLLs

Built `Release`/`net10.0` from `github.com/nicopon/transportr`,
branch `investigation/arrow-serialization`, commit `37c0a88`. Refreshing these is a port, not a
copy: rebuild from a later commit only after checking what changed against
`DtPipe.Coordinator`'s use of it.

| File | sha256 | Used by |
|---|---|---|
| `TransportR.dll` | `a8a6dfbedde68ab16da688092e83b6bdffcc33f46d95d667cb97f47ed6072fd0` | core interfaces and models; every project below references it |
| `TransportR.Hub.SignalR.dll` | `01ec0cae6b1e5cc5b991bebbfaeca2288d17d3469ac933334307abb6f0c0ec76` | `DtPipe.Coordinator` — hosts `CommandHub`, `AddDataHub()`/`UseHub<THub>()` |
| `TransportR.FlowControl.dll` | `71e7f26fddd2bd58d819e4e1bfe4e831540b4bed21d5af611fa83d7464f29581` | `DtPipe.Coordinator` — `DefaultFlowControlService`, the plan's edge check |
| `TransportR.Client.SignalR.dll` | `dc9f067e144a841779744c9df6e7b1fbdcaa85d9fd0383431b18ae9c715b568e` | `DtPipe.Coordinator.Tests` only, standing in for pipeline nodes |
| `TransportR.Serialization.MessagePack.dll` | `c7ad4a8de9ab543e7e7404cacd8a1fff304c2d2bc102d04d51ec59fd6e9e682a` | `DtPipe.Coordinator.Tests` only, paired with the client above |

Not carried: `TransportR.Auth.ClientCredentials.dll` and `TransportR.Serialization.Arrow.dll`,
unreferenced by anything on this branch — the coordinator relays no application data and has no
authentication story yet.

## History

| Commit | Date | Why |
|---|---|---|
| `4b7de44` | 2026-09-24 | C3 — first refresh, DLL tracking starts here |
| `37c0a88` | 2026-09-24 | Package upgrade on the TransportR side: OpenTelemetry 1.14.0 → 1.19.1 (clears `GHSA-g94r-2vxg-569j`), MessagePack 3.1.4 → 3.1.10 (clears 3 `High` + 9 `Moderate` advisories), `net8.0` support dropped (`net10.0` only — no effect here, already the only TFM used), `Microsoft.Extensions.*` and `Microsoft.AspNetCore.SignalR.Client` → `10.0.12`. `DtPipe.Coordinator.csproj`'s `OpenTelemetry` pin and `NuGetAuditSuppress` entry, and the test project's `MessagePack`/`SignalR.Client`/`TestHost` pins, bumped to match |
