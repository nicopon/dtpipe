# TransportR DLLs

Built `Release`/`net10.0` from `github.com/nicopon/transportr`,
branch `investigation/arrow-serialization`, commit `4b7de44`. Refreshing these is a port, not a
copy: rebuild from a later commit only after checking what changed against
`DtPipe.Coordinator`'s use of it.

| File | sha256 | Used by |
|---|---|---|
| `TransportR.dll` | `1788d98151f8095d01e7964e40bfe19913a32baab908c23dcb3307b2df8a4194` | core interfaces and models; every project below references it |
| `TransportR.Hub.SignalR.dll` | `7ee5edc082903e40c38df0cca8ae988d9c2e894c53b1a6f551cd4bfb59685925` | `DtPipe.Coordinator` — hosts `CommandHub`, `AddDataHub()`/`UseHub<THub>()` |
| `TransportR.FlowControl.dll` | `1376c0bc8729c5bda3e452adca65d9bd3b18cc8c0e4cdf66f90b086752dfe007` | `DtPipe.Coordinator` — `DefaultFlowControlService`, the plan's edge check |
| `TransportR.Client.SignalR.dll` | `02732b370b862bac5d86c6e2744444084514647cebec00c1a546f64936b0adea` | `DtPipe.Coordinator.Tests` only, standing in for pipeline nodes |
| `TransportR.Serialization.MessagePack.dll` | `06a6e32d601d1c35c0f08dd01482d2c34dbd81a828d03f61a6ab4e5538be02ec` | `DtPipe.Coordinator.Tests` only, paired with the client above |

Not carried: `TransportR.Auth.ClientCredentials.dll` and `TransportR.Serialization.Arrow.dll`,
unreferenced by anything on this branch — the coordinator relays no application data and has no
authentication story yet.
