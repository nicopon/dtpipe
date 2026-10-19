# Running the coordinator and the runner

The images of a distributed pipeline's two hosts come from one Dockerfile, `deploy/Dockerfile`.
A deployment builds them from a checkout and runs them with its own orchestrator: nothing here
publishes an image, starts a container for you or ships a chart. This page is the list of what a
deployment must provide. The behaviour of the server is in
[src/DtPipe.Coordinator.Server/CLAUDE.md](../src/DtPipe.Coordinator.Server/CLAUDE.md); the
settings a node host takes are `DtPipeNode__*` (the runner) or the host team's own.

This file is not covered by the repository's documentation checks; keep lines to 100 columns.

## The images

```bash
docker build -f deploy/Dockerfile --target coordinator \
    --build-arg SOURCE_REVISION=$(git rev-parse HEAD) -t dtpipe-coordinator .
docker build -f deploy/Dockerfile --target runner \
    --build-arg SOURCE_REVISION=$(git rev-parse HEAD) -t dtpipe-runner .
```

- `SOURCE_REVISION` is required and must be the full commit id. The build fails without it, and
  fails again if the `dtpipe` binary or a host does not report it. It is also the image's
  `org.opencontainers.image.revision` label, which is how a deployment tells what it runs.
- Both run as `1654:1654` (the `app` user of the .NET images), listen on 8080 and keep their
  state in `/state`, which that user owns. A named volume mounted there inherits the owner; a
  bind mount must be owned by that uid. Another uid works if it can write `/state` and `/tmp`.
- Run them under an init (`docker run --init`, `init: true` in compose): the runner starts
  `dtpipe` child processes, and dotnet as PID 1 ignores the signal of its own crash.
- `GET /healthz` answers 200 whenever the coordinator serves; it is anonymous.

## What a deployment provides

- **Redis** (`Coordinator__Redis__ConnectionString`, a secret). It holds the rights, the run
  journal, the armed deployments and the session keys. Persist every write (`appendonly yes`,
  `appendfsync always`). The session keys are stored unencrypted: protect Redis with TLS and an
  ACL. Without Redis the coordinator does not start.
- **The first start** (`Coordinator__Redis__Initialize=true`), on an empty Redis only. Leave it
  unset afterwards, so that a Redis that lost its data stops the start instead of beginning again.
- **The library.** With `Coordinator__Library__Remote__Url` (and `__Token`, a secret, `__User`,
  `__Timeout`), every save is pushed to that git remote before it is accepted. Without it,
  `/state/library` holds the **only copy**: back that directory up, or accept losing the library
  with it. A deployment chooses one of the two.
- **An identity provider** (`Coordinator__Authority`, https in production). The pages sign
  people in with `Coordinator__Ui__ClientId` and `Coordinator__Ui__ClientSecret` (a secret): a
  confidential client whose redirect URI is `https://<public address>/signin-oidc`.
  `Coordinator__Studio__ClientId` names the public client `dtpipe studio` signs in with.
- **People**: `Coordinator__Roles__Admin`, `__Designer`, `__Reader`. Nobody unlisted may do
  anything.
- **The state directory** (`Coordinator__StateDirectory`, `/state` in the image): scratch files
  and the library's local copy.
- **TLS**, by a proxy in front: see below.

A node host (the runner, or a team's own) takes its coordinator's address and the provider's
client credentials (`DtPipeNode__CoordinatorUrl`, `DtPipeNode__Oidc__*`); its rights are
the coordinator's, never its own.

## HTTPS in front

The coordinator listens in plain HTTP and never terminates TLS: a proxy or ingress does, and
passes the request on with `X-Forwarded-Proto` and `X-Forwarded-Host` (or the original `Host`
header). The server builds from them the address it tells the studio
(`/.well-known/oauth-protected-resource`), the sign-in's return address, and the `Secure` flag
of the session cookie. It believes them **only from the peers listed in
`Coordinator__TrustedProxies__0`, `__1`, … (addresses or CIDR networks)**. Left empty, no
forwarded header is read, so behind a TLS proxy the sign-in would send `http://` return addresses.
List the proxy, never a network a client can reach the server from.

The node channel and the data plane are SignalR: the proxy must pass WebSocket upgrades and
keep long requests open. A node may also reach the coordinator directly, inside the network.

## Secrets

The Redis connection string, the remote's token and the pages' client secret are given to the
container as environment variables by the orchestrator's secret mechanism. They are in no
`ARG`, no `ENV` of the image and no command line, and the server writes none of them to a log or
a response. The token reaches git in the environment of the one process that talks to the remote.
A person who may inspect the container (`docker inspect`, the pod's spec) can read its
environment: give that right with the same care as the secrets.

Checked: the forwarded headers, in `ForwardedHeadersTests` of
[tests/DtPipe.Coordinator.Server.Tests](../tests/DtPipe.Coordinator.Server.Tests). Not checked:
a real TLS proxy, a Linux Docker host, an https remote with a token.
