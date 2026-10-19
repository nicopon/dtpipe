# Running the coordinator and the runner

The images of a distributed pipeline's two hosts come from one Dockerfile, `deploy/Dockerfile`.
A deployment builds them from a checkout and runs them with its own orchestrator: nothing here
publishes an image, starts a container for you or ships a chart. This page is the list of what a
deployment must provide. The behaviour of the server is in
[src/DtPipe.Coordinator.Server/CLAUDE.md](../src/DtPipe.Coordinator.Server/CLAUDE.md); the
settings a node host takes are `DtPipeNode__*` (the runner) or the host team's own.

## The images

```bash
docker build -f deploy/Dockerfile --target coordinator \
    --build-arg SOURCE_REVISION=$(git rev-parse HEAD) -t dtpipe-coordinator .
docker build -f deploy/Dockerfile --target runner \
    --build-arg SOURCE_REVISION=$(git rev-parse HEAD) -t dtpipe-runner .
```

- `SOURCE_REVISION` is the full commit id the images are built from, and `.git` is not in the
  build context, so it must be handed over. Given, a wrong value fails the build, and so does a
  `dtpipe` binary or a host that does not report it; it is also the image's
  `org.opencontainers.image.revision` label, which is how a deployment tells what it runs. Left
  out, the build only warns: the binaries report no revision and the label says `unknown`.
- Both run as `1654:1654` (the `app` user of the .NET images), listen on 8080 and keep their
  state in `/state`, which that user owns. A named volume mounted there inherits the owner; a
  bind mount must be owned by that uid. The runner's `dtpipe` runs under another uid that can
  write `/state` and `/tmp`; the coordinator under a uid with no home is not checked.
- Run them under an init (`docker run --init`, `init: true` in compose): the runner starts
  `dtpipe` child processes, and dotnet as PID 1 ignores the signal of its own crash.
- `GET /healthz` answers 200 whenever the coordinator serves; it is anonymous.

## The smallest coordinator

One coordinator, one volume, no other service. The coordinator keeps its state in a SQLite file,
`coordinator.db`, and its library as a git repository, both under `/state`:

```bash
docker run -d --init --name coordinator -p 8080:8080 -v coordinator-state:/state \
    -e Coordinator__Initialize=true \
    -e Coordinator__Authority=https://idp.example \
    -e Coordinator__Ui__ClientSecret=... \
    -e Coordinator__PublicUrl=https://coordinator.example \
    dtpipe-coordinator
```

- `Coordinator__Initialize=true` is for the first start only. A store that holds nothing stops the
  start unless it is set, because rights begun again could grant what was revoked and run ids
  would be given twice. Remove it afterwards, so that a volume that was not mounted stops the
  start instead of beginning again.
- The volume is the coordinator's whole state: **back it up**. The database is a single file that
  one process holds; a second coordinator on the same volume is refused at its start.
- `Coordinator__Sqlite__Directory` puts the database in another directory, for example a volume of
  its own mounted at `/data`. The directory must be writable by `1654`.
- A SQLite coordinator is one coordinator. To run several, or to keep the state off the host, use
  Redis: `Coordinator__Store=redis` with `Coordinator__Redis__ConnectionString` (a secret). Redis
  settings given while the store is SQLite stop the start. Persist every Redis write
  (`appendonly yes`, `appendfsync always`), and protect it with TLS and an ACL: the session keys
  are stored unencrypted.

## What a deployment provides

- **A store**: the coordinator's volume (SQLite, the default), or a Redis
  (`Coordinator__Store=redis`).
- **The library.** With `Coordinator__Library__Remote__Url` (and `__Token`, a secret, `__User`,
  `__Timeout`), every save is pushed to that git remote before it is accepted. Without it,
  `/state/library` holds the **only copy**: it is in the volume, so back the volume up, or accept
  losing the library with it. A deployment chooses one of the two.
- **An identity provider** (`Coordinator__Authority`, https in production). The pages sign
  people in with `Coordinator__Ui__ClientId` and `Coordinator__Ui__ClientSecret` (a secret): a
  confidential client whose redirect URI is `https://<public address>/signin-oidc`.
  `Coordinator__Studio__ClientId` names the public client `dtpipe studio` signs in with.
- **People**: `Coordinator__Roles__Admin`, `__Designer`, `__Reader`. Nobody unlisted may do
  anything.
- **TLS**, by a proxy in front, and `Coordinator__PublicUrl`: see below.

A node host (the runner, or a team's own) takes its coordinator's address and the provider's
client credentials (`DtPipeNode__CoordinatorUrl`, `DtPipeNode__Oidc__*`); its rights are
the coordinator's, never its own.

## HTTPS in front

The coordinator listens in plain HTTP and never terminates TLS: a proxy or ingress does. Tell the
coordinator the address people reach it at, once, as `Coordinator__PublicUrl` (for example
`https://coordinator.example`: a scheme, a host and a port if not the default, no path). The
server builds from it the address it tells the studio (`/.well-known/oauth-protected-resource`),
the sign-in's return address, and the `Secure` flag of the session cookie. It reads no
`X-Forwarded-*` header and ignores the `Host` of a request, so no caller can change them. Without
it the request's own scheme and host stand, and behind a TLS proxy the sign-in would send
`http://` return addresses.

The node channel and the data plane are SignalR: the proxy must pass WebSocket upgrades and keep
long requests open. A node may also reach the coordinator directly, inside the network.

## Secrets

The Redis connection string, the remote's token and the pages' client secret are given to the
container as environment variables by the orchestrator's secret mechanism. They are in no
`ARG`, no `ENV` of the image and no command line, and the server writes none of them to a log or
a response. The token reaches git in the environment of the one process that talks to the remote.
A person who may inspect the container (`docker inspect`, the pod's spec) can read its
environment: give that right with the same care as the secrets.

Checked: the public address, in `PublicUrlTests`, and the SQLite store, in `SqliteStoreTests`, of
[tests/DtPipe.Coordinator.Server.Tests](../tests/DtPipe.Coordinator.Server.Tests). Not checked:
a real TLS proxy, a Linux Docker host, an https remote with a token.
