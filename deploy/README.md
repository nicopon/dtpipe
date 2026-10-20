# Running the coordinator and the runner

The images of a distributed pipeline's two hosts come from one Dockerfile, `deploy/Dockerfile`.
Each release publishes them on GHCR, `ghcr.io/nicopon/dtpipe-coordinator` and
`ghcr.io/nicopon/dtpipe-runner`, tagged with the version, its major.minor and `latest`, for
`linux/amd64` and `linux/arm64`. [`deploy/compose.yaml`](./compose.yaml) is a recipe that runs
them to try them. A deployment runs the images with its own orchestrator: nothing here ships a
chart, and there is no image of the `dtpipe` CLI or of a ready-made data node. This page is the
list of what a deployment must provide. The behaviour of the server is in
[src/DtPipe.Coordinator.Server/CLAUDE.md](../src/DtPipe.Coordinator.Server/CLAUDE.md); the
settings a node host takes are `DtPipeNode__*` (the runner) or the host team's own.

## Trying it

```bash
docker compose -f deploy/compose.yaml up --build --wait
```

**Not for production.** The recipe starts a coordinator on Redis, a runner, a data node and an
identity provider, with secrets that are written in the files. It listens in plain HTTP on the
loopback of the host only. Every file it mounts is under `deploy/`; the images of the coordinator
and the runner come from GHCR, and the identity provider and the data node are built from the
checkout (the first start takes a few minutes for them).

- **People.** The pages are at <http://coordinator.localtest.me:8080>. Any user name made of
  letters, digits, `.`, `_` or `-` signs in on the trial identity provider with its name followed
  by `pwd` as the password; `admin` (`adminpwd`) is the coordinator's administrator, and nobody
  else may do anything. The address needs `localtest.me`, a public name that resolves to
  `127.0.0.1`; without a network that resolves it, add `idp.localtest.me` and
  `coordinator.localtest.me` to `/etc/hosts`.
- **A run.** The library holds one pipeline, `trial-totals`: a generated source on the data node,
  summed by the runner, written back to the data node. It runs from the pages, or as a scheduler
  would: a `launcher-1` token (client `launcher-1`, secret `launcher-1-trial-secret`, from
  `http://idp.localtest.me:5190/connect/token`) posted to
  `/service/runs` with `{"pipelineId": "trial-totals"}`. The result is in the data node's
  `/state/totals.csv`. The pipeline is armed first, from its page: launched unarmed, the
  coordinator answers 409.
- **Version.** The identity provider and the data node are built from the checkout you run the
  recipe in, so run it from the checkout of a tag, with `DTPIPE_VERSION` set to that version
  without the `v` (`DTPIPE_VERSION=1.11.0`).
- **Local images.** `DTPIPE_VERSION` picks a published version (default `latest`);
  `DTPIPE_COORDINATOR_IMAGE` and `DTPIPE_RUNNER_IMAGE` name any other image, for example one built
  as [below](#the-images). `SOURCE_REVISION` is handed to the two images the recipe builds.
- **`Coordinator__Initialize` stays on.** The recipe is meant to start again after
  `docker compose down -v`; a deployment sets it for the first start only, as
  [below](#the-smallest-coordinator).
- **The trial identity provider** (`src/DtPipe.Coordinator.TrialIdp`) says who a caller is and
  nothing else, signs with a key kept in its volume, keeps no user, and accepts any name. The
  data node (`src/DtPipe.PipelineNode.TrialNode`) is what a team's own service looks like to the
  coordinator: it adds a node to itself and declares two bricks in a file. Neither is published.

## The images

A release publishes them; to build them from a checkout instead:

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
- `GET /healthz` answers 200 whenever the coordinator serves; it is anonymous. The coordinator
  image checks it itself (`docker ps` shows `healthy`); an orchestrator's own probe replaces that
  check. The runner has no such route and its image no check.

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
- The volume is the coordinator's whole state: **back it up**, as
  [below](#backing-up-a-sqlite-coordinator). The database is a single file that one process holds;
  a second coordinator on the same volume is refused at its start.
- `Coordinator__Sqlite__Directory` puts the database in another directory, for example a volume of
  its own mounted at `/data`. The directory must be writable by `1654`.
- A SQLite coordinator is one coordinator. To run several, or to keep the state off the host, use
  Redis: `Coordinator__Store=redis` with `Coordinator__Redis__ConnectionString` (a secret). Redis
  settings given while the store is SQLite stop the start. Persist every Redis write
  (`appendonly yes`, `appendfsync always`), and protect it with TLS and an ACL: the session keys
  are stored unencrypted.

## Backing up a SQLite coordinator

There is no online backup. One process holds `coordinator.db` exclusively and in WAL mode, so a
copy made while it runs lacks what the journal has not given back to the file, and no other tool
(`sqlite3 .backup`, `litestream`) can open it beside the coordinator. Back it up stopped, and copy
the **whole state**: `coordinator.db`, its `-wal` file if there is one, and `library/`. A file alone
restores the rights and the runs but not the library.

```bash
docker stop coordinator
docker run --rm -v coordinator-state:/state:ro -v "$PWD":/backup alpine \
    tar -C /state -czf /backup/coordinator-state.tgz .
docker start coordinator
```

To restore, stop the coordinator, empty the volume (or use a new one), unpack the copy into it and
start the coordinator **without** `Coordinator__Initialize`:

```bash
docker run --rm -v coordinator-state:/state -v "$PWD":/backup:ro alpine \
    sh -c 'find /state -mindepth 1 -delete && tar -C /state -xzf /backup/coordinator-state.tgz'
```

If `Coordinator__Sqlite__Directory` puts the database on another volume, back up both and restore
both.

- Without `Initialize`, a volume that did not get its database stops the start ("the rights
  document is absent") instead of beginning again, so a restore that missed the file shows at once.
- What comes back is the state of the copy: rights edits, armed pipelines, runs, session keys and
  library saves made since are gone, and the run ids that followed the copy are given again.
- With a library remote (`Coordinator__Library__Remote__Url`), `library/` is aligned to the remote
  at the start: the remote is the authority, so a save pushed since the copy is kept. Without one,
  `library/` in the volume is the only copy, and a restore brings it back as it was at the copy.
- Keep the copy as private as the volume: it holds the session keys, unencrypted.
- A Redis coordinator is backed up through Redis (`appendonly` file or snapshot), and its library
  as above.

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
[tests/DtPipe.Coordinator.Server.Tests](../tests/DtPipe.Coordinator.Server.Tests). Checked on the
real containers, on SQLite: the coordinator ended and started again, and a copy of its state
directory put back, read back the rights, the library, the armed pipelines, the run journal and a
session cookie. Not checked: a real TLS proxy, a Linux Docker host, a volume owned by another uid,
a SQLite file on a network volume, an https remote with a token.
