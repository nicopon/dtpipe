# Install

[← Documentation](README.md)

DtPipe ships as **one file**. There is no runtime to install alongside it, no Python environment,
no ODBC manager and no database client libraries: the drivers for PostgreSQL, MySQL, SQL Server,
Oracle, SQLite and DuckDB are compiled in.

## A binary from the releases page

Every tagged version publishes a self-contained archive per platform:

| Platform | Archive |
|:---|:---|
| Linux | `dtpipe-linux-x64.tar.gz`, `dtpipe-linux-arm64.tar.gz` |
| macOS | `dtpipe-macos-x64.tar.gz`, `dtpipe-macos-arm64.tar.gz` |
| Windows | `dtpipe-windows-x64.exe.zip`, `dtpipe-windows-arm64.exe.zip` |

```bash
curl -sSL https://github.com/nicopon/dtpipe/releases/latest/download/dtpipe-linux-x64.tar.gz | tar xz
sudo mv dtpipe /usr/local/bin/
dtpipe --version
```

Unpack it wherever you like — a CI cache, a mounted volume, a locked-down workstation. It needs no
installer and writes nothing outside the directory it runs in.

## As a .NET global tool

```bash
dotnet tool install -g dtpipe
dtpipe --version
```

Requires the [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0). Update with
`dotnet tool update -g dtpipe`, remove with `dotnet tool uninstall -g dtpipe`.

## From source

```bash
git clone https://github.com/nicopon/dtpipe.git
cd dtpipe
./build.sh          # or ./build.ps1 on PowerShell
```

The binary lands in `dist/release/dtpipe`. `build.sh` runs the unit tests first, so a green build
is a tested build.

## Shell completion

```bash
dtpipe completion --install        # bash, zsh or powershell, auto-detected
dtpipe completion zsh              # print the script instead of installing it
dtpipe completion --uninstall      # remove it from every profile it was added to
```

## Check the install

```bash
dtpipe --version
dtpipe providers        # every reader, writer and stream processor the binary carries
dtpipe --help
```

`dtpipe providers` is the authority on what this build supports — the catalog is derived from the
registered components, so it cannot drift from the binary you are holding.

---

Next: [Quickstart](quickstart.md) · [Concepts](concepts.md)
