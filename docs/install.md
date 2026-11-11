# Install

[← Documentation](README.md)

DtPipe is one file of about 250 MB. The .NET runtime, DuckDB and the drivers for PostgreSQL,
MySQL, SQL Server, Oracle and SQLite are inside it. It collects no telemetry and connects only to
the sources and targets you name.

## As a .NET tool

```bash
dotnet tool install -g dtpipe       # needs the .NET 10 SDK
dotnet tool update -g dtpipe
```

## As a binary

Each release publishes one archive per platform on the
[releases page](https://github.com/nicopon/dtpipe/releases):

| Platform | Archive |
|:---|:---|
| Linux | `dtpipe-linux-x64.tar.gz`, `dtpipe-linux-arm64.tar.gz` |
| macOS | `dtpipe-macos-x64.tar.gz`, `dtpipe-macos-arm64.tar.gz` |
| Windows | `dtpipe-windows-x64.exe.zip`, `dtpipe-windows-arm64.exe.zip` |

```bash
curl -sSL https://github.com/nicopon/dtpipe/releases/latest/download/dtpipe-linux-x64.tar.gz | tar xz
sudo mv dtpipe /usr/local/bin/
```

```powershell
$url = "https://github.com/nicopon/dtpipe/releases/latest/download/dtpipe-windows-x64.exe.zip"
Invoke-WebRequest -Uri $url -OutFile dtpipe.zip
Expand-Archive -Path dtpipe.zip -DestinationPath $env:LOCALAPPDATA\dtpipe -Force
[Environment]::SetEnvironmentVariable("PATH", "$env:PATH;$env:LOCALAPPDATA\dtpipe", "User")
```

No installer and no admin rights: unpack it anywhere.

## From source

```bash
git clone https://github.com/nicopon/dtpipe.git && cd dtpipe
./build.sh            # .\build.ps1 on Windows; runs the unit tests, then writes dist/release/
```

## Shell completion

```bash
dtpipe completion --install        # bash, zsh or PowerShell, detected
dtpipe completion --uninstall
```

## Check

```bash
dtpipe --version
dtpipe providers        # what this binary can read and write (--format json for a script)
```

---

Next: [Quickstart](quickstart.md). The common flags: [REFERENCE.md](../REFERENCE.md).
