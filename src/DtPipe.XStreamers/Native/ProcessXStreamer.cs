using System.Diagnostics;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Infrastructure.Arrow;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Runtime.CompilerServices;

namespace DtPipe.XStreamers.Native;

/// <summary>
/// Orchestrates an external `dtpipe engine-duckdb` process to perform SQL joins/transformations.
///
/// Two modes:
///   1. Memory-channel mode (legacy): reads from upstream DAG Arrow channels via _mainAlias/_refAliases.
///   2. Sub-process mode (Piste D): spawns child `dtpipe` processes to convert each source to Arrow IPC,
///      then feeds the results to engine-duckdb via temp files (refs) and stdin pipe (main).
///
/// Mode is selected automatically: if srcMain is non-empty, sub-process mode is used.
/// </summary>
public class ProcessXStreamer : IStreamReader
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _query;
    private readonly string _mainAlias;
    private readonly string[] _refAliases;
    private readonly ILogger _logger;
    private readonly int _batchSize;

    // Piste D: direct source specs
    private readonly string _srcMain;
    private readonly string[] _srcRefs;

    private IReadOnlyList<PipeColumnInfo>? _columns;
    private Process? _engineProcess;
    private ArrowStreamReader? _engineReader;
    private readonly List<string> _tempFiles = new();

    public ProcessXStreamer(
        IMemoryChannelRegistry registry,
        string query,
        string mainAlias,
        string[] refAliases,
        ILogger logger,
        string srcMain = "",
        string[] srcRefs = null!,
        int batchSize = 50000)
    {
        _registry = registry;
        _query = query;
        _mainAlias = mainAlias;
        _refAliases = refAliases;
        _logger = logger;
        _srcMain = srcMain ?? "";
        _srcRefs = srcRefs ?? System.Array.Empty<string>();
        _batchSize = batchSize;
    }

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;

    public async Task OpenAsync(CancellationToken ct = default)
    {
        if (!string.IsNullOrEmpty(_srcMain))
            await OpenWithSubProcessesAsync(ct);
        else
            await OpenWithMemoryChannelsAsync(ct);
    }

    // ──────────────────────────────────────────────────────────────
    // MODE 1 — Memory-channel (legacy)
    // ──────────────────────────────────────────────────────────────

    private async Task OpenWithMemoryChannelsAsync(CancellationToken ct)
    {
        _logger.LogInformation("[duck-engine] Memory-channel mode. Waiting for upstream schemas...");
        Console.Error.WriteLine($"[STREAMER] {DateTime.Now:HH:mm:ss.fff} Waiting for schemas...");

        var mainSchema = await _registry.WaitForArrowChannelSchemaAsync(_mainAlias, ct);
        var refSchemas = await Task.WhenAll(_refAliases.Select(a => _registry.WaitForArrowChannelSchemaAsync(a, ct)));

        Console.Error.WriteLine("[STREAMER] All schemas received");

        var exePath = Environment.ProcessPath ?? "dotnet";
        var startInfo = BuildBaseProcessStartInfo(exePath);

        // Prepare DOT-NET assembly arg if running under dotnet
        PrependDllIfNeeded(startInfo, exePath);

        startInfo.ArgumentList.Add("engine-duckdb");
        startInfo.ArgumentList.Add("--query");
        startInfo.ArgumentList.Add(_query);

        // 1. Materialize refs to Arrow IPC stream files
        var pipingTasks = new List<Task>();
        for (int i = 0; i < _refAliases.Length; i++)
        {
            var alias = _refAliases[i];
            var schema = refSchemas[i];
            var tempFile = CreateTempFile(alias);
            startInfo.ArgumentList.Add("--in");
            startInfo.ArgumentList.Add($"{alias}={tempFile}");
            pipingTasks.Add(Task.Run(() => PipeArrowChannelToFileAsync(alias, schema, tempFile, ct)));
        }

        if (pipingTasks.Count > 0)
        {
            _logger.LogInformation("[duck-engine] Buffering {Count} ref(s) to Arrow IPC files...", pipingTasks.Count);
            await Task.WhenAll(pipingTasks);
            _logger.LogInformation("[duck-engine] Refs buffered.");
            foreach (var f in _tempFiles)
                _logger.LogDebug("[duck-engine] Ref file: {Path} ({Size} bytes)", f, new FileInfo(f).Length);
        }

        // 2. Main via stdin
        startInfo.ArgumentList.Add("--in");
        startInfo.ArgumentList.Add($"{_mainAlias}=/dev/stdin");

        _engineProcess = Process.Start(startInfo) ?? throw new Exception("Failed to start engine-duckdb process.");

        _ = Task.Run(() => PipeArrowChannelToStreamAsync(_mainAlias, mainSchema, _engineProcess.StandardInput.BaseStream, ct));
        StartStderrReader();

        _engineReader = new ArrowStreamReader(_engineProcess.StandardOutput.BaseStream);
        await ReadEngineSchemaAsync(ct);
    }

    // ──────────────────────────────────────────────────────────────
    // MODE 2 — Sub-process (Piste D)
    // ──────────────────────────────────────────────────────────────

    private async Task OpenWithSubProcessesAsync(CancellationToken ct)
    {
        _logger.LogInformation("[duck-engine] Sub-process mode. src-main={Main}, src-refs={Refs}", _srcMain, string.Join(",", _srcRefs));

        var exePath = Environment.ProcessPath ?? throw new Exception("Cannot determine dtpipe executable path for sub-process mode.");

        // Materialize BOTH refs AND main to Arrow IPC files via child dtpipe processes.
        // Note: DuckDB's read_arrow() requires Arrow IPC *file* format (ARROW1 magic),
        //       not the IPC *stream* format. Temp files on disk satisfy this requirement.
        var mainAlias = !string.IsNullOrEmpty(_mainAlias) ? _mainAlias : "main";
        var allMaterializationTasks = new List<Task>();
        var engineInputArgs = new List<string>();

        // 1a. Materialize ref sources
        for (int i = 0; i < _srcRefs.Length; i++)
        {
            var srcRef = _srcRefs[i];
            var alias = _refAliases.Length > i ? _refAliases[i] : $"ref{i}";
            var tempFile = CreateTempFile(alias);
            engineInputArgs.Add($"{alias}={tempFile}");

            var capturedSrc = srcRef;
            var capturedFile = tempFile;
            var capturedAlias = alias;
            allMaterializationTasks.Add(Task.Run(async () =>
            {
                _logger.LogInformation("[duck-engine] Materializing ref '{Alias}' from {Src}...", capturedAlias, capturedSrc);
                await MaterializeSourceToFileAsync(exePath, capturedSrc, capturedFile, ct);
                _logger.LogInformation("[duck-engine] Ref '{Alias}' ready: {Size} bytes", capturedAlias, new FileInfo(capturedFile).Length);
            }));
        }

        // 1b. Materialize main source
        var mainTempFile = CreateTempFile(mainAlias);
        engineInputArgs.Add($"{mainAlias}={mainTempFile}");
        allMaterializationTasks.Add(Task.Run(async () =>
        {
            _logger.LogInformation("[duck-engine] Materializing main '{Alias}' from {Src}...", mainAlias, _srcMain);
            await MaterializeSourceToFileAsync(exePath, _srcMain, mainTempFile, ct);
            _logger.LogInformation("[duck-engine] Main '{Alias}' ready: {Size} bytes", mainAlias, new FileInfo(mainTempFile).Length);
        }));

        // 2. Wait for all materializations
        _logger.LogInformation("[duck-engine] Waiting for {Count} source(s) to materialize...", allMaterializationTasks.Count);
        await Task.WhenAll(allMaterializationTasks);
        _logger.LogInformation("[duck-engine] All sources materialized. Spawning engine-duckdb...");

        // 3. Start engine-duckdb with all inputs as file paths (no stdin pipe needed)
        var enginePsi = new ProcessStartInfo
        {
            FileName = exePath,
            RedirectStandardInput = false, // no stdin needed in file mode
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };
        PrependDllIfNeeded(enginePsi, exePath);
        enginePsi.ArgumentList.Add("engine-duckdb");
        enginePsi.ArgumentList.Add("--query");
        enginePsi.ArgumentList.Add(_query);
        foreach (var arg in engineInputArgs)
        {
            enginePsi.ArgumentList.Add("--in");
            enginePsi.ArgumentList.Add(arg);
        }

        _engineProcess = Process.Start(enginePsi) ?? throw new Exception("Failed to start engine-duckdb.");
        StartStderrReader();

        // 4. Read schema from engine output
        _engineReader = new ArrowStreamReader(_engineProcess.StandardOutput.BaseStream);
        await ReadEngineSchemaAsync(ct);
    }

    // ──────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────

    private async Task MaterializeSourceToFileAsync(string exePath, string source, string destFile, CancellationToken ct)
    {
        var psi = BuildBaseProcessStartInfo(exePath);
        PrependDllIfNeeded(psi, exePath);
        psi.RedirectStandardOutput = false; // writes to file via -o
        psi.RedirectStandardError = true;
        psi.ArgumentList.Add("-i"); psi.ArgumentList.Add(source);
        psi.ArgumentList.Add("-o"); psi.ArgumentList.Add($"arrow:{destFile}");
        psi.ArgumentList.Add("--no-stats");

        using var proc = Process.Start(psi) ?? throw new Exception($"Failed to start materialization process for {source}");

        // Log stderr in background
        _ = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    var line = await proc.StandardError.ReadLineAsync();
                    if (line == null) break;
                    _logger.LogDebug("[mat-sub] {Line}", line);
                }
            }
            catch { /* ignore */ }
        });

        await proc.WaitForExitAsync(ct);
        if (proc.ExitCode != 0)
            throw new Exception($"Materialization of '{source}' failed with exit code {proc.ExitCode}.");
    }

    private async Task<Process> SpawnAndPipeMainAsync(string exePath, string source, Stream engineStdin, CancellationToken ct)
    {
        var psi = BuildBaseProcessStartInfo(exePath);
        PrependDllIfNeeded(psi, exePath);
        psi.RedirectStandardOutput = true;
        psi.RedirectStandardError = true;
        psi.ArgumentList.Add("-i"); psi.ArgumentList.Add(source);
        psi.ArgumentList.Add("-o"); psi.ArgumentList.Add("arrow:-");
        psi.ArgumentList.Add("--no-stats");

        var proc = Process.Start(psi) ?? throw new Exception($"Failed to start main source process for {source}");

        // Log stderr
        _ = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    var line = await proc.StandardError.ReadLineAsync();
                    if (line == null) break;
                    _logger.LogDebug("[main-sub] {Line}", line);
                }
            }
            catch { /* ignore */ }
        });

        // Pump stdout → engine stdin
        _ = Task.Run(async () =>
        {
            try
            {
                await proc.StandardOutput.BaseStream.CopyToAsync(engineStdin, ct);
                engineStdin.Close();
                _logger.LogInformation("[duck-engine] Main source fully piped to engine.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[duck-engine] Error pumping main source to engine.");
            }
        }, ct);

        return proc;
    }

    private async Task ReadEngineSchemaAsync(CancellationToken ct)
    {
        var schemaTask = Task.Run(() => _engineReader!.Schema);

        if (await Task.WhenAny(schemaTask, Task.Delay(15000, ct)) != schemaTask)
            throw new TimeoutException("Timeout waiting for engine-duckdb output schema.");

        var schema = await schemaTask;
        if (schema == null)
            throw new Exception("Engine process failed to produce an Arrow schema. Check engine stderr for details.");

        _columns = ConvertSchemaToPipeColumns(schema);
        _logger.LogInformation("[duck-engine] Engine ready. Output schema: {Count} columns.", _columns.Count);
    }

    private static ProcessStartInfo BuildBaseProcessStartInfo(string exePath) => new()
    {
        FileName = exePath,
        RedirectStandardInput = true,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
        CreateNoWindow = true
    };

    private static void PrependDllIfNeeded(ProcessStartInfo psi, string exePath)
    {
        if (exePath.EndsWith("dotnet", StringComparison.OrdinalIgnoreCase) ||
            exePath.EndsWith("dotnet.exe", StringComparison.OrdinalIgnoreCase))
        {
            var dllPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DtPipe.dll");
            if (!File.Exists(dllPath))
                dllPath = Path.Combine(Path.GetDirectoryName(typeof(ProcessXStreamer).Assembly.Location)!, "DtPipe.dll");
            if (File.Exists(dllPath))
                psi.ArgumentList.Add(dllPath);
        }
    }

    private void StartStderrReader()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                var stderr = _engineProcess!.StandardError;
                while (true)
                {
                    var line = await stderr.ReadLineAsync();
                    if (line == null) break;
                    _logger.LogWarning("ENGINE_STDERR: {Line}", line);
                    await Console.Error.WriteLineAsync($"[engine] {line}");
                    await Console.Error.FlushAsync();
                }
            }
            catch (Exception ex) { _logger.LogDebug("Engine stderr reader failed: {Msg}", ex.Message); }
        });
    }

    private string CreateTempFile(string alias)
    {
        var path = Path.Combine(Path.GetTempPath(), $"dtpipe_{alias}_{Guid.NewGuid():N}.arrow");
        _tempFiles.Add(path);
        return path;
    }

    private async Task PipeArrowChannelToFileAsync(string alias, Schema schema, string filePath, CancellationToken ct)
    {
        try
        {
            var entry = _registry.GetArrowChannel(alias);
            if (!entry.HasValue) return;

            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
            using var writer = new ArrowStreamWriter(fs, schema);
            int totalRows = 0;
            await foreach (var batch in entry.Value.Channel.Reader.ReadAllAsync(ct))
            {
                await writer.WriteRecordBatchAsync(batch, ct);
                totalRows += batch.Length;
            }
            await writer.WriteEndAsync(ct);
            _logger.LogDebug("[duck-engine] Buffered '{Alias}': {Count} rows.", alias, totalRows);
        }
        catch (Exception ex) { _logger.LogError(ex, "Error buffering Arrow channel '{Alias}'", alias); }
    }

    private async Task PipeArrowChannelToStreamAsync(string alias, Schema schema, Stream target, CancellationToken ct)
    {
        try
        {
            var entry = _registry.GetArrowChannel(alias);
            if (!entry.HasValue) return;

            using var writer = new ArrowStreamWriter(target, schema, leaveOpen: true);
            await target.FlushAsync(ct);
            await foreach (var batch in entry.Value.Channel.Reader.ReadAllAsync(ct))
            {
                await writer.WriteRecordBatchAsync(batch, ct);
                await target.FlushAsync(ct);
            }
            await writer.WriteEndAsync(ct);
            await target.FlushAsync(ct);
            target.Close();
        }
        catch (Exception ex) { _logger.LogError(ex, "Error piping Arrow channel '{Alias}' to engine", alias); }
    }

    // ──────────────────────────────────────────────────────────────
    // IStreamReader
    // ──────────────────────────────────────────────────────────────

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_engineProcess == null || _engineReader == null) yield break;

        while (!ct.IsCancellationRequested)
        {
            RecordBatch? batch = await _engineReader.ReadNextRecordBatchAsync(ct);
            if (batch == null) break;

            var rows = new object?[batch.Length][];
            for (int i = 0; i < batch.Length; i++) rows[i] = new object?[batch.Schema.FieldsList.Count];

            for (int colIdx = 0; colIdx < batch.Schema.FieldsList.Count; colIdx++)
            {
                var array = batch.Column(colIdx);
                for (int rowIdx = 0; rowIdx < batch.Length; rowIdx++)
                    rows[rowIdx][colIdx] = GetValue(array, rowIdx);
            }

            yield return new ReadOnlyMemory<object?[]>(rows);
        }

        await _engineProcess.WaitForExitAsync(ct);
    }

    private static object? GetValue(IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;
        return array switch
        {
            StringArray s  => s.GetString(index),
            Int32Array i   => i.GetValue(index),
            Int64Array l   => l.GetValue(index),
            DoubleArray d  => d.GetValue(index),
            BooleanArray b => b.GetValue(index),
            Date64Array d  => d.GetDateTime(index),
            TimestampArray t => t.GetTimestamp(index),
            _ => null
        };
    }

    private IReadOnlyList<PipeColumnInfo> ConvertSchemaToPipeColumns(Schema? schema)
    {
        if (schema == null) return System.Array.Empty<PipeColumnInfo>();
        return schema.FieldsList.Select(f => new PipeColumnInfo(f.Name, GetClrType(f.DataType), f.IsNullable)).ToList();
    }

    private static Type GetClrType(IArrowType type) => type.TypeId switch
    {
        ArrowTypeId.Int32     => typeof(int),
        ArrowTypeId.Int64     => typeof(long),
        ArrowTypeId.Double    => typeof(double),
        ArrowTypeId.Boolean   => typeof(bool),
        ArrowTypeId.String    => typeof(string),
        ArrowTypeId.Date64    => typeof(DateTime),
        ArrowTypeId.Timestamp => typeof(DateTimeOffset),
        _ => typeof(string)
    };

    public async ValueTask DisposeAsync()
    {
        if (_engineProcess != null && !_engineProcess.HasExited)
            _engineProcess.Kill();
        _engineProcess?.Dispose();

        foreach (var f in _tempFiles)
            if (File.Exists(f)) File.Delete(f);

        await ValueTask.CompletedTask;
    }
}
