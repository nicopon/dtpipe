using System.Globalization;
using System.Text;
using System.Threading.Channels;
using Apache.Arrow;

namespace DtPipe.Processors.DataFusion;

/// <summary>
/// Materializes a DtPipe Arrow memory channel into a temporary CSV file
/// so that DataFusion can read it via RegisterCsvAsync.
/// CSV is used instead of Arrow IPC because RecordBatches from the Row→Arrow bridge
/// have partially initialized native buffers that cause NullRef in Apache.Arrow.Ipc.ArrowStreamWriter.
/// </summary>
internal static class ArrowChannelMaterializer
{
    /// <summary>
    /// Consumes the Arrow channel until completion and writes batches to CSV.
    /// Returns the path to the CSV file.
    /// </summary>
    public static async Task<string> MaterializeToTempFileAsync(
        Channel<RecordBatch> channel,
        Schema schema,
        string alias,
        CancellationToken ct = default)
    {
        var tmpPath = Path.Combine(Path.GetTempPath(), "dtpipe", $"{alias}_{Guid.NewGuid():N}.csv");

        var directory = Path.GetDirectoryName(tmpPath);
        if (directory != null)
        {
            Directory.CreateDirectory(directory);
        }

        await using var fileStream = File.Create(tmpPath);
        await using var writer = new StreamWriter(fileStream, Encoding.UTF8);

        bool headerWritten = false;

        await foreach (var batch in channel.Reader.ReadAllAsync(ct))
        {
            if (!headerWritten)
            {
                // Write CSV header from the batch schema
                var header = string.Join(",", batch.Schema.FieldsList.Select(f => f.Name));
                await writer.WriteLineAsync(header);
                headerWritten = true;
            }

            // Write each row
            for (int r = 0; r < batch.Length; r++)
            {
                var sb = new StringBuilder();
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    if (c > 0) sb.Append(',');
                    var val = GetCsvValue(batch.Column(c), r);
                    sb.Append(val);
                }
                await writer.WriteLineAsync(sb.ToString());
            }
        }

        if (!headerWritten)
        {
            // No batches received — write header from the fallback schema
            var header = string.Join(",", schema.FieldsList.Select(f => f.Name));
            await writer.WriteLineAsync(header);
        }

        return tmpPath;
    }

    private static string GetCsvValue(IArrowArray col, int idx)
    {
        var val = col switch
        {
            Int64Array a => a.GetValue(idx)?.ToString(CultureInfo.InvariantCulture),
            Int32Array a => a.GetValue(idx)?.ToString(CultureInfo.InvariantCulture),
            Int16Array a => a.GetValue(idx)?.ToString(CultureInfo.InvariantCulture),
            Int8Array a => a.GetValue(idx)?.ToString(CultureInfo.InvariantCulture),
            UInt64Array a => a.GetValue(idx)?.ToString(CultureInfo.InvariantCulture),
            UInt32Array a => a.GetValue(idx)?.ToString(CultureInfo.InvariantCulture),
            DoubleArray a => a.GetValue(idx)?.ToString(CultureInfo.InvariantCulture),
            FloatArray a => a.GetValue(idx)?.ToString(CultureInfo.InvariantCulture),
            BooleanArray a => a.GetValue(idx)?.ToString(),
            StringArray a => EscapeCsv(a.GetString(idx)),
            _ => col.GetType().Name
        };
        return val ?? "";
    }

    private static string EscapeCsv(string? value)
    {
        if (value == null) return "";
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n'))
            return $"\"{value.Replace("\"", "\"\"")}\"";
        return value;
    }
}
