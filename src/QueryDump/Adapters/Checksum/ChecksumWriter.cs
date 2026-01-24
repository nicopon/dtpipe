using System.Security.Cryptography;
using System.Text;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using Microsoft.Extensions.Logging;

namespace QueryDump.Adapters.Checksum;

public class ChecksumWriter : IDataWriter
{
    private readonly ChecksumOptions _options;
    private readonly ILogger _logger;
    private readonly StringBuilder _buffer = new();
    private readonly SHA256 _hasher = SHA256.Create();
    // Using a simple XOR incremental hash or accumulating text then hashing at end? 
    // For large datasets, we must update incrementally.
    // Approach: Hash each row string representation, then XOR or Add to global state.
    // Better: Feed row bytes directly into incremental hash.
    // However, .NET standard incremental hash requires collecting bytes.
    // Let's stick to: Hash(RowString) -> XOR/Add to Accumulator? 
    // Or: Just keep updating one IncrementalHash instance if available, or just chain hashes.
    
    // Simple robust approach for "Chain Validation":
    // running_hash = SHA256(running_hash + SHA256(current_row))
    // This is order-dependent and content-dependent.
    
    private byte[] _currentHash = new byte[32]; // Start with zeroed
    
    public long BytesWritten { get; private set; }

    public ChecksumWriter(ChecksumOptions options, ILogger<ChecksumWriter> logger)
    {
        _options = options;
        _logger = logger;
    }

    public ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        _logger.LogInformation("Initializing Checksum Writer to {Path}", _options.OutputPath);
        return ValueTask.CompletedTask;
    }

    public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        foreach (var row in rows)
        {
            // Canonicalize row to string: "val1|val2|val3"
            _buffer.Clear();
            for (int i = 0; i < row.Length; i++)
            {
                if (i > 0) _buffer.Append('|');
                
                var val = row[i];
                if (val == null || val == DBNull.Value)
                {
                    _buffer.Append("NULL");
                }
                else if (val is DateTime dt)
                {
                    // Use fixed format for consistency across providers
                    _buffer.Append(dt.ToString("yyyy-MM-dd HH:mm:ss.fff")); 
                }
                else if (val is double d)
                {
                    // Use fixed culture
                    _buffer.Append(d.ToString("G", System.Globalization.CultureInfo.InvariantCulture));
                }
                else
                {
                    _buffer.Append(val.ToString());
                }
            }

            var rowString = _buffer.ToString();
            var rowBytes = Encoding.UTF8.GetBytes(rowString);
            BytesWritten += rowBytes.Length;
            
            // Hash current row
            var rowHash = SHA256.HashData(rowBytes);

            // Update running hash: running = Hash(running + rowHash)
            var combined = new byte[_currentHash.Length + rowHash.Length];
            Buffer.BlockCopy(_currentHash, 0, combined, 0, _currentHash.Length);
            Buffer.BlockCopy(rowHash, 0, combined, _currentHash.Length, rowHash.Length);
            
            _currentHash = SHA256.HashData(combined);
        }
        
        return ValueTask.CompletedTask;
    }

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        var finalHash = Convert.ToHexString(_currentHash);
        _logger.LogInformation("Final Checksum: {Hash}", finalHash);
        
        if (!string.IsNullOrEmpty(_options.OutputPath))
        {
            var output = _options.OutputPath;
            // Handle console output special case? No, file is safer for automation.
            await File.WriteAllTextAsync(output, finalHash, ct);
        }
        else
        {
            Console.WriteLine(finalHash);
        }
    }

    public ValueTask DisposeAsync()
    {
        _hasher.Dispose();
        return ValueTask.CompletedTask;
    }
}
