using System.Text;

namespace QueryDump.Transformers.Fake;

/// <summary>
/// Provides stable hash computation for deterministic faker seeding.
/// Uses FNV-1a which is simple, fast, and stable across .NET versions.
/// </summary>
public static class StableHash
{
    private const uint FnvPrime = 16777619;
    private const uint FnvOffsetBasis = 2166136261;
    
    /// <summary>
    /// Computes a stable 32-bit FNV-1a hash from any value.
    /// Returns 0 for null values.
    /// </summary>
    public static uint Compute(object? value)
    {
        if (value is null) return 0;
        return ComputeFromString(value.ToString()!);
    }
    
    /// <summary>
    /// Computes a stable 32-bit FNV-1a hash from a string.
    /// Used for faker path hashing.
    /// </summary>
    public static uint ComputeFromString(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var hash = FnvOffsetBasis;
        
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= FnvPrime;
        }
        
        return hash;
    }
}
