using System.Text;

namespace DtPipe.Transformers.Fake;

/// <summary>
/// Provides stable hash computation for deterministic faker seeding.
/// Uses FNV-1a which is simple, fast, and stable.
/// Optimized to avoid allocations.
/// </summary>
public static class StableHash
{
    private const uint FnvPrime = 16777619;
    private const uint FnvOffsetBasis = 2166136261;
    
    /// <summary>
    /// Computes a stable 32-bit FNV-1a hash from any value.
    /// Optimized for common types to avoid allocations/boxing where possible.
    /// </summary>
    public static uint Compute(object? value)
    {
        if (value is null) return 0;
        
        // Fast path for common primitives
        if (value is int i) return Compute(i);
        if (value is long l) return Compute(l);
        if (value is string s) return ComputeFromString(s);
        
        return ComputeFromString(value.ToString()!);
    }
    
    public static uint Compute(int value)
    {
        unchecked
        {
            uint hash = FnvOffsetBasis;
            hash ^= (uint)(value & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 8) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 16) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 24) & 0xFF); hash *= FnvPrime;
            return hash;
        }
    }
    
    public static uint Compute(long value)
    {
        unchecked
        {
            uint hash = FnvOffsetBasis;
            hash ^= (uint)(value & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 8) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 16) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 24) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 32) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 40) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 48) & 0xFF); hash *= FnvPrime;
            hash ^= (uint)((value >> 56) & 0xFF); hash *= FnvPrime;
            return hash;
        }
    }

    /// <summary>
    /// Computes a stable 32-bit FNV-1a hash from a string.
    /// Iterates chars to avoid byte array allocation.
    /// </summary>
    public static uint ComputeFromString(string value)
    {
        unchecked
        {
            uint hash = FnvOffsetBasis;
            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                // Mix low byte of char
                hash ^= (uint)(c & 0xFF);
                hash *= FnvPrime;
                // Mix high byte of char
                hash ^= (uint)((c >> 8) & 0xFF);
                hash *= FnvPrime;
            }
            return hash;
        }
    }
}
