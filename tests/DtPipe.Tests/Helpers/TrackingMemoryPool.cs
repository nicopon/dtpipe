using Apache.Arrow;
using Apache.Arrow.Memory;
using System.Collections.Concurrent;
using System.Buffers;

namespace DtPipe.Tests.Helpers;

public class TrackingMemoryPool : MemoryAllocator
{
    private long _activeAllocations;
    public long ActiveAllocations => Interlocked.Read(ref _activeAllocations);
    public long TotalAllocations { get; private set; }

    public TrackingMemoryPool() : base()
    {
    }

    protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
    {
        // Use the native allocator for actual work
        var owner = NativeMemoryAllocator.Default.Value.Allocate(length);
        Interlocked.Increment(ref _activeAllocations);
        TotalAllocations++;
        bytesAllocated = length; // Approximation
        return new TrackingMemoryOwner(owner, this);
    }

    private class TrackingMemoryOwner : IMemoryOwner<byte>
    {
        private readonly IMemoryOwner<byte> _inner;
        private readonly TrackingMemoryPool _pool;
        private bool _disposed;

        public Memory<byte> Memory => _inner.Memory;

        public TrackingMemoryOwner(IMemoryOwner<byte> inner, TrackingMemoryPool pool)
        {
            _inner = inner;
            _pool = pool;
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _inner.Dispose();
                Interlocked.Decrement(ref _pool._activeAllocations);
                _disposed = true;
            }
        }
    }
}
