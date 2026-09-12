using System.Collections;
using Apache.Arrow;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// A lightweight, read-only view over a single row of a RecordBatch.
/// Does not allocate an object?[] — values are extracted on demand.
/// Since it is a struct capturing the rowIndex, it is safer than a reusable class instance.
/// </summary>
/// <remarks>
/// <see cref="ICollection{T}"/> is implemented for one reason: the pipeline is full of
/// <c>row as object?[] ?? row.ToArray()</c>, whose second half binds to
/// <see cref="System.Linq.Enumerable.ToArray"/>. Without a collection to ask for a count, that
/// grows a buffer through the boxed iterator — several times the cost of the array it produces —
/// and the first half can never match, because this is a struct. The mutators throw.
/// </remarks>
public readonly struct ArrowRowView : IReadOnlyList<object?>, ICollection<object?>
{
    private readonly RecordBatch _batch;
    private readonly int _rowIndex;
    private readonly IReadOnlyDictionary<string, int> _nameToIndex;

    public ArrowRowView(RecordBatch batch, int rowIndex, IReadOnlyDictionary<string, int> nameToIndex)
    {
        _batch = batch ?? throw new ArgumentNullException(nameof(batch));
        _rowIndex = rowIndex;
        _nameToIndex = nameToIndex ?? throw new ArgumentNullException(nameof(nameToIndex));
    }

    /// <summary>
    /// Gets the value at the specified column index.
    /// </summary>
    public object? this[int index]
    {
        get
        {
            if (_batch == null) throw new ObjectDisposedException(nameof(ArrowRowView));
            return ArrowTypeMapper.GetValueForField(
                _batch.Column(index),
                _batch.Schema.GetFieldByIndex(index),
                _rowIndex);
        }
    }

    /// <summary>
    /// Gets the value at the specified column name (case-insensitive).
    /// </summary>
    public object? this[string name] => this[_nameToIndex[name]];

    public int Count => _batch?.ColumnCount ?? 0;

    public IEnumerator<object?> GetEnumerator()
    {
        for (int i = 0; i < Count; i++)
            yield return this[i];
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Materializes the view into a physical array.
    /// Use this if you need to store the row data beyond the lifetime of the batch
    /// or if you need to pass it to a component that mutates the array.
    /// </summary>
    public object?[] ToArray()
    {
        var array = new object?[Count];
        for (int i = 0; i < array.Length; i++)
            array[i] = this[i];
        return array;
    }

    bool ICollection<object?>.IsReadOnly => true;

    void ICollection<object?>.CopyTo(object?[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        if (arrayIndex < 0 || array.Length - arrayIndex < Count)
            throw new ArgumentOutOfRangeException(nameof(arrayIndex));

        for (int i = 0; i < Count; i++)
            array[arrayIndex + i] = this[i];
    }

    bool ICollection<object?>.Contains(object? item)
    {
        for (int i = 0; i < Count; i++)
            if (Equals(this[i], item)) return true;
        return false;
    }

    void ICollection<object?>.Add(object? item) => throw new NotSupportedException("ArrowRowView is a read-only view over a RecordBatch row.");
    void ICollection<object?>.Clear() => throw new NotSupportedException("ArrowRowView is a read-only view over a RecordBatch row.");
    bool ICollection<object?>.Remove(object? item) => throw new NotSupportedException("ArrowRowView is a read-only view over a RecordBatch row.");
}
