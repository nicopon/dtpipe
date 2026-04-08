using System.Collections;
using Apache.Arrow;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// A lightweight, read-only view over a single row of a RecordBatch.
/// Does not allocate an object?[] — values are extracted on demand.
/// Since it is a struct capturing the rowIndex, it is safer than a reusable class instance.
/// </summary>
public readonly struct ArrowRowView : IReadOnlyList<object?>
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
}
