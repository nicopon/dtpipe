using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Transformers.Services;

internal static class ArrowColumnOperations
{
    public static RecordBatch CompactBatch(RecordBatch batch, bool[] mask, int selectedCount)
    {
        int colCount = batch.ColumnCount;
        var newArrays = new IArrowArray[colCount];
        for (int colIdx = 0; colIdx < colCount; colIdx++)
            newArrays[colIdx] = CompactArray(batch.Column(colIdx), batch.Schema.GetFieldByIndex(colIdx), mask, selectedCount);

        return new RecordBatch(batch.Schema, newArrays, selectedCount);
    }

    public static IArrowArray CompactArray(IArrowArray original, Field field, bool[] mask, int count)
    {
        if (field.HasMetadata)
            return CompactViaMapper(original, field, mask);

        switch (original)
        {
            case Int32Array a when a.NullCount == 0:
                {
                    var buf = new ArrowBuffer.Builder<int>(count);
                    buf.Resize(count);
                    var dst = buf.Span;
                    var src = a.Values;
                    int k = 0;
                    for (int i = 0; i < mask.Length; i++)
                        if (mask[i]) dst[k++] = src[i];
                    return new Int32Array(buf.Build(), ArrowBuffer.Empty, count, 0, 0);
                }
            case Int32Array a:
                {
                    var builder = new Int32Array.Builder();
                    for (int i = 0; i < mask.Length; i++)
                    {
                        if (!mask[i]) continue;
                        if (a.IsNull(i)) builder.AppendNull();
                        else builder.Append(a.Values[i]);
                    }
                    return builder.Build();
                }
            case Int64Array a when a.NullCount == 0:
                {
                    var buf = new ArrowBuffer.Builder<long>(count);
                    buf.Resize(count);
                    var dst = buf.Span;
                    var src = a.Values;
                    int k = 0;
                    for (int i = 0; i < mask.Length; i++)
                        if (mask[i]) dst[k++] = src[i];
                    return new Int64Array(buf.Build(), ArrowBuffer.Empty, count, 0, 0);
                }
            case Int64Array a:
                {
                    var builder = new Int64Array.Builder();
                    for (int i = 0; i < mask.Length; i++)
                    {
                        if (!mask[i]) continue;
                        if (a.IsNull(i)) builder.AppendNull();
                        else builder.Append(a.Values[i]);
                    }
                    return builder.Build();
                }
            case DoubleArray a when a.NullCount == 0:
                {
                    var buf = new ArrowBuffer.Builder<double>(count);
                    buf.Resize(count);
                    var dst = buf.Span;
                    var src = a.Values;
                    int k = 0;
                    for (int i = 0; i < mask.Length; i++)
                        if (mask[i]) dst[k++] = src[i];
                    return new DoubleArray(buf.Build(), ArrowBuffer.Empty, count, 0, 0);
                }
            case DoubleArray a:
                {
                    var builder = new DoubleArray.Builder();
                    for (int i = 0; i < mask.Length; i++)
                    {
                        if (!mask[i]) continue;
                        if (a.IsNull(i)) builder.AppendNull();
                        else builder.Append(a.Values[i]);
                    }
                    return builder.Build();
                }
            case FloatArray a when a.NullCount == 0:
                {
                    var buf = new ArrowBuffer.Builder<float>(count);
                    buf.Resize(count);
                    var dst = buf.Span;
                    var src = a.Values;
                    int k = 0;
                    for (int i = 0; i < mask.Length; i++)
                        if (mask[i]) dst[k++] = src[i];
                    return new FloatArray(buf.Build(), ArrowBuffer.Empty, count, 0, 0);
                }
            case BooleanArray a:
                {
                    var builder = new BooleanArray.Builder();
                    for (int i = 0; i < mask.Length; i++)
                    {
                        if (!mask[i]) continue;
                        if (a.IsNull(i)) builder.AppendNull();
                        else builder.Append(a.GetValue(i)!.Value);
                    }
                    return builder.Build();
                }
            case StringArray a:
                {
                    var builder = new StringArray.Builder();
                    for (int i = 0; i < mask.Length; i++)
                    {
                        if (!mask[i]) continue;
                        AppendCell(builder, a, i);
                    }
                    return builder.Build();
                }
            case Decimal128Array a:
                {
                    var builder = new Decimal128Array.Builder((Decimal128Type)a.Data.DataType);
                    for (int i = 0; i < mask.Length; i++)
                    {
                        if (!mask[i]) continue;
                        if (a.IsNull(i)) builder.AppendNull();
                        else builder.Append(a.GetValue(i)!.Value);
                    }
                    return builder.Build();
                }
            default:
                return CompactViaMapper(original, field, mask);
        }
    }

    private static IArrowArray CompactViaMapper(IArrowArray original, Field field, bool[] mask)
    {
        var reader = ArrowTypeMap.CompileValueReader(original, field);
        var builder = ArrowTypeMapper.CreateBuilder(original.Data.DataType);
        var append = ArrowTypeMapper.ResolveAppender(builder);
        for (int i = 0; i < mask.Length; i++)
        {
            if (mask[i])
                append(reader(i));
        }
        return ArrowTypeMapper.BuildArray(builder);
    }

    public static IArrowArray GatherScalarArray(
        IArrowArray original,
        Field field,
        ReadOnlySpan<int> offsets,
        int inLen,
        int outLen)
    {
        if (field.HasMetadata)
            return GatherViaMapper(original, field, offsets, inLen);

        switch (original)
        {
            case Int32Array a when a.NullCount == 0:
                {
                    var buf = new ArrowBuffer.Builder<int>(outLen);
                    buf.Resize(outLen);
                    var dst = buf.Span;
                    var src = a.Values;
                    int pos = 0;
                    for (int i = 0; i < inLen; i++)
                    {
                        int rep = offsets[i + 1] - offsets[i];
                        if (rep > 0)
                        {
                            dst.Slice(pos, rep).Fill(src[i]);
                            pos += rep;
                        }
                    }
                    return new Int32Array(buf.Build(), ArrowBuffer.Empty, outLen, 0, 0);
                }
            case Int32Array a:
                {
                    var builder = new Int32Array.Builder();
                    for (int i = 0; i < inLen; i++)
                    {
                        int rep = offsets[i + 1] - offsets[i];
                        if (a.IsNull(i))
                        {
                            for (int r = 0; r < rep; r++) builder.AppendNull();
                        }
                        else
                        {
                            int v = a.Values[i];
                            for (int r = 0; r < rep; r++) builder.Append(v);
                        }
                    }
                    return builder.Build();
                }
            case Int64Array a when a.NullCount == 0:
                {
                    var buf = new ArrowBuffer.Builder<long>(outLen);
                    buf.Resize(outLen);
                    var dst = buf.Span;
                    var src = a.Values;
                    int pos = 0;
                    for (int i = 0; i < inLen; i++)
                    {
                        int rep = offsets[i + 1] - offsets[i];
                        if (rep > 0)
                        {
                            dst.Slice(pos, rep).Fill(src[i]);
                            pos += rep;
                        }
                    }
                    return new Int64Array(buf.Build(), ArrowBuffer.Empty, outLen, 0, 0);
                }
            case Int64Array a:
                {
                    var builder = new Int64Array.Builder();
                    for (int i = 0; i < inLen; i++)
                    {
                        int rep = offsets[i + 1] - offsets[i];
                        if (a.IsNull(i))
                        {
                            for (int r = 0; r < rep; r++) builder.AppendNull();
                        }
                        else
                        {
                            long v = a.Values[i];
                            for (int r = 0; r < rep; r++) builder.Append(v);
                        }
                    }
                    return builder.Build();
                }
            case DoubleArray a when a.NullCount == 0:
                {
                    var buf = new ArrowBuffer.Builder<double>(outLen);
                    buf.Resize(outLen);
                    var dst = buf.Span;
                    var src = a.Values;
                    int pos = 0;
                    for (int i = 0; i < inLen; i++)
                    {
                        int rep = offsets[i + 1] - offsets[i];
                        if (rep > 0)
                        {
                            dst.Slice(pos, rep).Fill(src[i]);
                            pos += rep;
                        }
                    }
                    return new DoubleArray(buf.Build(), ArrowBuffer.Empty, outLen, 0, 0);
                }
            case DoubleArray a:
                {
                    var builder = new DoubleArray.Builder();
                    for (int i = 0; i < inLen; i++)
                    {
                        int rep = offsets[i + 1] - offsets[i];
                        if (a.IsNull(i))
                        {
                            for (int r = 0; r < rep; r++) builder.AppendNull();
                        }
                        else
                        {
                            double v = a.Values[i];
                            for (int r = 0; r < rep; r++) builder.Append(v);
                        }
                    }
                    return builder.Build();
                }
            case StringArray a:
                {
                    var builder = new StringArray.Builder();
                    for (int i = 0; i < inLen; i++)
                    {
                        int rep = offsets[i + 1] - offsets[i];
                        for (int r = 0; r < rep; r++)
                            AppendCell(builder, a, i);
                    }
                    return builder.Build();
                }
            default:
                return GatherViaMapper(original, field, offsets, inLen);
        }
    }

    private static IArrowArray GatherViaMapper(IArrowArray original, Field field, ReadOnlySpan<int> offsets, int inLen)
    {
        var reader = ArrowTypeMap.CompileValueReader(original, field);
        var builder = ArrowTypeMapper.CreateBuilder(original.Data.DataType);
        var append = ArrowTypeMapper.ResolveAppender(builder);
        for (int i = 0; i < inLen; i++)
        {
            int rep = offsets[i + 1] - offsets[i];
            if (rep == 0) continue;
            var val = reader(i);
            for (int r = 0; r < rep; r++) append(val);
        }
        return ArrowTypeMapper.BuildArray(builder);
    }

    public static ListArray GatherListArray(ListArray src, ReadOnlySpan<int> offsets, int inLen, int outLen)
    {
        var listType = (ListType)src.Data.DataType;
        var outOffsets = new ArrowBuffer.Builder<int>(outLen + 1);
        outOffsets.Resize(outLen + 1);
        var outOffsetSpan = outOffsets.Span;
        outOffsetSpan[0] = 0;

        int rowPos = 0;
        int elemPos = 0;

        if (src.Values is StringArray itemStrings)
        {
            var valBuilder = new StringArray.Builder();
            for (int i = 0; i < inLen; i++)
            {
                int start = offsets[i];
                int end = offsets[i + 1];
                int len = end - start;
                for (int r = 0; r < len; r++)
                {
                    for (int j = start; j < end; j++)
                    {
                        AppendCell(valBuilder, itemStrings, j);
                    }
                    elemPos += len;
                    outOffsetSpan[++rowPos] = elemPos;
                }
            }
            return new ListArray(listType, outLen, outOffsets.Build(), valBuilder.Build(), ArrowBuffer.Empty, 0, 0);
        }

        var elemReader = ArrowTypeMap.CompileValueReader(src.Values, listType.ValueField);
        var elemBuilder = ArrowTypeMapper.CreateBuilder(listType.ValueDataType);
        var appendElem = ArrowTypeMapper.ResolveAppender(elemBuilder);

        for (int i = 0; i < inLen; i++)
        {
            int start = offsets[i];
            int end = offsets[i + 1];
            int len = end - start;
            for (int r = 0; r < len; r++)
            {
                for (int j = start; j < end; j++)
                    appendElem(elemReader(j));
                elemPos += len;
                outOffsetSpan[++rowPos] = elemPos;
            }
        }

        return new ListArray(listType, outLen, outOffsets.Build(), ArrowTypeMapper.BuildArray(elemBuilder), ArrowBuffer.Empty, 0, 0);
    }

    /// <summary>
    /// The rows <c>[start, start + length)</c> of <paramref name="values"/> as a view over the same
    /// buffers, which it retains: the view outlives the batch that owned the array, and the caller owns
    /// the view. The buffers are never copied, so it holds for every array type, the ones with no builder
    /// included.
    /// </summary>
    public static IArrowArray SliceArray(IArrowArray values, int start, int length)
        => global::Apache.Arrow.ArrowArrayFactory.SliceShared(values, start, length);

    // Copies a cell's UTF-8 bytes as they are, with no decoding to a string and encoding back; a null
    // stays a null and an empty string an empty string.
    private static void AppendCell(StringArray.Builder builder, StringArray source, int index)
    {
        var bytes = source.GetBytes(index, out bool isNull);
        if (isNull) builder.AppendNull();
        else builder.Append(bytes);
    }
}
