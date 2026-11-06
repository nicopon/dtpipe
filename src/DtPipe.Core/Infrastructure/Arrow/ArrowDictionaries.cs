using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// A dictionary-encoded column decoded where a reader hands it over, for every reader that produces
/// Arrow itself. The encoding is storage, not meaning, and past this point no step has to know it: the
/// sampler rebuilds a batch through a builder, which a dictionary has none of, and a columnar writer has
/// no form for one. A dictionary nested in a list or a struct stays encoded, and so does one whose values
/// have no builder: a value still reads through it, and a gate answers for the rest.
/// </summary>
public static class ArrowDictionaries
{
    /// <summary>The schema of the decoded batches; <paramref name="schema"/> itself when it holds nothing to decode.</summary>
    public static Schema Decode(Schema schema)
    {
        if (!schema.FieldsList.Any(IsDecoded)) return schema;

        var fields = schema.FieldsList.Select(f => IsDecoded(f)
            ? new Field(f.Name, ((DictionaryType)f.DataType).ValueType, f.IsNullable, f.HasMetadata ? f.Metadata : null)
            : f);
        return new Schema(fields, schema.Metadata);
    }

    /// <summary>
    /// <paramref name="batch"/> under <paramref name="decoded"/>, the schema <see cref="Decode(Schema)"/>
    /// gave for it. Takes ownership of the batch and returns it unchanged when the two schemas are one.
    /// </summary>
    public static RecordBatch Decode(RecordBatch batch, Schema decoded)
    {
        if (ReferenceEquals(batch.Schema, decoded)) return batch;

        using (batch)
        {
            var columns = new IArrowArray[batch.ColumnCount];
            for (var i = 0; i < columns.Length; i++)
            {
                columns[i] = batch.Column(i) is DictionaryArray dictionary && IsDecoded(batch.Schema.GetFieldByIndex(i))
                    ? Flatten(dictionary)
                    : ArrowOwnership.RetainArray(batch.Column(i));
            }
            return new RecordBatch(decoded, columns, batch.Length);
        }
    }

    private static bool IsDecoded(Field field)
        => field.DataType is DictionaryType dictionary && ArrowTypeMapper.HasBuilder(dictionary.ValueType);

    private static IArrowArray Flatten(DictionaryArray dictionary)
    {
        var builder = ArrowTypeMapper.CreateBuilder(((DictionaryType)dictionary.Data.DataType).ValueType);
        for (var row = 0; row < dictionary.Length; row++)
        {
            if (dictionary.IsNull(row))
                ArrowTypeMapper.AppendNull(builder);
            else
                ArrowTypeMapper.AppendArrayValue(builder, dictionary.Dictionary, Convert.ToInt32(ArrowTypeMapper.GetValue(dictionary.Indices, row)));
        }
        return ArrowTypeMapper.BuildArray(builder);
    }
}
