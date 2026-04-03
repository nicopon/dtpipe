using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Tests.Helpers;

public static class ArrowTestHelper
{
    public static RecordBatch ToRecordBatch(this IEnumerable<object?[]> rows, IReadOnlyList<PipeColumnInfo> columns)
    {
        var schema = ArrowSchemaFactory.Create(columns);
        var rowList = rows.ToList();
        var nRows = rowList.Count;

        var builders = new IArrowArrayBuilder[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            builders[i] = ArrowTypeMapper.CreateBuilder(schema.GetFieldByIndex(i).DataType);
        }

        for (int r = 0; r < nRows; r++)
        {
            var row = rowList[r];
            for (int c = 0; c < columns.Count; c++)
            {
                var val = row[c];
                ArrowTypeMapper.AppendValue(builders[c], val);
            }
        }

        var arrays = builders.Select(b => ArrowTypeMapper.BuildArray(b)).ToList();
        return new RecordBatch(schema, arrays, nRows);
    }
}
