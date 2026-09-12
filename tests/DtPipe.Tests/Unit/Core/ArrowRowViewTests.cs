using Apache.Arrow;
using AwesomeAssertions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class ArrowRowViewTests
{
	private static RecordBatch Batch()
	{
		var columns = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true)
		};
		var schema = ArrowSchemaFactory.Create(columns);
		var ids = new Int32Array.Builder().Append(7).Append(8).Build();
		var names = new StringArray.Builder().Append("alpha").AppendNull().Build();
		return new RecordBatch(schema, [ids, names], 2);
	}

	[Fact]
	public void ToArray_ThroughTheInterface_MaterializesTheSameValues()
	{
		using var batch = Batch();
		IReadOnlyList<object?> row = new ArrowRowView(batch, 0, new Dictionary<string, int> { ["Id"] = 0, ["Name"] = 1 });

		// The shape the pipeline uses everywhere: the cast never matches a struct, so this is
		// Enumerable.ToArray over the view.
		var materialized = row as object?[] ?? row.ToArray();

		materialized.Should().Equal(7, "alpha");
	}

	[Fact]
	public void View_IsACollection_SoEnumerableToArrayCanPreSize()
	{
		using var batch = Batch();
		IReadOnlyList<object?> row = new ArrowRowView(batch, 1, new Dictionary<string, int> { ["Id"] = 0, ["Name"] = 1 });

		row.Should().BeAssignableTo<ICollection<object?>>();
		((ICollection<object?>)row).Count.Should().Be(2);

		var destination = new object?[3];
		((ICollection<object?>)row).CopyTo(destination, 1);
		destination.Should().Equal(null, 8, null);
	}

	[Fact]
	public void Mutators_AreRefused()
	{
		using var batch = Batch();
		ICollection<object?> row = new ArrowRowView(batch, 0, new Dictionary<string, int> { ["Id"] = 0, ["Name"] = 1 });

		row.IsReadOnly.Should().BeTrue();
		((Action)(() => row.Add(1))).Should().Throw<NotSupportedException>();
		((Action)(() => row.Clear())).Should().Throw<NotSupportedException>();
		((Action)(() => row.Remove(1))).Should().Throw<NotSupportedException>();
	}
}
