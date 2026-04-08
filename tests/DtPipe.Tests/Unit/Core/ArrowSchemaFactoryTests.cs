using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class ArrowSchemaFactoryTests
{
	[Fact]
	public void CreateEnriched_ShouldKeepBaseField_WhenRichSchemaHasIncompatibleType()
	{
		// Simulates the main bug scenario: a transformer mutated column "Amount" from Int64 → string,
		// but the reader's original Arrow schema still says Int64. CreateEnriched must keep the
		// PipeColumnInfo-derived StringType, not the stale Int64Type from richSchema.
		var columns = new List<PipeColumnInfo> { new("Amount", typeof(string), true) };
		var richSchema = new Schema(new[] { new Field("Amount", Int64Type.Default, true) }, null);

		var result = ArrowSchemaFactory.CreateEnriched(columns, richSchema);

		result.GetFieldByIndex(0).DataType.Should().BeOfType<StringType>(
			"PipeColumnInfo (typeof(string)) must win when richSchema has an incompatible type");
	}

	[Fact]
	public void CreateEnriched_ShouldUseRichField_WhenBaseTypesAreCompatible()
	{
		// Both PipeColumnInfo and richSchema agree on TimestampType — richSchema wins because it
		// carries richer metadata (timezone). This preserves Timestamp timezone, Decimal
		// precision/scale, and arrow.uuid annotations when no mutation occurred.
		var columns = new List<PipeColumnInfo> { new("CreatedAt", typeof(DateTimeOffset), true) };
		var richField = new Field("CreatedAt", new TimestampType(TimeUnit.Microsecond, "Europe/Paris"), true);
		var richSchema = new Schema(new[] { richField }, null);

		var result = ArrowSchemaFactory.CreateEnriched(columns, richSchema);

		result.GetFieldByIndex(0).Should().BeSameAs(richField,
			"richSchema field should be used to preserve timezone metadata when base types match");
	}

	[Fact]
	public void CreateEnriched_ShouldIgnoreRichColumn_WhenNotPresentInPipeColumnInfo()
	{
		// richSchema may reference a column that was dropped by a transformer. Only columns
		// declared in PipeColumnInfo appear in the output schema.
		var columns = new List<PipeColumnInfo>
		{
			new("A", typeof(string), true),
			new("C", typeof(string), true)
		};
		var richSchema = new Schema(new[]
		{
			new Field("A", StringType.Default, true),
			new Field("B", Int32Type.Default, false), // dropped column
			new Field("C", StringType.Default, true)
		}, null);

		var result = ArrowSchemaFactory.CreateEnriched(columns, richSchema);

		result.FieldsList.Should().HaveCount(2);
		result.FieldsList.Select(f => f.Name).Should().Equal("A", "C");
	}

	[Fact]
	public void CreateEnriched_ShouldAddNewColumn_WhenNotPresentInRichSchema()
	{
		// A transformer may add a virtual column that didn't exist in the reader's schema.
		// That column must still appear in the output with its PipeColumnInfo-derived type.
		var columns = new List<PipeColumnInfo>
		{
			new("Existing", typeof(string), true),
			new("NewVirtual", typeof(string), true)
		};
		var richSchema = new Schema(new[] { new Field("Existing", StringType.Default, true) }, null);

		var result = ArrowSchemaFactory.CreateEnriched(columns, richSchema);

		result.FieldsList.Should().HaveCount(2);
		result.GetFieldByIndex(1).Name.Should().Be("NewVirtual");
		result.GetFieldByIndex(1).DataType.Should().BeOfType<StringType>();
	}

	[Fact]
	public void CreateEnriched_ShouldProduceCorrectFieldCount_WhenRichSchemaMatchesExactly()
	{
		var columns = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true)
		};
		var richSchema = ArrowSchemaFactory.Create(columns); // identical schema

		var result = ArrowSchemaFactory.CreateEnriched(columns, richSchema);

		result.FieldsList.Should().HaveCount(2);
	}
}
