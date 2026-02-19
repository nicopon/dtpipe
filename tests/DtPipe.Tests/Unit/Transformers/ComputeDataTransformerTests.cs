using DtPipe.Core.Models;
using DtPipe.Core.Services;
using DtPipe.Transformers.Script;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

public class ComputeDataTransformerTests : IDisposable
{
	private readonly ComputeOptions _options;
	private readonly IJsEngineProvider _jsEngineProvider;

	public ComputeDataTransformerTests()
	{
		_options = new ComputeOptions();
		_jsEngineProvider = new JsEngineProvider();
	}

	public void Dispose()
	{
		_jsEngineProvider?.Dispose();
	}

	[Fact]
	public async Task Initialize_WithScript_PreparesProcessors()
	{
		// Arrange
		var options = new ComputeOptions
		{
			Compute = new[] { "Name:row.Name.substring(0,2)" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new List<PipeColumnInfo>
		{
			new("Name", typeof(string), false)
		};

		// Act
		var result = await transformer.InitializeAsync(columns);

		// Assert
		result.Should().BeEquivalentTo(columns);
	}

	[Fact]
	public async Task Transform_ModifiesValue_UsingScript()
	{
		// Arrange
		var options = new ComputeOptions
		{
			Compute = new[] { "Name:row.Name.toUpperCase()" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), false)
		};

		await transformer.InitializeAsync(columns);
		var row = new object?[] { 1, "Alice" };

		// Act
		transformer.Transform(row);

		// Assert
		row[1].Should().Be("ALICE");
	}

	[Fact]
	public async Task Transform_SequentialExecution_MaintainsState_IfScriptAllows()
	{
		// Verify parallelism is safe with engine locking (handled by provider via ThreadLocal or new instance per thread)

		var options = new ComputeOptions
		{
			Compute = new[] { "Val:row.Val * 2" }
		};
		// Note: Transformer instance is shared across threads?
		// If parallelism happens, Transform is called concurrently.
		// JsEngineProvider uses ThreadLocal, so each thread gets its own engine.

		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new[] { new PipeColumnInfo("Val", typeof(int), false) };
		await transformer.InitializeAsync(columns);

		// Sequential execution
		for (int i = 0; i < 100; i++)
		{
			var row = new object?[] { i };
			transformer.Transform(row);
			((double)row[0]!).Should().Be(i * 2);
		}
	}

	[Fact]
	public async Task Transform_Security_CannotAccessSystem()
	{
		// Require Jint to be in strict mode / sandbox
		var options = new ComputeOptions
		{
			Compute = new[] { "Val:System.IO.File.Exists('foo')" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new[] { new PipeColumnInfo("Val", typeof(string), false) };
		await transformer.InitializeAsync(columns);

		var row = new object?[] { "test" };

		// Act & Assert
		// Should throw Jint.Runtime.JavaScriptException or similar, or return undefined/error
		// Because System is not defined in Jint strict mode by default unless CLR allowed.

		var act = () => transformer.Transform(row);

		// Changed: ComputeDataTransformer now wraps execution errors in InvalidOperationException
		act.Should().Throw<InvalidOperationException>()
		   .WithInnerException<Jint.Runtime.JavaScriptException>();
	}

	[Fact]
	public async Task Transform_SkipNull_DoesNotExecuteScript()
	{
		// Arrange
		var options = new ComputeOptions
		{
			Compute = new[] { "Val:row.Val + '_processed'" },
			SkipNull = true
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new[] { new PipeColumnInfo("Val", typeof(string), true) };
		await transformer.InitializeAsync(columns);

		var row = new object?[] { null };

		// Act
		transformer.Transform(row);

		// Assert
		row[0].Should().BeNull(); // Should remain null

		// Safety check: if it ran, it would likely ideally throw or return "null_processed" (if Jint handles null as null value in JS)
		// Let's verify with non-null matches

		var row2 = new object?[] { "test" };
		transformer.Transform(row2);
		row2[0].Should().Be("test_processed");
	}

	[Fact]
	public async Task Transform_CanAccessOtherColumns_UsingRow()
	{
		// Arrange
		var options = new ComputeOptions
		{
			Compute = new[] { "FullName:return row.FirstName + ' ' + row.LastName;" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new List<PipeColumnInfo>
		{
			new("FirstName", typeof(string), false),
			new("LastName", typeof(string), false),
			new("FullName", typeof(string), false)
		};

		await transformer.InitializeAsync(columns);
		var row = new object?[] { "Alice", "Smith", "" };

		// Act
		transformer.Transform(row);

		// Assert
		row[2].Should().Be("Alice Smith");
	}

	[Fact]
	public async Task InitializeAsync_NewColumnName_AppearsInOutputSchema()
	{
		// Arrange — NUM_DBL does not exist in the input schema
		var options = new ComputeOptions
		{
			Compute = new[] { "NUM_DBL:row.NUM * 2" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new List<PipeColumnInfo>
		{
			new("NUM", typeof(int), false)
		};

		// Act
		var result = await transformer.InitializeAsync(columns);

		// Assert — schema must have grown
		result.Should().HaveCount(2);
		result.Should().Contain(c => c.Name == "NUM_DBL");
	}

	[Fact]
	public async Task Transform_NewVirtualColumn_IsPopulatedInExpandedRow()
	{
		// Arrange — NUM_DBL is a new virtual column not in the input schema
		var options = new ComputeOptions
		{
			Compute = new[] { "NUM_DBL:row.NUM * 2" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new List<PipeColumnInfo>
		{
			new("NUM", typeof(int), false)
		};

		await transformer.InitializeAsync(columns);
		var row = new object?[] { 42 };

		// Act
		var result = transformer.Transform(row);

		// Assert — row must be extended and new column must be computed
		result.Should().NotBeNull();
		result!.Should().HaveCount(2);
		// JS evaluates 42 * 2 = 84 (as double in JS)
		Convert.ToDouble(result[1]).Should().Be(84.0);
	}

	[Fact]
	public async Task Transform_MultipleNewColumns_AllPopulated()
	{
		// Arrange — both NUM_DBL and STR_UPPER are new virtual columns
		var options = new ComputeOptions
		{
			Compute = new[] { "NUM_DBL:row.NUM * 2", "STR_UPPER:row.STR.toUpperCase()" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new List<PipeColumnInfo>
		{
			new("NUM", typeof(int), false),
			new("STR", typeof(string), false)
		};

		await transformer.InitializeAsync(columns);
		var row = new object?[] { 10, "hello" };

		// Act
		var result = transformer.Transform(row);

		// Assert
		result.Should().NotBeNull();
		result!.Should().HaveCount(4);
		Convert.ToDouble(result[2]).Should().Be(20.0); // NUM_DBL
		result[3].Should().Be("HELLO");               // STR_UPPER
	}

	[Fact]
	public async Task InitializeAsync_TypeHint_AppliedToNewVirtualColumn()
	{
		// Arrange — NUM_DBL is new, with an explicit type hint
		var options = new ComputeOptions
		{
			Compute = new[] { "NUM_DBL:row.NUM * 2" },
			ComputeTypes = new Dictionary<string, string> { { "NUM_DBL", "double" } }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new List<PipeColumnInfo>
		{
			new("NUM", typeof(int), false)
		};

		// Act
		var result = await transformer.InitializeAsync(columns);

		// Assert — NUM_DBL must have ClrType = double, not default string
		var newCol = result.Should().ContainSingle(c => c.Name == "NUM_DBL").Subject;
		newCol.ClrType.Should().Be(typeof(double));
	}

	[Fact]
	public async Task Transform_CanAccessColumns_UsingDictionarySyntax()
	{
		// Arrange: Handle columns with spaces using row["Col Name"]
		var options = new ComputeOptions
		{
			Compute = new[] { "Code:row['Product Code']" }
		};
		var transformer = new ComputeDataTransformer(options, _jsEngineProvider);
		var columns = new List<PipeColumnInfo>
		{
			new("Product Code", typeof(string), false),
			new("Code", typeof(string), false)
		};

		await transformer.InitializeAsync(columns);
		var row = new object?[] { "ABC-123", "" };

		// Act
		transformer.Transform(row);

		// Assert
		row[1].Should().Be("ABC-123");
	}
}
