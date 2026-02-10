using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.DryRun;
using Moq;
using Xunit;

namespace DtPipe.Tests.Integration;

public class DryRunConstraintValidationTests
{
	private class MockSchemaInspector : ISchemaInspector, IHasSqlDialect
	{
		public ISqlDialect Dialect { get; set; } = new DtPipe.Core.Dialects.PostgreSqlDialect(); // Default to PG
		public List<TargetColumnInfo> TargetColumns { get; set; } = new();
		public List<string>? TargetPKs { get; set; } = null;
		public List<string>? TargetUniqueCols { get; set; } = null; // Phase 3: Unique Columns
		public bool Exists { get; set; } = true;

		public Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
		{
			if (!Exists) return Task.FromResult<TargetSchemaInfo?>(null);

			// Use TargetColumns directly as they contain IsNullable info
			return Task.FromResult<TargetSchemaInfo?>(
				new TargetSchemaInfo(TargetColumns, true, 0, 0, TargetPKs, TargetUniqueCols)
			);
		}
	}

	private async IAsyncEnumerable<ReadOnlyMemory<object?[]>> GetSampleData(object?[][] rows)
	{
		yield return new ReadOnlyMemory<object?[]>(rows);
		await Task.CompletedTask;
	}

	private void SetupReader(Mock<IStreamReader> readerMock, List<PipeColumnInfo> columns, object?[][] rows)
	{
		readerMock.Setup(r => r.Columns).Returns(columns);
		readerMock.Setup(r => r.ReadBatchesAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
				  .Returns(GetSampleData(rows));
		readerMock.Setup(r => r.OpenAsync(It.IsAny<CancellationToken>()))
				  .Returns(Task.CompletedTask);
	}

	[Fact]
	public async Task Validate_NotNullViolation_ReturnsError()
	{
		// Arrange
		var analyzer = new DryRunAnalyzer();
		var reader = new Mock<IStreamReader>();

		// Source has NULL in 'name'
		var sourceCols = new List<PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
		var rows = new object?[][]
		{
			new object?[] { 1, "Alice" },
			new object?[] { 2, null } // Violation!
        };
		SetupReader(reader, sourceCols, rows);

		var inspector = new MockSchemaInspector
		{
			TargetColumns = new List<TargetColumnInfo>
			{
				new("id", "int", typeof(int), false, true, false), // PK, Not Null
                new("name", "varchar", typeof(string), false, false, false) // NOT NULL
            }
		};

		// Act
		var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, inspector);

		// Assert
		Assert.NotNull(result.ConstraintValidation);
		Assert.False(result.ConstraintValidation.IsValid);
		Assert.NotNull(result.ConstraintValidation.Errors);
		Assert.NotEmpty(result.ConstraintValidation.Errors);
		Assert.Contains("is NOT NULL in target but contains NULL values", result.ConstraintValidation.Errors[0]);
	}

	[Fact]
	public async Task Validate_UniqueViolation_ReturnsWarning()
	{
		// Arrange
		var analyzer = new DryRunAnalyzer();
		var reader = new Mock<IStreamReader>();

		// Source has duplicate 'email'
		var sourceCols = new List<PipeColumnInfo> { new("id", typeof(int), false), new("email", typeof(string), true) };
		var rows = new object?[][]
		{
			new object?[] { 1, "bob@example.com" },
			new object?[] { 2, "bob@example.com" } // Duplicate!
        };
		SetupReader(reader, sourceCols, rows);

		var inspector = new MockSchemaInspector
		{
			TargetColumns = new List<TargetColumnInfo>
			{
				new("id", "int", typeof(int), false, true, false),
				new("email", "varchar", typeof(string), true, false, true) // Unique
            },
			TargetUniqueCols = new List<string> { "email" }
		};

		// Act
		var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, inspector);

		// Assert
		Assert.NotNull(result.ConstraintValidation);
		Assert.False(result.ConstraintValidation.IsValid);
		Assert.NotNull(result.ConstraintValidation.Warnings);
		Assert.NotEmpty(result.ConstraintValidation.Warnings);
		Assert.Contains("is UNIQUE in target but sample contains duplicates", result.ConstraintValidation.Warnings[0]);
	}

	[Fact]
	public async Task Validate_ConstraintSuccess_ReturnsValid()
	{
		// Arrange
		var analyzer = new DryRunAnalyzer();
		var reader = new Mock<IStreamReader>();

		var sourceCols = new List<PipeColumnInfo> { new("id", typeof(int), false), new("email", typeof(string), true) };
		var rows = new object?[][]
		{
			new object?[] { 1, "alice@example.com" },
			new object?[] { 2, "bob@example.com" }
		};
		SetupReader(reader, sourceCols, rows);

		var inspector = new MockSchemaInspector
		{
			TargetColumns = new List<TargetColumnInfo>
			{
				new("id", "int", typeof(int), false, true, false),
				new("email", "varchar", typeof(string), false, false, true) // Not Null, Unique
            },
			TargetUniqueCols = new List<string> { "email" }
		};

		// Act
		var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, inspector);

		// Assert
		Assert.NotNull(result.ConstraintValidation);
		Assert.True(result.ConstraintValidation.IsValid);
		Assert.Empty(result.ConstraintValidation.Errors ?? new List<string>());
		Assert.Empty(result.ConstraintValidation.Warnings ?? new List<string>());
	}

	[Fact]
	public async Task Validate_NullableColumn_AllowsNulls()
	{
		// Arrange
		var analyzer = new DryRunAnalyzer();
		var reader = new Mock<IStreamReader>();

		var sourceCols = new List<PipeColumnInfo> { new("id", typeof(int), false), new("description", typeof(string), true) };
		var rows = new object?[][]
		{
			new object?[] { 1, "test" },
			new object?[] { 2, null } // Null ok
        };
		SetupReader(reader, sourceCols, rows);

		var inspector = new MockSchemaInspector
		{
			TargetColumns = new List<TargetColumnInfo>
			{
				new("id", "int", typeof(int), false, true, false),
				new("description", "varchar", typeof(string), true, false, false) // Nullable
            }
		};

		// Act
		var result = await analyzer.AnalyzeAsync(reader.Object, new List<IDataTransformer>(), 10, inspector);

		// Assert
		Assert.NotNull(result.ConstraintValidation);
		Assert.True(result.ConstraintValidation.IsValid);
	}
}
