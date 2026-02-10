using DtPipe.Cli.Abstractions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;
using Xunit;

namespace DtPipe.Tests;

public class SchemaCompatibilityAnalyzerTests
{
	[Fact]
	public void Analyze_WhenTargetDoesNotExist_ReturnsWillBeCreatedStatus()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true)
		};

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, null);

		// Assert
		Assert.True(report.IsCompatible);
		Assert.Equal(2, report.Columns.Count);
		Assert.All(report.Columns, c => Assert.Equal(CompatibilityStatus.WillBeCreated, c.Status));
	}

	[Fact]
	public void Analyze_WhenTargetExistsButEmpty_ReturnsWillBeCreatedStatus()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true)
		};

		var targetSchema = new TargetSchemaInfo([], false, null, null, null);

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		// Assert
		Assert.True(report.IsCompatible);
		Assert.All(report.Columns, c => Assert.Equal(CompatibilityStatus.WillBeCreated, c.Status));
	}

	[Fact]
	public void Analyze_WhenTypesMatch_ReturnsCompatibleStatus()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true)
		};

		var targetSchema = new TargetSchemaInfo(
			new List<TargetColumnInfo>
			{
				new("Id", "INTEGER", typeof(int), false, true, false),
				new("Name", "VARCHAR(100)", typeof(string), true, false, false, 100)
			},
			Exists: true,
			RowCount: 0,
			SizeBytes: 1024,
			PrimaryKeyColumns: new[] { "Id" }
		);

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		// Assert
		Assert.True(report.IsCompatible);
		Assert.Equal(2, report.Columns.Count);
		Assert.All(report.Columns, c => Assert.Equal(CompatibilityStatus.Compatible, c.Status));
	}

	[Fact]
	public void Analyze_WhenColumnMissingInTarget_ReturnsError()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Email", typeof(string), true) // Not in target
        };

		var targetSchema = new TargetSchemaInfo(
			new List<TargetColumnInfo>
			{
				new("Id", "INTEGER", typeof(int), false, true, false)
			},
			Exists: true,
			RowCount: 100,
			SizeBytes: 2048,
			PrimaryKeyColumns: new[] { "Id" }
		);

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		// Assert
		Assert.False(report.IsCompatible);
		Assert.Single(report.Errors);
		Assert.Contains("Email", report.Errors[0]);

		var emailCol = report.Columns.First(c => c.ColumnName == "Email");
		Assert.Equal(CompatibilityStatus.MissingInTarget, emailCol.Status);
	}

	[Fact]
	public void Analyze_WhenExtraColumnInTargetIsNullable_ReturnsWarning()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false)
		};

		var targetSchema = new TargetSchemaInfo(
			new List<TargetColumnInfo>
			{
				new("Id", "INTEGER", typeof(int), false, true, false),
				new("LegacyField", "VARCHAR(50)", typeof(string), true, false, false) // Extra nullable
            },
			Exists: true,
			RowCount: 50,
			SizeBytes: 1024,
			PrimaryKeyColumns: new[] { "Id" }
		);

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		// Assert
		Assert.True(report.IsCompatible); // Warnings don't make it incompatible
		Assert.Single(report.Warnings, w => w.Contains("LegacyField"));

		var legacyCol = report.Columns.First(c => c.ColumnName == "LegacyField");
		Assert.Equal(CompatibilityStatus.ExtraInTarget, legacyCol.Status);
	}

	[Fact]
	public void Analyze_WhenExtraColumnInTargetIsNotNull_ReturnsError()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false)
		};

		var targetSchema = new TargetSchemaInfo(
			new List<TargetColumnInfo>
			{
				new("Id", "INTEGER", typeof(int), false, true, false),
				new("RequiredField", "INTEGER", typeof(int), false, false, false) // Extra NOT NULL
            },
			Exists: true,
			RowCount: 0,
			SizeBytes: null,
			PrimaryKeyColumns: new[] { "Id" }
		);

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		// Assert
		Assert.False(report.IsCompatible);
		Assert.Single(report.Errors, e => e.Contains("RequiredField"));

		var reqCol = report.Columns.First(c => c.ColumnName == "RequiredField");
		Assert.Equal(CompatibilityStatus.ExtraInTargetNotNull, reqCol.Status);
	}

	[Fact]
	public void Analyze_WhenNullabilityConflict_ReturnsWarning()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("Status", typeof(string), true) // Nullable source
        };

		var targetSchema = new TargetSchemaInfo(
			new List<TargetColumnInfo>
			{
				new("Status", "VARCHAR(20)", typeof(string), false, false, false) // NOT NULL target
            },
			Exists: true,
			RowCount: 0,
			SizeBytes: null,
			PrimaryKeyColumns: null
		);

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		// Assert
		Assert.True(report.IsCompatible); // Nullability conflict is a warning, not error

		var statusCol = report.Columns.First(c => c.ColumnName == "Status");
		Assert.Equal(CompatibilityStatus.NullabilityConflict, statusCol.Status);
	}

	[Fact]
	public void Analyze_WhenTargetHasExistingData_AddsWarning()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false)
		};

		var targetSchema = new TargetSchemaInfo(
			new List<TargetColumnInfo>
			{
				new("Id", "INTEGER", typeof(int), false, true, false)
			},
			Exists: true,
			RowCount: 10000,
			SizeBytes: 1024 * 1024 * 5, // 5 MB
			PrimaryKeyColumns: new[] { "Id" }
		);

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		// Assert
		Assert.True(report.IsCompatible);
		// Check for either "10,000" or "10 000" depending on locale
		Assert.Contains(report.Warnings, w => w.Contains("10") && w.Contains("rows"));
	}



	[Fact]
	public void Analyze_NumericUpcast_IsCompatible()
	{
		// Arrange
		var sourceSchema = new List<PipeColumnInfo>
		{
			new("SmallValue", typeof(short), false) // short
        };

		var targetSchema = new TargetSchemaInfo(
			new List<TargetColumnInfo>
			{
				new("SmallValue", "BIGINT", typeof(long), false, false, false) // long - larger
            },
			Exists: true,
			RowCount: 0,
			SizeBytes: null,
			PrimaryKeyColumns: null
		);

		// Act
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		// Assert
		Assert.True(report.IsCompatible);
		Assert.Equal(CompatibilityStatus.Compatible, report.Columns[0].Status);
	}
}
