using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Fake;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class FakeDataTransformerTests
{
	[Fact]
	public void Constructor_WithFake_ShouldParseCorrectly()
	{
		var options = new FakeOptions { Fake = new[] { "CITY:address.city", "NAME:name.firstname" } };
		var transformer = new FakeDataTransformer(options);
		transformer.HasFake.Should().BeTrue();
	}

	[Fact]
	public void Constructor_ShouldThrowArgumentException_WhenBothDeterministicAndSeedColumnSet()
	{
		var options = new FakeOptions
		{
			Fake = new[] { "NAME:name.firstname" },
			Deterministic = true,
			SeedColumn = "ID"
		};
		var act = () => new FakeDataTransformer(options);
		act.Should().Throw<ArgumentException>().WithMessage("*cannot be used together*");
	}

	[Fact]
	public async Task Transform_ShouldReplaceValues_WhenMappingExists()
	{
		var options = new FakeOptions { Fake = new[] { "Name:name.firstname", "City:address.city" }, Seed = 12345 };
		var transformer = new FakeDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("ID", typeof(int), false),
			new("Name", typeof(string), true),
			new("City", typeof(string), true)
		};

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns,
			new object?[] { 1, "OldName1", "OldCity1" },
			new object?[] { 2, "OldName2", "OldCity2" });
		var result = await transformer.TransformBatchAsync(batch);

		result.Should().NotBeNull();
		result!.Length.Should().Be(2);

		// ID unchanged
		TestBatchBuilder.GetVal(result, 0, 0).Should().Be(1);
		TestBatchBuilder.GetVal(result, 0, 1).Should().Be(2);

		// Name and City replaced
		TestBatchBuilder.GetVal(result, 1, 0).Should().NotBe("OldName1").And.NotBeNull();
		TestBatchBuilder.GetVal(result, 2, 0).Should().NotBe("OldCity1").And.NotBeNull();
		TestBatchBuilder.GetVal(result, 1, 1).Should().NotBe("OldName2").And.NotBeNull();
		TestBatchBuilder.GetVal(result, 2, 1).Should().NotBe("OldCity2").And.NotBeNull();
	}

	[Fact]
	public async Task Transform_ShouldBeCaseInsensitive_ForColumnNames()
	{
		var options = new FakeOptions { Fake = new[] { "NAME:name.firstname" }, Seed = 123 };
		var transformer = new FakeDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("name", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "Original" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().NotBe("Original");
	}

	[Fact]
	public async Task Transform_ShouldBeDeterministic_WithSeed()
	{
		var mappings = new[] { "NAME:name.firstname" };
		var columns = new List<PipeColumnInfo> { new("NAME", typeof(string), true) };

		var transformer1 = new FakeDataTransformer(new FakeOptions { Fake = mappings, Seed = 42 });
		var transformer2 = new FakeDataTransformer(new FakeOptions { Fake = mappings, Seed = 42 });

		await transformer1.InitializeAsync(columns, TestContext.Current.CancellationToken);
		await transformer2.InitializeAsync(columns, TestContext.Current.CancellationToken);

		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "Original" });
		var result1 = await transformer1.TransformBatchAsync(batch);
		var result2 = await transformer2.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result1!, 0, 0).Should().Be(TestBatchBuilder.GetVal(result2!, 0, 0));
	}

	[Fact]
	public async Task Transform_ShouldRespectLocale()
	{
		var options = new FakeOptions { Fake = new[] { "COUNTRY:address.country" }, Locale = "fr", Seed = 1 };
		var transformer = new FakeDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("COUNTRY", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "Orig" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().BeOfType<string>();
	}

	[Fact]
	public async Task Transform_ShouldFallbackToString_WhenDatasetIsUnknown()
	{
		// "invalid" is not a known dataset → treated as a hardcoded string literal
		var options = new FakeOptions { Fake = new[] { "NAME:invalid.dataset" }, Seed = 123 };
		var transformer = new FakeDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("NAME", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "Original" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be("invalid.dataset");
	}

	[Fact]
	public void Constructor_ShouldThrowException_WhenDatasetValidButMethodInvalid()
	{
		var options = new FakeOptions { Fake = new[] { "NAME:name.invalidmethod" }, Seed = 123 };
		var act = () => new FakeDataTransformer(options);
		act.Should().Throw<InvalidOperationException>()
		   .WithMessage("*Unknown faker method*name.invalidmethod*");
	}

	[Fact]
	public async Task Transform_Reproduction_ColonSyntaxCheck()
	{
		var columns = new List<PipeColumnInfo> { new("IBAN", typeof(string), true) };

		// Case 1: colon syntax "finance:iban" — normalized to real IBAN generator
		var transformerColon = new FakeDataTransformer(new FakeOptions { Fake = new[] { "IBAN:finance:iban" } });
		await transformerColon.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batchColon = TestBatchBuilder.FromRows(columns, new object?[] { "Original" });
		var resColon = await transformerColon.TransformBatchAsync(batchColon);
		var ibanColon = TestBatchBuilder.GetVal(resColon!, 0, 0);
		ibanColon.Should().NotBe("finance:iban").And.NotBeNull();
		ibanColon!.ToString()!.Length.Should().BeGreaterThan(10);

		// Case 2: dot syntax "finance.iban" — canonical faker path
		var transformerDot = new FakeDataTransformer(new FakeOptions { Fake = new[] { "IBAN:finance.iban" } });
		await transformerDot.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batchDot = TestBatchBuilder.FromRows(columns, new object?[] { "Original" });
		var resDot = await transformerDot.TransformBatchAsync(batchDot);
		var ibanDot = TestBatchBuilder.GetVal(resDot!, 0, 0);
		ibanDot.Should().NotBe("finance.iban").And.NotBeNull();
		ibanDot!.ToString()!.Length.Should().BeGreaterThan(10);

		// Case 3: lowercase dot syntax — same result
		var transformerLower = new FakeDataTransformer(new FakeOptions { Fake = new[] { "IBAN:finance.iban" } });
		await transformerLower.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batchLower = TestBatchBuilder.FromRows(columns, new object?[] { "Original" });
		var resLower = await transformerLower.TransformBatchAsync(batchLower);
		TestBatchBuilder.GetVal(resLower!, 0, 0).Should().NotBe("finance.iban");
	}

	[Fact]
	public async Task Transform_ShouldSkipFake_WhenSkipNullEnabled_AndValueIsNull()
	{
		var options = new FakeOptions { Fake = new[] { "NAME:name.firstname" }, SkipNull = true, Seed = 123 };
		var transformer = new FakeDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("NAME", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns,
			new object?[] { null },
			new object?[] { "Original" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().BeNull("Should not fake null because SkipNull is true");
		TestBatchBuilder.GetVal(result!, 0, 1).Should().NotBe("Original").And.NotBeNull("Should still fake non-null values");
	}
}
