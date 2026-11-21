using Apache.Arrow;
using Bogus;
using Bogus.Bson;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Fake;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

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
    public void Constructor_ShouldThrowArgumentException_WhenBothSeedRowAndSeedColumnsSet()
    {
        var options = new FakeOptions
        {
            Fake = new[] { "NAME:name.firstname" },
            SeedRow = true,
            SeedColumn = new[] { "ID" }
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

    /// <summary>
    /// An unresolvable value is refused, not written into every row. Accepting it as a constant is
    /// what let a recorded session map four columns to 'firstName', 'lastName', 'safeEmail' and
    /// 'membership' and ship two thousand rows of those four words as anonymised data. Writing a
    /// constant is another transformer's job.
    /// </summary>
    [Theory]
    [InlineData("NAME:invalid.dataset")]   // no such dataset
    [InlineData("NAME:firstName")]         // a Bogus method without its dataset
    [InlineData("NAME:Nike")]              // a plain word
    [InlineData("NAME:nope:iban")]         // colon spelling of a path that does not exist
    public void Constructor_ShouldThrow_WhenTheValueIsNotAFakerPath(string mapping)
    {
        var act = () => new FakeDataTransformer(new FakeOptions { Fake = new[] { mapping }, Seed = 123 });

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*Unknown faker path*");
    }

    /// <summary>The refusal carries what the dataset does have, so the next attempt has enough.</summary>
    [Fact]
    public void An_Unknown_Method_Names_The_Methods_Of_Its_Dataset()
    {
        var act = () => new FakeDataTransformer(new FakeOptions { Fake = new[] { "D:date" } });

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("*date.past*").And.Message.Should().NotContain("fake-list");
    }

    /// <summary>A refused path names the datasets, which is where a caller with no dot starts.</summary>
    [Fact]
    public void A_Refused_Path_Names_The_Datasets()
    {
        var act = () => new FakeDataTransformer(new FakeOptions { Fake = new[] { "NAME:firstName" } });

        act.Should().Throw<InvalidOperationException>().WithMessage("*name*");
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

    [Fact]
    public async Task Transform_ShouldSupportMultiSeedColumns()
    {
        // Test multiple seed columns for composite keys (e.g. Region + Branch)
        var options1 = new FakeOptions { Fake = new[] { "NAME:name.firstname" }, SeedColumn = new[] { "Region", "Branch" }, Seed = 42 };
        var options2 = new FakeOptions { Fake = new[] { "NAME:name.firstname" }, SeedColumn = new[] { "Region", "Branch" }, Seed = 42 };

        var transformer1 = new FakeDataTransformer(options1);
        var transformer2 = new FakeDataTransformer(options2);

        var columns = new List<PipeColumnInfo>
        {
            new("Region", typeof(string), false),
            new("Branch", typeof(string), false),
            new("NAME", typeof(string), true)
        };

        await transformer1.InitializeAsync(columns, TestContext.Current.CancellationToken);
        await transformer2.InitializeAsync(columns, TestContext.Current.CancellationToken);

        var batch1 = TestBatchBuilder.FromRows(columns, new object?[] { "North", "A", "Original" });
        var batch2 = TestBatchBuilder.FromRows(columns, new object?[] { "North", "A", "Original" });
        var batch3 = TestBatchBuilder.FromRows(columns, new object?[] { "North", "B", "Original" }); // Different composite key

        var res1 = await transformer1.TransformBatchAsync(batch1);
        var res2 = await transformer2.TransformBatchAsync(batch2);
        var res3 = await transformer1.TransformBatchAsync(batch3);

        var val1 = TestBatchBuilder.GetVal(res1!, 2, 0);
        var val2 = TestBatchBuilder.GetVal(res2!, 2, 0);
        var val3 = TestBatchBuilder.GetVal(res3!, 2, 0);

        // Same seed columns + same values + same global seed = same fake output
        val1.Should().Be(val2);

        // Different composite seed column value = different fake output
        val1.Should().NotBe(val3);
    }

    [Fact]
    public async Task Transform_SeedRow_ShouldBeInfluencedByGlobalSeed()
    {
        // Ensure changing the global seed modifies the output in seed-row mode, but remains reproducible for the same seed
        var optionsSeed42_1 = new FakeOptions { Fake = new[] { "NAME:name.firstname" }, SeedRow = true, Seed = 42 };
        var optionsSeed42_2 = new FakeOptions { Fake = new[] { "NAME:name.firstname" }, SeedRow = true, Seed = 42 };
        var optionsSeed100 = new FakeOptions { Fake = new[] { "NAME:name.firstname" }, SeedRow = true, Seed = 100 };

        var transformer42_1 = new FakeDataTransformer(optionsSeed42_1);
        var transformer42_2 = new FakeDataTransformer(optionsSeed42_2);
        var transformer100 = new FakeDataTransformer(optionsSeed100);

        var columns = new List<PipeColumnInfo> { new("NAME", typeof(string), true) };

        await transformer42_1.InitializeAsync(columns, TestContext.Current.CancellationToken);
        await transformer42_2.InitializeAsync(columns, TestContext.Current.CancellationToken);
        await transformer100.InitializeAsync(columns, TestContext.Current.CancellationToken);

        var batch = TestBatchBuilder.FromRows(columns, new object?[] { "Original" });

        var res42_1 = await transformer42_1.TransformBatchAsync(batch);
        var res42_2 = await transformer42_2.TransformBatchAsync(batch);
        var res100 = await transformer100.TransformBatchAsync(batch);

        var val42_1 = TestBatchBuilder.GetVal(res42_1!, 0, 0);
        var val42_2 = TestBatchBuilder.GetVal(res42_2!, 0, 0);
        var val100 = TestBatchBuilder.GetVal(res100!, 0, 0);

        // Reproducibility check: same global seed = same row-deterministic faking
        val42_1.Should().Be(val42_2);

        // Seed dependency check: changing the global seed must change the row-deterministic output
        val42_1.Should().NotBe(val100);
    }

    [Fact]
    public async Task Transform_SeedColumn_ShouldPreserveCrossTableJoin_AndIgnoreCaseInPath()
    {
        var t1 = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[] { "email:internet.email" },
            SeedColumn = new[] { "customer_id" },
            Seed = 42
        });
        var t2 = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[] { "customer_email:internet.Email", "full_name:name.fullName" },
            SeedColumn = new[] { "customer_id" },
            Seed = 42
        });

        var cols1 = new List<PipeColumnInfo>
        {
            new("customer_id", typeof(string), false),
            new("email", typeof(string), true)
        };
        var cols2 = new List<PipeColumnInfo>
        {
            new("order_id", typeof(int), false),
            new("customer_id", typeof(string), false),
            new("customer_email", typeof(string), true)
        };

        await t1.InitializeAsync(cols1, TestContext.Current.CancellationToken);
        await t2.InitializeAsync(cols2, TestContext.Current.CancellationToken);

        using var b1 = TestBatchBuilder.FromRows(cols1, new object?[] { "CUST-1", "a@b.com" }, new object?[] { "CUST-2", "c@d.com" });
        using var b2 = TestBatchBuilder.FromRows(cols2, new object?[] { 10, "CUST-2", "x" }, new object?[] { 11, "CUST-1", "y" });

        using var r1 = await t1.TransformBatchAsync(b1, TestContext.Current.CancellationToken);
        using var r2 = await t2.TransformBatchAsync(b2, TestContext.Current.CancellationToken);

        TestBatchBuilder.GetVal(r1!, 1, 0).Should().Be(TestBatchBuilder.GetVal(r2!, 2, 1));
        TestBatchBuilder.GetVal(r1!, 1, 1).Should().Be(TestBatchBuilder.GetVal(r2!, 2, 0));
    }

    [Fact]
    public async Task Transform_SeedRow_ShouldKeepColumnIndependent_WhenAddingOrReorderingMappings()
    {
        var cols = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("email", typeof(string), true),
            new("city", typeof(string), true)
        };

        var single = new FakeDataTransformer(new FakeOptions { Fake = new[] { "email:internet.email" }, SeedRow = true, Seed = 9 });
        var reordered = new FakeDataTransformer(new FakeOptions { Fake = new[] { "city:address.city", "email:internet.email" }, SeedRow = true, Seed = 9 });

        await single.InitializeAsync(cols, TestContext.Current.CancellationToken);
        await reordered.InitializeAsync(cols, TestContext.Current.CancellationToken);

        using var batch = TestBatchBuilder.FromRows(cols, new object?[] { 1, "a", "x" }, new object?[] { 2, "b", "y" });
        using var rSingle = await single.TransformBatchAsync(batch, TestContext.Current.CancellationToken);
        using var rReordered = await reordered.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        TestBatchBuilder.GetVal(rSingle!, 1, 0).Should().Be(TestBatchBuilder.GetVal(rReordered!, 1, 0));
        TestBatchBuilder.GetVal(rSingle!, 1, 1).Should().Be(TestBatchBuilder.GetVal(rReordered!, 1, 1));
    }

    [Fact]
    public async Task Transform_VariantSuffix_ShouldProduceDistinctDeterministicValues()
    {
        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[] { "primary:internet.email", "secondary:internet.email#alt" },
            SeedColumn = new[] { "id" },
            Seed = 77
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var batch = TestBatchBuilder.FromRows(cols, new object?[] { 101 }, new object?[] { 102 });
        using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        TestBatchBuilder.GetVal(res!, 1, 0).Should().NotBe(TestBatchBuilder.GetVal(res!, 2, 0));
        TestBatchBuilder.GetVal(res!, 1, 1).Should().NotBe(TestBatchBuilder.GetVal(res!, 2, 1));
    }

    [Fact]
    public async Task Transform_NonDeterministicSamePathWithoutVariant_ShouldDifferBetweenColumnsAndBeReproducibleWithSeed()
    {
        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        var opts = new FakeOptions
        {
            Fake = new[] { "email1:internet.email", "email2:internet.email" },
            Seed = 42
        };

        var t1 = new FakeDataTransformer(opts);
        var t2 = new FakeDataTransformer(opts);
        await t1.InitializeAsync(cols, TestContext.Current.CancellationToken);
        await t2.InitializeAsync(cols, TestContext.Current.CancellationToken);

        using var batch = TestBatchBuilder.FromRows(cols, new object?[] { 1 }, new object?[] { 2 });
        using var res1 = await t1.TransformBatchAsync(batch, TestContext.Current.CancellationToken);
        using var res2 = await t2.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        TestBatchBuilder.GetVal(res1!, 1, 0).Should().NotBe(TestBatchBuilder.GetVal(res1!, 2, 0));
        TestBatchBuilder.GetVal(res1!, 1, 0).Should().Be(TestBatchBuilder.GetVal(res2!, 1, 0));
        TestBatchBuilder.GetVal(res1!, 2, 0).Should().Be(TestBatchBuilder.GetVal(res2!, 2, 0));
    }

    [Fact]
    public async Task Transform_SeedRow_ShouldNotCycleAt65536()
    {
        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[]
            {
                "u:random.uuid",
                "email:internet.email",
                "name:name.fullName"
            },
            SeedRow = true,
            Seed = 55
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);

        // Emit two batches covering index 0 and index 65536
        using var firstBatch = new RecordBatch(
            new Schema.Builder().Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false)).Build(),
            new IArrowArray[] { new Int32Array.Builder().AppendRange(Enumerable.Range(0, 65536)).Build() },
            65536);
        using var secondBatch = TestBatchBuilder.FromRows(cols, new object?[] { 65536 });

        using var res1 = await t.TransformBatchAsync(firstBatch, TestContext.Current.CancellationToken);
        using var res2 = await t.TransformBatchAsync(secondBatch, TestContext.Current.CancellationToken);

        TestBatchBuilder.GetVal(res1!, 1, 0).Should().NotBe(TestBatchBuilder.GetVal(res2!, 1, 0));
        TestBatchBuilder.GetVal(res1!, 2, 0).Should().NotBe(TestBatchBuilder.GetVal(res2!, 2, 0));
        TestBatchBuilder.GetVal(res1!, 3, 0).Should().NotBe(TestBatchBuilder.GetVal(res2!, 3, 0));
    }

    [Fact]
    public async Task Transform_FrenchLocale_ShouldUseFrenchFirstNamesAndAsciiEmails()
    {
        var frNameObj = (BObject)Database.GetLocale("fr")["name"];
        var frMale = ((BArray)frNameObj["male_first_name"]).Cast<BValue>().Select(v => v.StringValue);
        var frFemale = ((BArray)frNameObj["female_first_name"]).Cast<BValue>().Select(v => v.StringValue);
        var frenchFirstNames = new HashSet<string>(frMale.Concat(frFemale), StringComparer.Ordinal);

        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[] { "first:name.firstName", "email:internet.email" },
            Locale = "fr",
            SeedRow = true,
            Seed = 7
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var batch = TestBatchBuilder.FromRows(cols,
            new object?[] { 1 }, new object?[] { 2 }, new object?[] { 3 }, new object?[] { 4 }, new object?[] { 5 });
        using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        for (int r = 0; r < 5; r++)
        {
            var first = (string)TestBatchBuilder.GetVal(res!, 1, r)!;
            var email = (string)TestBatchBuilder.GetVal(res!, 2, r)!;
            frenchFirstNames.Should().Contain(first);
            email.Should().Contain("@").And.MatchRegex(@"^[a-z0-9._-]+@[a-z0-9.-]+$");
        }
    }

    [Theory]
    [InlineData("ja")]
    [InlineData("zh_CN")]
    [InlineData("ar")]
    public async Task Transform_NonLatinLocales_ShouldProduceValidDataAndAsciiEmails(string locale)
    {
        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[]
            {
                "name:name.fullName",
                "email:internet.email",
                "city:address.city",
                "addr:address.fullAddress",
                "phone:phone.phoneNumber"
            },
            Locale = locale,
            SeedColumn = new[] { "id" },
            Seed = 11
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var batch = TestBatchBuilder.FromRows(cols, new object?[] { 1 }, new object?[] { 2 });
        using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        for (int c = 1; c <= 5; c++)
        {
            ((string)TestBatchBuilder.GetVal(res!, c, 0)!).Should().NotBeNullOrWhiteSpace();
        }
        ((string)TestBatchBuilder.GetVal(res!, 2, 0)!).Should().MatchRegex(@"^[a-z0-9._-]+@[a-z0-9.-]+$");
    }

    [Theory]
    [InlineData("en")]
    [InlineData("fr")]
    public async Task Transform_All94Paths_ShouldSucceedAcrossAllSeedingModes(string locale)
    {
        var registry = new FakerRegistry();
        var mappings = registry.ListAll()
            .SelectMany(g => g.Methods.Select(m => $"{g.Dataset}_{m.Method}:{g.Dataset}.{m.Method}"))
            .ToArray();
        mappings.Length.Should().Be(94);

        var cols = new List<PipeColumnInfo> { new("seed_key", typeof(string), true) };

        foreach (var opts in new[]
        {
            new FakeOptions { Fake = mappings, Locale = locale, SeedColumn = new[] { "seed_key" }, Seed = 42 },
            new FakeOptions { Fake = mappings, Locale = locale, SeedRow = true, Seed = 42 },
            new FakeOptions { Fake = mappings, Locale = locale, Seed = 42 }
        })
        {
            var t = new FakeDataTransformer(opts);
            await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
            using var batch = TestBatchBuilder.FromRows(cols, new object?[] { "k1" }, new object?[] { null });
            using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

            res!.Length.Should().Be(2);
            res.ColumnCount.Should().Be(1 + mappings.Length);
            for (int c = 1; c < res.ColumnCount; c++)
            {
                TestBatchBuilder.GetVal(res, c, 0).Should().NotBeNull();
                TestBatchBuilder.GetVal(res, c, 1).Should().NotBeNull();
            }
        }
    }

    [Fact]
    public async Task Transform_CompositeSeedNullSemantics_AndSkipNullTarget()
    {
        var cols = new List<PipeColumnInfo>
        {
            new("k1", typeof(string), true),
            new("k2", typeof(string), true),
            new("target", typeof(string), true)
        };

        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[] { "target:name.firstName", "virt:name.lastName" },
            SeedColumn = new[] { "k1", "k2" },
            SkipNull = true,
            Seed = 99
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var batch = TestBatchBuilder.FromRows(cols,
            new object?[] { "North", null, "orig1" },
            new object?[] { null, "North", "orig2" },
            new object?[] { null, null, "orig3" },
            new object?[] { "North", "A", null });

        using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        var t0 = TestBatchBuilder.GetVal(res!, 2, 0);
        var t1 = TestBatchBuilder.GetVal(res!, 2, 1);
        var t2 = TestBatchBuilder.GetVal(res!, 2, 2);
        var t3 = TestBatchBuilder.GetVal(res!, 2, 3);

        t0.Should().NotBeNull().And.NotBe(t1);
        t2.Should().NotBeNull();
        t3.Should().BeNull();
        TestBatchBuilder.GetVal(res!, 3, 2).Should().NotBeNull();
    }

    [Fact]
    public async Task Transform_RealColumnTypeCoercion_ShouldPreserveTargetSchemaTypes()
    {
        var cols = new List<PipeColumnInfo>
        {
            new("num_long", typeof(long), true),
            new("num_str", typeof(string), true),
            new("uuid_str", typeof(string), true),
            new("price_dec", typeof(decimal), true)
        };

        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[]
            {
                "num_long:random.number",
                "num_str:random.number",
                "uuid_str:random.uuid",
                "price_dec:commerce.price"
            },
            SeedRow = true,
            Seed = 17
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var batch = TestBatchBuilder.FromRows(cols, new object?[] { 1L, "a", "b", 1.0m });
        using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        res!.Column(0).Should().BeOfType<Int64Array>();
        res.Column(1).Should().BeOfType<StringArray>();
        res.Column(2).Should().BeOfType<StringArray>();
        res.Column(3).Should().BeOfType<Decimal128Array>();

        ((string)TestBatchBuilder.GetVal(res, 2, 0)!).Should().MatchRegex(@"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
        ((decimal)TestBatchBuilder.GetVal(res, 3, 0)!).Should().BeGreaterThanOrEqualTo(1.00m);
    }

    [Fact]
    public async Task Transform_Templates_ShouldInterpolateRealFakedAndSelfColumnsWithSkipNull()
    {
        var cols = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("first", typeof(string), true),
            new("tag", typeof(string), true)
        };

        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[]
            {
                "first:name.firstName",
                "tag:User-{tag}",
                "summary:{id}:{first}:{greeting}",
                "greeting:Hi {first}"
            },
            SkipNull = true,
            SeedRow = true,
            Seed = 3
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var batch = TestBatchBuilder.FromRows(cols,
            new object?[] { 10, "Old", "vip" },
            new object?[] { 20, null, null });
        using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        var f0 = (string)TestBatchBuilder.GetVal(res!, 1, 0)!;
        TestBatchBuilder.GetVal(res!, 2, 0).Should().Be("User-vip");
        TestBatchBuilder.GetVal(res!, 4, 0).Should().Be($"Hi {f0}");
        TestBatchBuilder.GetVal(res!, 3, 0).Should().Be($"10:{f0}:Hi {f0}");

        TestBatchBuilder.GetVal(res!, 1, 1).Should().BeNull();
        TestBatchBuilder.GetVal(res!, 2, 1).Should().BeNull();
    }

    [Fact]
    public async Task Transform_ParallelBatchDeterminism_ShouldMatchAcrossRunsMixingNativeAndFallback()
    {
        var mappings = new[]
        {
            "first:name.firstName",
            "email:internet.email",
            "ua:internet.userAgent",
            "iban:finance.iban",
            "addr:address.fullAddress",
            "city:address.city",
            "num:random.number",
            "uuid:random.uuid",
            "amt:finance.amount",
            "created:date.past",
            "phrase:hacker.phrase",
            "path:system.filePath",
            "mime:system.mimeType"
        };

        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        var opts = new FakeOptions { Fake = mappings, SeedColumn = new[] { "id" }, Seed = 2026 };

        var t1 = new FakeDataTransformer(opts);
        var t2 = new FakeDataTransformer(opts);
        await t1.InitializeAsync(cols, TestContext.Current.CancellationToken);
        await t2.InitializeAsync(cols, TestContext.Current.CancellationToken);

        const int rows = 5000;
        var idBuilder = new Int32Array.Builder();
        idBuilder.Reserve(rows);
        for (int i = 0; i < rows; i++) idBuilder.Append(i);
        var idSchema = new Schema.Builder().Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int32Type.Default).Nullable(false)).Build();
        using var batch = new RecordBatch(idSchema, new IArrowArray[] { idBuilder.Build() }, rows);

        using var res1 = await t1.TransformBatchAsync(batch, TestContext.Current.CancellationToken);
        using var res2 = await t2.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        res1!.ColumnCount.Should().Be(1 + mappings.Length);
        for (int c = 1; c < res1.ColumnCount; c++)
        {
            for (int r = 0; r < rows; r += 317)
            {
                TestBatchBuilder.GetVal(res1, c, r).Should().Be(TestBatchBuilder.GetVal(res2!, c, r));
            }
        }
    }

    [Fact]
    public async Task Transform_NativeIbanCreditCardAndUserAgent_ShouldSatisfyFormatAndChecksums()
    {
        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[]
            {
                "iban:finance.iban",
                "cc:finance.creditCardNumber",
                "ua:internet.userAgent",
                "bic:finance.bic",
                "eth:finance.ethereumAddress",
                "btc:finance.bitcoinAddress"
            },
            SeedRow = true,
            Seed = 123
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
        var rowData = Enumerable.Range(0, 20).Select(i => new object?[] { i }).ToArray();
        using var batch = TestBatchBuilder.FromRows(cols, rowData);
        using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        for (int r = 0; r < 20; r++)
        {
            var iban = (string)TestBatchBuilder.GetVal(res!, 1, r)!;
            var rearranged = iban[4..] + iban[..4];
            int mod97 = 0;
            foreach (char ch in rearranged)
            {
                if (char.IsDigit(ch))
                    mod97 = (mod97 * 10 + (ch - '0')) % 97;
                else
                {
                    int val = ch - 'A' + 10;
                    mod97 = (mod97 * 100 + val) % 97;
                }
            }
            mod97.Should().Be(1, $"IBAN '{iban}' must pass ISO 13616 Mod-97");

            var cc = (string)TestBatchBuilder.GetVal(res!, 2, r)!;
            cc.Should().HaveLength(16);
            int luhnSum = 0;
            for (int i = 0; i < 16; i++)
            {
                int d = cc[i] - '0';
                if ((i & 1) == 0)
                {
                    d *= 2;
                    if (d > 9) d -= 9;
                }
                luhnSum += d;
            }
            (luhnSum % 10).Should().Be(0, $"CC '{cc}' must pass Luhn");

            var ua = (string)TestBatchBuilder.GetVal(res!, 3, r)!;
            ua.Should().StartWith("Mozilla/5.0 (");

            var bic = (string)TestBatchBuilder.GetVal(res!, 4, r)!;
            bic.Should().MatchRegex(@"^[A-Z]{6}[0-9A-Z]{2}$");

            var eth = (string)TestBatchBuilder.GetVal(res!, 5, r)!;
            eth.Should().MatchRegex(@"^0x[0-9a-f]{40}$");

            var btc = (string)TestBatchBuilder.GetVal(res!, 6, r)!;
            btc.Should().MatchRegex(@"^1[1-9A-HJ-NP-Za-km-z]{33}$");
        }
    }

    [Theory]
    [InlineData("en")]
    [InlineData("fr")]
    [InlineData("ja")]
    public async Task Transform_FullAddress_ShouldProduceRealisticNonEmptyOutput(string locale)
    {
        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        var t = new FakeDataTransformer(new FakeOptions
        {
            Fake = new[] { "addr:address.fullAddress" },
            Locale = locale,
            SeedRow = true,
            Seed = 99
        });

        await t.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var batch = TestBatchBuilder.FromRows(cols, new object?[] { 1 }, new object?[] { 2 });
        using var res = await t.TransformBatchAsync(batch, TestContext.Current.CancellationToken);

        for (int r = 0; r < 2; r++)
        {
            var addr = (string)TestBatchBuilder.GetVal(res!, 1, r)!;
            addr.Should().NotBeNullOrWhiteSpace().And.NotContain("#{");
        }
    }

    [Fact]
    public async Task Transform_DeterministicModesWithoutGlobalSeed_ShouldBeReproducibleAcrossInstances()
    {
        var cols = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        using var batch = TestBatchBuilder.FromRows(cols, new object?[] { 42 }, new object?[] { 99 });

        var col1 = new FakeDataTransformer(new FakeOptions { Fake = new[] { "email:internet.email" }, SeedColumn = new[] { "id" } });
        var col2 = new FakeDataTransformer(new FakeOptions { Fake = new[] { "email:internet.email" }, SeedColumn = new[] { "id" } });
        await col1.InitializeAsync(cols, TestContext.Current.CancellationToken);
        await col2.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var colRes1 = await col1.TransformBatchAsync(batch, TestContext.Current.CancellationToken);
        using var colRes2 = await col2.TransformBatchAsync(batch, TestContext.Current.CancellationToken);
        TestBatchBuilder.GetVal(colRes1!, 1, 0).Should().Be(TestBatchBuilder.GetVal(colRes2!, 1, 0));
        TestBatchBuilder.GetVal(colRes1!, 1, 1).Should().Be(TestBatchBuilder.GetVal(colRes2!, 1, 1));

        var row1 = new FakeDataTransformer(new FakeOptions { Fake = new[] { "email:internet.email" }, SeedRow = true });
        var row2 = new FakeDataTransformer(new FakeOptions { Fake = new[] { "email:internet.email" }, SeedRow = true });
        await row1.InitializeAsync(cols, TestContext.Current.CancellationToken);
        await row2.InitializeAsync(cols, TestContext.Current.CancellationToken);
        using var rowRes1 = await row1.TransformBatchAsync(batch, TestContext.Current.CancellationToken);
        using var rowRes2 = await row2.TransformBatchAsync(batch, TestContext.Current.CancellationToken);
        TestBatchBuilder.GetVal(rowRes1!, 1, 0).Should().Be(TestBatchBuilder.GetVal(rowRes2!, 1, 0));
        TestBatchBuilder.GetVal(rowRes1!, 1, 1).Should().Be(TestBatchBuilder.GetVal(rowRes2!, 1, 1));
    }
}
