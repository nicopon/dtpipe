using DtPipe.Configuration;
using Xunit;

namespace DtPipe.Tests.Unit.Configuration;

/// <summary>
/// A key written twice in one mapping used to keep the last and drop the first, so a branch with
/// two <c>provider-options:</c> blocks lost its processor and still exited 0.
/// </summary>
public class YamlDuplicateKeyGuardTests
{
    [Fact]
    public void Two_ProviderOptions_Blocks_In_One_Branch_Are_Refused_With_The_Key_And_Both_Lines()
    {
        var yaml = """
            a:
              input: csv:a.csv
            main:
              from: a
              provider-options:
                sql:
                  query: SELECT id FROM a
              provider-options:
                csv-writer:
                  header: true
              output: csv:-
            """;

        var fault = Assert.Throws<InvalidOperationException>(() => JobFileParser.ParseContent(yaml));

        Assert.Contains("'provider-options' appears twice", fault.Message);
        Assert.Contains("first at line 5", fault.Message);
        Assert.Contains("line 8", fault.Message);
    }

    [Fact]
    public void A_Branch_Name_Written_Twice_Is_Refused()
    {
        var yaml = """
            main:
              input: csv:a.csv
            main:
              input: csv:b.csv
            """;

        var fault = Assert.Throws<InvalidOperationException>(() => JobFileParser.ParseContent(yaml));

        Assert.Contains("'main' appears twice", fault.Message);
    }

    [Fact]
    public void The_Same_Key_In_Different_Mappings_Is_Not_A_Duplicate()
    {
        var yaml = """
            a:
              input: csv:a.csv
              provider-options:
                csv-reader:
                  header: true
            b:
              input: csv:b.csv
              provider-options:
                csv-reader:
                  header: true
            """;

        var jobs = JobFileParser.ParseContent(yaml);

        Assert.Equal(2, jobs.Count);
    }
}
