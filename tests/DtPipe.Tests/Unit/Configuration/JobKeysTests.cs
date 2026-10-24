using AwesomeAssertions;
using DtPipe.Core.Models;
using DtPipe.Jobs;
using Xunit;
using YamlDotNet.Serialization.NamingConventions;

namespace DtPipe.Tests.Unit.Configuration;

public class JobKeysTests
{
    // The CLI's model derives its key names from its property names; the family names the same keys
    // through JobKeys. A rename on either side that leaves the other behind fails here.
    [Theory]
    [InlineData(nameof(JobDefinition.Input), JobKeys.Input)]
    [InlineData(nameof(JobDefinition.Output), JobKeys.Output)]
    [InlineData(nameof(JobDefinition.From), JobKeys.From)]
    [InlineData(nameof(JobDefinition.Ref), JobKeys.Ref)]
    [InlineData(nameof(JobDefinition.ProviderOptions), JobKeys.ProviderOptions)]
    public void AKeyOfTheFamily_IsTheNameTheJobModelSerializesUnder(string property, string key)
    {
        typeof(JobDefinition).GetProperty(property).Should().NotBeNull();
        HyphenatedNamingConvention.Instance.Apply(property).Should().Be(key);
    }
}
