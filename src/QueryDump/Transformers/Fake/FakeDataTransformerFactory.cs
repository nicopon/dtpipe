using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Fake;

/// <summary>
/// Factory for creating fake data transformers.
/// </summary>
public interface IFakeDataTransformerFactory
{
    /// <summary>
    /// Creates a transformer based on the provided options.
    /// </summary>
    IDataTransformer? Create(DumpOptions options);
    
    IEnumerable<Type> GetSupportedOptionTypes();
}

public class FakeDataTransformerFactory : IFakeDataTransformerFactory
{
    private readonly OptionsRegistry _registry;

    public FakeDataTransformerFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<FakeDataTransformer>();
    }

    public IDataTransformer? Create(DumpOptions options)
    {
        var fakeOptions = _registry.Get<FakeOptions>();

        if (fakeOptions.Mappings.Count == 0)
        {
            return null;
        }

        return new FakeDataTransformer(fakeOptions);
    }
}
