namespace QueryDump.Core.Options;

using QueryDump.Writers;

/// <summary>
/// Service that discovers option types from registered factories.
/// </summary>
public interface IOptionTypeProvider
{
    /// <summary>
    /// Returns all option types discovered from registered factories.
    /// </summary>
    IEnumerable<Type> GetAllOptionTypes();
}

/// <summary>
/// Aggregates option types from registered factories.
/// </summary>
public class OptionTypeProvider : IOptionTypeProvider
{
    private readonly IDataSourceFactory _dataSourceFactory;
    private readonly IDataWriterFactory _dataWriterFactory;
    private readonly Transformers.Fake.IFakeDataTransformerFactory _fakeDataTransformerFactory;

    public OptionTypeProvider(
        IDataSourceFactory dataSourceFactory,
        IDataWriterFactory dataWriterFactory,
        Transformers.Fake.IFakeDataTransformerFactory fakeDataTransformerFactory)
    {
        _dataSourceFactory = dataSourceFactory;
        _dataWriterFactory = dataWriterFactory;
        _fakeDataTransformerFactory = fakeDataTransformerFactory;
    }

    public IEnumerable<Type> GetAllOptionTypes()
    {
        var types = new List<Type>();
        
        types.AddRange(_dataSourceFactory.GetSupportedOptionTypes());
        types.AddRange(_dataWriterFactory.GetSupportedOptionTypes());
        types.AddRange(_fakeDataTransformerFactory.GetSupportedOptionTypes());
        
        return types.Distinct();
    }
}
