using System.Linq;
using DtPipe.Adapters.Csv;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Common;

/// <summary>
/// A RequiresQuery reader with no Query bound must fail loudly at CliStreamReaderFactory.Create()
/// instead of silently falling back to a default query. The check lives once in
/// CliStreamReaderFactory (keyed off IProviderDescriptor.RequiresQuery), not duplicated per provider.
/// Covers every RequiresQuery reader discovered by reflection, so a new one is covered automatically.
/// </summary>
public class SqlReaderDescriptorQueryValidationTests
{
    private static readonly IServiceProvider EmptyServiceProvider = new ServiceCollection().BuildServiceProvider();

    public static TheoryData<IProviderDescriptor<IStreamReader>, object> RequiresQueryDescriptors()
    {
        var descriptorInterface = typeof(IProviderDescriptor<IStreamReader>);
        var assembly = typeof(CsvReaderDescriptor).Assembly; // DtPipe.Adapters

        var descriptors = assembly.GetTypes()
            .Where(t => !t.IsAbstract && descriptorInterface.IsAssignableFrom(t) && t.GetConstructor(Type.EmptyTypes) != null)
            .Select(t => (IProviderDescriptor<IStreamReader>)Activator.CreateInstance(t)!)
            .Where(d => d.RequiresQuery);

        var data = new TheoryData<IProviderDescriptor<IStreamReader>, object>();
        foreach (var descriptor in descriptors)
            data.Add(descriptor, Activator.CreateInstance(descriptor.OptionsType)!);
        return data;
    }

    [Theory]
    [MemberData(nameof(RequiresQueryDescriptors))]
    public void Create_Throws_WhenRequiresQueryAndNoneWasBound(IProviderDescriptor<IStreamReader> descriptor, object options)
    {
        options.Should().BeAssignableTo<IQueryAwareOptions>(
            "a RequiresQuery reader's options must implement IQueryAwareOptions so --query/--table can bind");

        var registry = new OptionsRegistry();
        registry.RegisterByType(descriptor.OptionsType, options);
        registry.Register(new ConnectionRoute("dummy-connection", string.Empty));

        var factory = new CliStreamReaderFactory(descriptor, registry, EmptyServiceProvider);

        var act = () => factory.Create(registry);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage($"*{descriptor.ComponentName}*");
    }
}
