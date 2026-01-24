using Xunit;

namespace QueryDump.Tests;

/// <summary>
/// Collection definition for Docker-based integration tests.
/// Tests in this collection run sequentially to avoid Docker resource conflicts.
/// </summary>
[CollectionDefinition("Docker Integration Tests", DisableParallelization = true)]
public class DockerIntegrationTestsCollection
{
    // This class has no code, and is never created. Its purpose is simply
    // to be the place to apply [CollectionDefinition] and all the
    // ICollectionFixture<> interfaces.
}
