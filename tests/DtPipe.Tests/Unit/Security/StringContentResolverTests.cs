using DtPipe.Cli.Security;
using DtPipe.Core.Security;
using Xunit;

namespace DtPipe.Tests.Unit.Security;

public class DefaultStringContentResolverTests : IAsyncLifetime
{
    private string? _tempFile;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        if (_tempFile != null && File.Exists(_tempFile))
            File.Delete(_tempFile);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task PlainString_Passthrough()
    {
        var result = await DefaultStringContentResolver.Instance.ResolveAsync("SELECT 1");
        Assert.Equal("SELECT 1", result);
    }

    [Fact]
    public async Task Null_ReturnsNull()
    {
        var result = await DefaultStringContentResolver.Instance.ResolveAsync(null);
        Assert.Null(result);
    }

    [Fact]
    public async Task AtFile_LoadsContent()
    {
        _tempFile = Path.GetTempFileName();
        await File.WriteAllTextAsync(_tempFile, "SELECT 42");
        var result = await DefaultStringContentResolver.Instance.ResolveAsync($"@{_tempFile}");
        Assert.Equal("SELECT 42", result);
    }

    [Fact]
    public async Task EnvVar_Substitutes()
    {
        Environment.SetEnvironmentVariable("DTPIPE_TEST_REGION", "eu-west-1");
        try
        {
            var result = await DefaultStringContentResolver.Instance.ResolveAsync("SET region='${{DTPIPE_TEST_REGION}}'");
            Assert.Equal("SET region='eu-west-1'", result);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_TEST_REGION", null);
        }
    }

    [Fact]
    public async Task EnvVar_Missing_KeepsLiteral()
    {
        Environment.SetEnvironmentVariable("DTPIPE_TEST_MISSING_VAR", null);
        var result = await DefaultStringContentResolver.Instance.ResolveAsync("SET k='${{DTPIPE_TEST_MISSING_VAR}}'");
        Assert.Equal("SET k='${{DTPIPE_TEST_MISSING_VAR}}'", result);
    }

    [Fact]
    public async Task KeyringPrefix_PassesThrough()
    {
        // Core resolver has no SecretsManager — keyring:// standalone is returned as-is
        var result = await DefaultStringContentResolver.Instance.ResolveAsync("keyring://my-secret");
        Assert.Equal("keyring://my-secret", result);
    }

    [Fact]
    public async Task KeyringInlineInterpolation_PassesThrough()
    {
        // ${{keyring://...}} is not substituted by DefaultStringContentResolver
        var result = await DefaultStringContentResolver.Instance.ResolveAsync("SET k='${{keyring://my-key}}'");
        Assert.Equal("SET k='${{keyring://my-key}}'", result);
    }

    [Fact]
    public async Task AtFile_WithEnvVar_InContent_BothResolved()
    {
        _tempFile = Path.GetTempFileName();
        await File.WriteAllTextAsync(_tempFile, "SET region='${{DTPIPE_TEST_FILE_REGION}}'");
        Environment.SetEnvironmentVariable("DTPIPE_TEST_FILE_REGION", "us-east-1");
        try
        {
            var result = await DefaultStringContentResolver.Instance.ResolveAsync($"@{_tempFile}");
            Assert.Equal("SET region='us-east-1'", result);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_TEST_FILE_REGION", null);
        }
    }
}

public class CliStringContentResolverTests : IAsyncLifetime
{
    private readonly CliStringContentResolver _resolver = new();
    private string? _tempFile;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        if (_tempFile != null && File.Exists(_tempFile))
            File.Delete(_tempFile);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task PlainString_Passthrough()
    {
        var result = await _resolver.ResolveAsync("SELECT 1");
        Assert.Equal("SELECT 1", result);
    }

    [Fact]
    public async Task AtFile_StillWorks()
    {
        _tempFile = Path.GetTempFileName();
        await File.WriteAllTextAsync(_tempFile, "LOAD json");
        var result = await _resolver.ResolveAsync($"@{_tempFile}");
        Assert.Equal("LOAD json", result);
    }

    [Fact]
    public async Task EnvVar_Substitutes()
    {
        Environment.SetEnvironmentVariable("DTPIPE_CLI_TEST_REGION", "ap-southeast-1");
        try
        {
            var result = await _resolver.ResolveAsync("SET r='${{DTPIPE_CLI_TEST_REGION}}'");
            Assert.Equal("SET r='ap-southeast-1'", result);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_CLI_TEST_REGION", null);
        }
    }

    [Fact]
    public async Task EnvVar_Missing_KeepsLiteral()
    {
        Environment.SetEnvironmentVariable("DTPIPE_CLI_TEST_MISSING", null);
        var result = await _resolver.ResolveAsync("SET k='${{DTPIPE_CLI_TEST_MISSING}}'");
        Assert.Equal("SET k='${{DTPIPE_CLI_TEST_MISSING}}'", result);
    }

    [Fact]
    public async Task KeyringStandalone_Resolves()
    {
        var mgr = new DtPipe.Cli.Security.SecretsManager();
        mgr.SetSecret("test-duck-init-standalone", "LOAD json;");
        try
        {
            var result = await _resolver.ResolveAsync("keyring://test-duck-init-standalone");
            Assert.Equal("LOAD json;", result);
        }
        finally
        {
            mgr.DeleteSecret("test-duck-init-standalone");
        }
    }

    [Fact]
    public async Task KeyringInline_SubstitutedInString()
    {
        var mgr = new DtPipe.Cli.Security.SecretsManager();
        mgr.SetSecret("test-duck-init-inline-key", "AKIAIOSFODNN7EXAMPLE");
        try
        {
            var result = await _resolver.ResolveAsync("SET s3_key='${{keyring://test-duck-init-inline-key}}'");
            Assert.Equal("SET s3_key='AKIAIOSFODNN7EXAMPLE'", result);
        }
        finally
        {
            mgr.DeleteSecret("test-duck-init-inline-key");
        }
    }

    [Fact]
    public async Task KeyringAndEnvVar_BothResolved()
    {
        var mgr = new DtPipe.Cli.Security.SecretsManager();
        mgr.SetSecret("test-duck-init-region-key", "eu-west-1");
        Environment.SetEnvironmentVariable("DTPIPE_CLI_TEST_ACCESS_KEY", "MY_KEY");
        try
        {
            var input = "LOAD httpfs; SET region='${{keyring://test-duck-init-region-key}}'; SET key='${{DTPIPE_CLI_TEST_ACCESS_KEY}}';";
            var result = await _resolver.ResolveAsync(input);
            Assert.Equal("LOAD httpfs; SET region='eu-west-1'; SET key='MY_KEY';", result);
        }
        finally
        {
            mgr.DeleteSecret("test-duck-init-region-key");
            Environment.SetEnvironmentVariable("DTPIPE_CLI_TEST_ACCESS_KEY", null);
        }
    }

    [Fact]
    public async Task KeyringValue_WithEnvVar_BothResolved()
    {
        // Secret stored in keyring itself contains a ${{VAR}} placeholder
        var mgr = new DtPipe.Cli.Security.SecretsManager();
        mgr.SetSecret("test-duck-init-template", "LOAD httpfs; SET region='${{DTPIPE_CLI_REGION_FROM_KEYRING}}';");
        Environment.SetEnvironmentVariable("DTPIPE_CLI_REGION_FROM_KEYRING", "us-east-1");
        try
        {
            var result = await _resolver.ResolveAsync("keyring://test-duck-init-template");
            Assert.Equal("LOAD httpfs; SET region='us-east-1';", result);
        }
        finally
        {
            mgr.DeleteSecret("test-duck-init-template");
            Environment.SetEnvironmentVariable("DTPIPE_CLI_REGION_FROM_KEYRING", null);
        }
    }
}
