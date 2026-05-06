namespace DtPipe.Core.Options;

/// <summary>
/// Implemented by DB writer options that support schema compatibility enforcement.
/// </summary>
public interface ISchemaValidationAware
{
    bool StrictSchema { get; set; }
    bool NoSchemaValidation { get; set; }
    bool AutoMigrate { get; set; }
}
