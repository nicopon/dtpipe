using System.Reflection;
using Bogus;

namespace QueryDump.Transformers.Fake;

/// <summary>
/// Registry for discovering and invoking Bogus fakers via reflection.
/// Provides dynamic lookup of all available Dataset.Method combinations.
/// </summary>
public sealed class FakerRegistry
{
    private readonly Dictionary<string, Func<Faker, object?>> _generators = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string> _descriptions = new(StringComparer.OrdinalIgnoreCase);

    public FakerRegistry()
    {
        RegisterBuiltInGenerators();
    }

    /// <summary>
    /// Get a generator function for the given path (e.g., "address.city").
    /// </summary>
    public Func<Faker, object?>? GetGenerator(string path)
    {
        return _generators.TryGetValue(path, out var generator) ? generator : null;
    }

    /// <summary>
    /// Check if a generator exists for the given path.
    /// </summary>
    public bool HasGenerator(string path) => _generators.ContainsKey(path);

    /// <summary>
    /// Get all available generators grouped by dataset.
    /// </summary>
    public IEnumerable<(string Dataset, IEnumerable<(string Method, string Description)> Methods)> ListAll()
    {
        return _generators.Keys
            .Select(k => k.Split('.'))
            .Where(parts => parts.Length == 2)
            .GroupBy(parts => parts[0], StringComparer.OrdinalIgnoreCase)
            .OrderBy(g => g.Key)
            .Select(g => (
                Dataset: g.Key,
                Methods: (IEnumerable<(string Method, string Description)>)g.Select(parts => (
                    Method: parts[1],
                    Description: _descriptions.TryGetValue($"{parts[0]}.{parts[1]}", out var desc) ? desc : ""
                )).OrderBy(m => m.Method)
            ));
    }

    private void RegisterBuiltInGenerators()
    {
        // Address
        Register("address.city", f => f.Address.City(), "Get a city name");
        Register("address.streetaddress", f => f.Address.StreetAddress(), "Get a street address");
        Register("address.zipcode", f => f.Address.ZipCode(), "Get a zipcode");
        Register("address.country", f => f.Address.Country(), "Get a country");
        Register("address.countrycode", f => f.Address.CountryCode(), "Get a country code (ISO 3166-1)");
        Register("address.state", f => f.Address.State(), "Get a state");
        Register("address.stateabbr", f => f.Address.StateAbbr(), "Get a state abbreviation");
        Register("address.fulladdress", f => f.Address.FullAddress(), "Get a full address");
        Register("address.streetname", f => f.Address.StreetName(), "Get a street name");
        Register("address.buildingnumber", f => f.Address.BuildingNumber(), "Get a building number");
        Register("address.latitude", f => f.Address.Latitude(), "Get a latitude");
        Register("address.longitude", f => f.Address.Longitude(), "Get a longitude");

        // Name
        Register("name.firstname", f => f.Name.FirstName(), "Get a first name");
        Register("name.lastname", f => f.Name.LastName(), "Get a last name");
        Register("name.fullname", f => f.Name.FullName(), "Get a full name");
        Register("name.prefix", f => f.Name.Prefix(), "Get a name prefix (Mr., Mrs., etc.)");
        Register("name.suffix", f => f.Name.Suffix(), "Get a name suffix (Jr., Sr., etc.)");
        Register("name.jobtitle", f => f.Name.JobTitle(), "Get a job title");
        Register("name.jobdescriptor", f => f.Name.JobDescriptor(), "Get a job descriptor");
        Register("name.jobarea", f => f.Name.JobArea(), "Get a job area");
        Register("name.jobtype", f => f.Name.JobType(), "Get a job type");

        // Internet
        Register("internet.email", f => f.Internet.Email(), "Generate an email address");
        Register("internet.exampleemail", f => f.Internet.ExampleEmail(), "Generate an example email (@example.com)");
        Register("internet.username", f => f.Internet.UserName(), "Generate a username");
        Register("internet.domainname", f => f.Internet.DomainName(), "Generate a domain name");
        Register("internet.url", f => f.Internet.Url(), "Generate a URL");
        Register("internet.ip", f => f.Internet.Ip(), "Generate an IPv4 address");
        Register("internet.ipv6", f => f.Internet.Ipv6(), "Generate an IPv6 address");
        Register("internet.mac", f => f.Internet.Mac(), "Generate a MAC address");
        Register("internet.password", f => f.Internet.Password(), "Generate a password");
        Register("internet.useragent", f => f.Internet.UserAgent(), "Generate a user agent string");

        // Phone
        Register("phone.phonenumber", f => f.Phone.PhoneNumber(), "Generate a phone number");

        // Company
        Register("company.companyname", f => f.Company.CompanyName(), "Get a company name");
        Register("company.catchphrase", f => f.Company.CatchPhrase(), "Get a company catch phrase");
        Register("company.bs", f => f.Company.Bs(), "Get a company BS phrase");
        Register("company.companysuffix", f => f.Company.CompanySuffix(), "Get a company suffix (Inc, LLC)");

        // Commerce
        Register("commerce.productname", f => f.Commerce.ProductName(), "Get a product name");
        Register("commerce.price", f => f.Commerce.Price(), "Get a product price");
        Register("commerce.department", f => f.Commerce.Department(), "Get a department name");
        Register("commerce.product", f => f.Commerce.Product(), "Get a product");
        Register("commerce.color", f => f.Commerce.Color(), "Get a color");
        Register("commerce.ean8", f => f.Commerce.Ean8(), "Get an EAN-8 barcode");
        Register("commerce.ean13", f => f.Commerce.Ean13(), "Get an EAN-13 barcode");

        // Finance
        Register("finance.account", f => f.Finance.Account(), "Get an account number");
        Register("finance.accountname", f => f.Finance.AccountName(), "Get an account name");
        Register("finance.amount", f => f.Finance.Amount(), "Get a random amount");
        Register("finance.iban", f => f.Finance.Iban(), "Get an IBAN");
        Register("finance.bic", f => f.Finance.Bic(), "Get a BIC code");
        Register("finance.bitcoinaddress", f => f.Finance.BitcoinAddress(), "Get a Bitcoin address");
        Register("finance.ethereumaddress", f => f.Finance.EthereumAddress(), "Get an Ethereum address");
        Register("finance.creditcardnumber", f => f.Finance.CreditCardNumber(), "Get a credit card number");
        Register("finance.creditcardcvv", f => f.Finance.CreditCardCvv(), "Get a credit card CVV");

        // Lorem
        Register("lorem.word", f => f.Lorem.Word(), "Get a random word");
        Register("lorem.words", f => string.Join(" ", f.Lorem.Words()), "Get random words");
        Register("lorem.sentence", f => f.Lorem.Sentence(), "Get a random sentence");
        Register("lorem.sentences", f => f.Lorem.Sentences(), "Get random sentences");
        Register("lorem.paragraph", f => f.Lorem.Paragraph(), "Get a paragraph");
        Register("lorem.paragraphs", f => f.Lorem.Paragraphs(), "Get multiple paragraphs");
        Register("lorem.text", f => f.Lorem.Text(), "Get random text");
        Register("lorem.slug", f => f.Lorem.Slug(), "Get a URL slug");

        // Date
        Register("date.past", f => f.Date.Past(), "Get a past date");
        Register("date.future", f => f.Date.Future(), "Get a future date");
        Register("date.recent", f => f.Date.Recent(), "Get a recent date");
        Register("date.soon", f => f.Date.Soon(), "Get an upcoming date");
        Register("date.month", f => f.Date.Month(), "Get a month name");
        Register("date.weekday", f => f.Date.Weekday(), "Get a weekday name");

        // Random
        Register("random.number", f => f.Random.Number(0, 1000000), "Get a random number");
        Register("random.guid", f => f.Random.Guid(), "Get a random GUID");
        Register("random.uuid", f => f.Random.Uuid(), "Get a random UUID");
        Register("random.bool", f => f.Random.Bool(), "Get a random boolean");
        Register("random.word", f => f.Random.Word(), "Get a random word");
        Register("random.words", f => f.Random.Words(), "Get random words");
        Register("random.hash", f => f.Random.Hash(), "Get a random hash");
        Register("random.alphanumeric", f => f.Random.AlphaNumeric(10), "Get alphanumeric string");

        // Vehicle
        Register("vehicle.vin", f => f.Vehicle.Vin(), "Get a vehicle VIN");
        Register("vehicle.manufacturer", f => f.Vehicle.Manufacturer(), "Get a vehicle manufacturer");
        Register("vehicle.model", f => f.Vehicle.Model(), "Get a vehicle model");
        Register("vehicle.type", f => f.Vehicle.Type(), "Get a vehicle type");
        Register("vehicle.fuel", f => f.Vehicle.Fuel(), "Get a fuel type");

        // Hacker
        Register("hacker.abbreviation", f => f.Hacker.Abbreviation(), "Get a hacker abbreviation");
        Register("hacker.adjective", f => f.Hacker.Adjective(), "Get a hacker adjective");
        Register("hacker.noun", f => f.Hacker.Noun(), "Get a hacker noun");
        Register("hacker.verb", f => f.Hacker.Verb(), "Get a hacker verb");
        Register("hacker.phrase", f => f.Hacker.Phrase(), "Get a hacker phrase");

        // System
        Register("system.filename", f => f.System.FileName(), "Get a file name");
        Register("system.directorypath", f => f.System.DirectoryPath(), "Get a directory path");
        Register("system.filepath", f => f.System.FilePath(), "Get a file path");
        Register("system.mimetype", f => f.System.MimeType(), "Get a MIME type");
        Register("system.fileext", f => f.System.FileExt(), "Get a file extension");
        Register("system.semver", f => f.System.Semver(), "Get a semver version");

        // Database
        Register("database.column", f => f.Database.Column(), "Get a database column name");
        Register("database.type", f => f.Database.Type(), "Get a database column type");
        Register("database.collation", f => f.Database.Collation(), "Get a database collation");
        Register("database.engine", f => f.Database.Engine(), "Get a database engine");
    }

    private void Register(string path, Func<Faker, object?> generator, string description)
    {
        _generators[path] = generator;
        _descriptions[path] = description;
    }
}
