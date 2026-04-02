using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow.Serialization.Tests;

public enum LogLevel { Debug, Info, Warning, Error, Critical }
public enum OrderStatus { Pending, Processing, Shipped, Delivered, Cancelled }
public enum Currency { USD, EUR, GBP, JPY, CAD }

internal static class EqualityHelper
{
    public static bool DictionaryEqual<K, V>(IDictionary<K, V>? d1, IDictionary<K, V>? d2)
    {
        if (ReferenceEquals(d1, d2)) return true;
        if (d1 == null || d2 == null) return false;
        if (d1.Count != d2.Count) return false;
        foreach (var pair in d1)
        {
            if (!d2.TryGetValue(pair.Key, out var value) || !EqualityComparer<V>.Default.Equals(pair.Value, value))
                return false;
        }
        return true;
    }

    public static bool ListEqual<T>(IList<T>? l1, IList<T>? l2)
    {
        if (ReferenceEquals(l1, l2)) return true;
        if (l1 == null || l2 == null) return false;
        return l1.SequenceEqual(l2);
    }
    
    public static int GetDictionaryHashCode<K, V>(IDictionary<K, V>? dict)
    {
        if (dict == null) return 0;
        int hash = 17;
        foreach (var pair in dict.OrderBy(p => p.Key))
        {
            hash = hash * 31 + (pair.Key?.GetHashCode() ?? 0);
            hash = hash * 31 + (pair.Value?.GetHashCode() ?? 0);
        }
        return hash;
    }

    public static int GetEnumerableHashCode<T>(IEnumerable<T>? en)
    {
        if (en == null) return 0;
        int hash = 17;
        foreach (var item in en)
        {
            hash = hash * 31 + (item?.GetHashCode() ?? 0);
        }
        return hash;
    }
}

public class SensorMetadata : IEquatable<SensorMetadata>
{
    public string SerialNumber { get; set; } = string.Empty;
    public string FirmwareVersion { get; set; } = string.Empty;
    public int CalibrationYear { get; set; }

    public bool Equals(SensorMetadata? other)
    {
        if (other is null) return false;
        return SerialNumber == other.SerialNumber && FirmwareVersion == other.FirmwareVersion && CalibrationYear == other.CalibrationYear;
    }
    public override bool Equals(object? obj) => Equals(obj as SensorMetadata);
    public override int GetHashCode() => HashCode.Combine(SerialNumber, FirmwareVersion, CalibrationYear);
}

public class IoTTelemetry : IEquatable<IoTTelemetry>
{
    public Guid DeviceId { get; set; }
    public string DeviceName { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public double[] Readings { get; set; } = System.Array.Empty<double>();
    public Dictionary<string, string> Metadata { get; set; } = new();
    public SensorMetadata SensorInfo { get; set; } = new();

    public bool Equals(IoTTelemetry? other)
    {
        if (other is null) return false;
        return DeviceId == other.DeviceId && 
               DeviceName == other.DeviceName && 
               Timestamp.Equals(other.Timestamp) && 
               Readings.SequenceEqual(other.Readings) && 
               EqualityHelper.DictionaryEqual(Metadata, other.Metadata) && 
               SensorInfo.Equals(other.SensorInfo);
    }
    public override bool Equals(object? obj) => Equals(obj as IoTTelemetry);
    public override int GetHashCode() => HashCode.Combine(DeviceId, DeviceName, Timestamp, EqualityHelper.GetEnumerableHashCode(Readings), EqualityHelper.GetDictionaryHashCode(Metadata), SensorInfo);
}

public class OrderItem : IEquatable<OrderItem>
{
    public string ProductSku { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }

    public bool Equals(OrderItem? other)
    {
        if (other is null) return false;
        return ProductSku == other.ProductSku && Quantity == other.Quantity && UnitPrice == other.UnitPrice;
    }
    public override bool Equals(object? obj) => Equals(obj as OrderItem);
    public override int GetHashCode() => HashCode.Combine(ProductSku, Quantity, UnitPrice);
}

public class ECommerceOrder : IEquatable<ECommerceOrder>
{
    public long OrderId { get; set; }
    public OrderStatus Status { get; set; }
    public List<OrderItem> Items { get; set; } = new();
    public decimal TotalAmount { get; set; }
    public Currency Currency { get; set; }
    public DateTimeOffset OrderedAt { get; set; }

    public bool Equals(ECommerceOrder? other)
    {
        if (other is null) return false;
        return OrderId == other.OrderId && 
               Status == other.Status && 
               Items.SequenceEqual(other.Items) && 
               TotalAmount == other.TotalAmount && 
               Currency == other.Currency && 
               OrderedAt.Equals(other.OrderedAt);
    }
    public override bool Equals(object? obj) => Equals(obj as ECommerceOrder);
    public override int GetHashCode() => HashCode.Combine(OrderId, Status, EqualityHelper.GetEnumerableHashCode(Items), TotalAmount, Currency, OrderedAt);
}

public class UserAccount : IEquatable<UserAccount>
{
    public Guid AccountId { get; set; }
    public string Email { get; set; } = string.Empty;
    public bool IsActive { get; set; }
    public HashSet<string> Roles { get; set; } = new();
    public DateTimeOffset CreatedAt { get; set; }
    public TimeSpan? SessionTimeout { get; set; }

    public bool Equals(UserAccount? other)
    {
        if (other is null) return false;
        return AccountId == other.AccountId && 
               Email == other.Email && 
               IsActive == other.IsActive && 
               Roles.SetEquals(other.Roles) && 
               CreatedAt.Equals(other.CreatedAt) && 
               Nullable.Equals(SessionTimeout, other.SessionTimeout);
    }
    public override bool Equals(object? obj) => Equals(obj as UserAccount);
    public override int GetHashCode() => HashCode.Combine(AccountId, Email, IsActive, EqualityHelper.GetEnumerableHashCode(Roles), CreatedAt, SessionTimeout);
}

public class LogEntry : IEquatable<LogEntry>
{
    public DateTime Timestamp { get; set; }
    public LogLevel Level { get; set; }
    public string Message { get; set; } = string.Empty;
    public Dictionary<string, string> Properties { get; set; } = new();

    public bool Equals(LogEntry? other)
    {
        if (other is null) return false;
        return Timestamp.Equals(other.Timestamp) && 
               Level == other.Level && 
               Message == other.Message && 
               EqualityHelper.DictionaryEqual(Properties, other.Properties);
    }
    public override bool Equals(object? obj) => Equals(obj as LogEntry);
    public override int GetHashCode() => HashCode.Combine(Timestamp, Level, Message, EqualityHelper.GetDictionaryHashCode(Properties));
}
