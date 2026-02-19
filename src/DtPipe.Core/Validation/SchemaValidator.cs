namespace DtPipe.Core.Validation;

using System.Globalization;
using DtPipe.Core.Models;

public record ValueValidationResult(
	bool IsNullViolation,
	bool IsLengthViolation,
	bool IsPrecisionViolation,
	int? ActualLength,
	int? ActualIntegerDigits,
	int? MaxIntegerDigits
)
{
	public bool HasAnyViolation => IsNullViolation || IsLengthViolation || IsPrecisionViolation;

	public static ValueValidationResult Success() => new(false, false, false, null, null, null);
}

public static class SchemaValidator
{
	public static ValueValidationResult Validate(object? value, TargetColumnInfo column)
	{
		bool isNullViolation = false;
		bool isLengthViolation = false;
		bool isPrecisionViolation = false;
		int? actualLength = null;
		int? actualIntegerDigits = null;
		int? maxIntegerDigits = null;

		// 1. Check Null Violation
		if (value is null)
		{
			if (!column.IsNullable && !column.IsPrimaryKey)
			{
				isNullViolation = true;
			}
			// If null, no other checks apply
			return new ValueValidationResult(isNullViolation, false, false, null, null, null);
		}

		// 2. Check Length Violation (for string types)
		// Restrict to String type to avoid false positives on binary-sized types like Oracle DATE.
		if (column.InferredClrType == typeof(string) && column.MaxLength.HasValue && column.MaxLength.Value > 0)
		{
			var strVal = value.ToString() ?? "";
			actualLength = strVal.Length;
			if (actualLength > column.MaxLength.Value)
			{
				isLengthViolation = true;
			}
		}

		// 3. Check Precision Violation (for numeric types)
		if (column.Precision.HasValue && IsNumericType(value.GetType()))
		{
			int precision = column.Precision.Value;
			int scale = column.Scale ?? 0;
			maxIntegerDigits = precision - scale;

			if (maxIntegerDigits > 0)
			{
				// Calculate integer digits
				var strVal = Convert.ToString(value, CultureInfo.InvariantCulture) ?? "";

				// Handle signs
				if (strVal.StartsWith("-") || strVal.StartsWith("+")) strVal = strVal.Substring(1);

				// Truncate at decimal point
				var decimalPointIdx = strVal.IndexOf('.');
				var integerPart = decimalPointIdx >= 0 ? strVal.Substring(0, decimalPointIdx) : strVal;

				actualIntegerDigits = integerPart.Length;

				if (actualIntegerDigits > maxIntegerDigits)
				{
					isPrecisionViolation = true;
				}
			}
		}

		return new ValueValidationResult(
			isNullViolation,
			isLengthViolation,
			isPrecisionViolation,
			actualLength,
			actualIntegerDigits,
			maxIntegerDigits
		);
	}

	private static bool IsNumericType(Type t)
	{
		t = Nullable.GetUnderlyingType(t) ?? t;
		return t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte) ||
			   t == typeof(ulong) || t == typeof(uint) || t == typeof(ushort) || t == typeof(sbyte) ||
			   t == typeof(float) || t == typeof(double) || t == typeof(decimal);
	}
}
