using System.Data;
using System.Text;

namespace BulkCopy;

/// <summary>
///     Basic streaming CSV parser.
/// </summary>
/// <remarks>
///     This parser expects comma-separated values, with optional quotes (").<br />
///     The file must be UTF-8 encoded.<br />
///     By default, "␀" is treated as a null value. This can be overridden by passing a different null character.
/// </remarks>
public static class CsvParser
{
    /// <summary>
    ///     Reads the entire CSV file into a DataTable. Must have a header row.
    /// </summary>
    /// <param name="filePath">Path to the CSV file to be parsed.</param>
    /// <param name="nullChar">Character to treat as null. Defaults to "␀" (null character).</param>
    /// <returns>A DataTable containing the parsed CSV data.</returns>
    /// <exception cref="FormatException">Thrown if the CSV stream is empty or has no header row.</exception>
    public static DataTable LoadCsvToDataTable(string filePath, string? nullChar = "␀")
    {
        using var reader = new StreamReader(filePath, Encoding.UTF8);
        return LoadCsvFromStream(reader, nullChar);
    }

    /// <summary>
    ///     Reads the CSV data from a StreamReader into a DataTable. Must have a header row.
    /// </summary>
    /// <param name="reader">Stream to read CSV data from.</param>
    /// <param name="nullChar">Character to treat as null. Defaults to "␀" (null character).</param>
    /// <returns>A DataTable containing the parsed CSV data.</returns>
    /// <exception cref="FormatException">Thrown if the CSV stream is empty or has no header row.</exception>
    public static DataTable LoadCsvFromStream(StreamReader reader, string? nullChar = "␀")
    {
        var dataTable = new DataTable();

        var firstRow = ReadCsvRow(reader);
        if (string.IsNullOrEmpty(firstRow))
        {
            throw new FormatException("CSV is empty or has no header row.");
        }

        var headers = ParseCsvLine(firstRow, nullChar);
        foreach (var header in headers)
        {
            dataTable.Columns.Add(header?.Trim() ?? "");
        }

        while (ReadCsvRow(reader) is { } row)
        {
            if (string.IsNullOrWhiteSpace(row))
            {
                continue;
            }

            var fields = ParseCsvLine(row, nullChar);

            // Handle rows with fewer fields than headers
            if (fields.Length < headers.Length)
            {
                var originalLength = fields.Length;
                Array.Resize(ref fields, headers.Length);
                for (var i = originalLength; i < headers.Length; i++)
                {
                    fields[i] = string.Empty;
                }
            }

            // ReSharper disable once CoVariantArrayConversion
            dataTable.Rows.Add(fields);
        }

        return dataTable;
    }

    /// <summary>
    ///     Reads a single CSV row from a StreamReader.
    /// </summary>
    /// <remarks>
    ///     This method reads the stream until an unquoted newline is encountered.
    /// </remarks>
    /// <param name="reader">Stream to read CSV data from.</param>
    /// <returns>The parsed CSV row as a string, or null if end of stream is reached.</returns>
    public static string? ReadCsvRow(StreamReader reader)
    {
        var row = new StringBuilder();
        var inQuotes = false;
        int currentChar;

        while ((currentChar = reader.Read()) != -1)
        {
            var c = (char)currentChar;

            switch (c)
            {
                case '"':
                {
                    row.Append(c);
                    // If it's escaped, this gets toggled back
                    inQuotes = !inQuotes;

                    break;
                }
                case '\n' when !inQuotes:
                {
                    // Skip trailing \r if present
                    var nextChar = reader.Peek();
                    if (nextChar == '\r')
                    {
                        reader.Read();
                    }

                    return row.Length > 0 ? row.ToString() : null;
                }
                case '\r' when !inQuotes:
                {
                    // Skip trailing \n if present
                    var nextChar = reader.Peek();
                    if (nextChar == '\n')
                    {
                        reader.Read();
                    }

                    return row.Length > 0 ? row.ToString() : null;
                }
                default:
                    row.Append(c);
                    break;
            }
        }

        // Return the last row if any content remains
        return row.Length > 0 ? row.ToString() : null;
    }

    /// <summary>
    ///     Parses a CSV line into an array of fields.
    /// </summary>
    /// <param name="line">The CSV line to parse.</param>
    /// <param name="nullChar">The character used to represent null values in the CSV line. Defaults to "␀" (null character).</param>
    /// <returns>An array of fields extracted from the CSV line.</returns>
    public static string?[] ParseCsvLine(string line, string? nullChar = "␀")
    {
        var fields = new List<string?>();
        var inQuotes = false;
        var fieldStart = 0;

        for (var i = 0; i < line.Length; i++)
        {
            switch (line[i])
            {
                case '"':
                    // If it's escaped, this gets toggled back
                    inQuotes = !inQuotes;
                    break;
                case ',' when !inQuotes:
                    fields.Add(ExtractField(line, fieldStart, i, nullChar));
                    fieldStart = i + 1;
                    break;
            }
        }

        // Add the last field
        fields.Add(ExtractField(line, fieldStart, line.Length, nullChar));

        return fields.ToArray();
    }

    /// <summary>
    ///     Extracts a field from a CSV line.
    /// </summary>
    /// <param name="line">The CSV line containing the field.</param>
    /// <param name="start">The starting index of the field within the line.</param>
    /// <param name="end">The ending index of the field within the line.</param>
    /// <param name="nullChar">The character used to represent null values in the CSV line. Defaults to "␀" (null character).</param>
    /// <returns>The extracted field as a string, or null if the field represents a null value.</returns>
    public static string? ExtractField(string line, int start, int end, string? nullChar = "␀")
    {
        var field = line.Substring(start, end - start).Trim();

        var isQuoted = field.StartsWith('"') && field.EndsWith('"') && field.Length >= 2;

        if (isQuoted)
        {
            field = field.Substring(1, field.Length - 2);
            // Unescape doubled quotes
            field = field.Replace("\"\"", "\"");
        }

        if (nullChar != null && field == nullChar)
        {
            return null;
        }

        return field;
    }
}