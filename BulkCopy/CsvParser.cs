using System.Data;
using System.Text;

namespace BulkCopy;

public class CsvParser
{
    public static DataTable LoadCsvToDataTable(string filePath, string? nullChar = "␀")
    {
        using var reader = new StreamReader(filePath, Encoding.UTF8);
        return LoadCsvFromStream(reader, nullChar);
    }

    public static DataTable LoadCsvFromStream(StreamReader reader, string? nullChar = "␀")
    {
        var dataTable = new DataTable();

        var firstRow = ReadCsvRow(reader);
        if (string.IsNullOrEmpty(firstRow))
        {
            throw new InvalidOperationException("CSV file is empty or has no header row.");
        }

        var headers = ParseCsvLine(firstRow, nullChar);
        foreach (var header in headers)
        {
            dataTable.Columns.Add(header?.Trim() ?? "");
        }

        // Read data rows
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

            dataTable.Rows.Add(fields);
        }

        return dataTable;
    }

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
                    // Check if this is an escaped quote (double quote)
                    if (inQuotes && reader.Peek() == '"')
                    {
                        // Escaped quote - read the next quote and don't toggle state
                        row.Append((char)reader.Read());
                    }
                    else
                    {
                        // Regular quote - toggle the inQuotes state
                        inQuotes = !inQuotes;
                    }

                    break;
                }
                case '\n' when !inQuotes:
                {
                    // End of row found (not inside quotes)
                    // Remove trailing \r if present
                    if (row.Length > 0 && row[^1] == '\r')
                    {
                        row.Length--;
                    }

                    return row.Length > 0 ? row.ToString() : null;
                }
                case '\r' when !inQuotes:
                {
                    // Check if next char is \n (Windows line ending)
                    var nextChar = reader.Peek();
                    if (nextChar == '\n')
                    {
                        reader.Read(); // Consume the \n
                    }

                    // Return row (handles both Windows \r\n and Mac \r line endings)
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

    public static string?[] ParseCsvLine(string line, string? nullChar = "␀")
    {
        var fields = new List<string?>();
        var inQuotes = false;
        var fieldStart = 0;

        for (var i = 0; i < line.Length; i++)
        {
            switch (line[i])
            {
                case '"' when inQuotes && i + 1 < line.Length && line[i + 1] == '"':
                    i++; // Escaped quote inside quoted field
                    break;
                case '"':
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

    public static string? ExtractField(string line, int start, int end, string? nullChar = "␀")
    {
        var field = line.Substring(start, end - start).Trim();

        // Check if the field is quoted
        var isQuoted = field.StartsWith('"') && field.EndsWith('"') && field.Length >= 2;

        // Remove surrounding quotes if present
        if (isQuoted)
        {
            field = field.Substring(1, field.Length - 2);
            // Unescape doubled quotes
            field = field.Replace("\"\"", "\"");
        }
        // If not quoted and matches null character, return null
        else if (nullChar != null && field == nullChar)
        {
            return null;
        }

        return field;
    }
}