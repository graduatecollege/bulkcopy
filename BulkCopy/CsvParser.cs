using System.Data;
using System.Text;

namespace BulkCopy;

public class CsvParser
{
    public static DataTable LoadCsvToDataTable(string filePath, string? nullChar = "␀")
    {
        using (StreamReader reader = new StreamReader(filePath, Encoding.UTF8))
        {
            return LoadCsvFromStream(reader, nullChar);
        }
    }

    public static DataTable LoadCsvFromStream(StreamReader reader, string? nullChar = "␀")
    {
        DataTable dataTable = new DataTable();

        // Read first row for headers
        string? firstRow = ReadCsvRow(reader);
        if (string.IsNullOrEmpty(firstRow))
        {
            throw new InvalidOperationException("CSV file is empty or has no header row.");
        }

        // Parse headers and create columns
        string?[] headers = ParseCsvLine(firstRow, nullChar);
        foreach (string? header in headers)
        {
            dataTable.Columns.Add(header?.Trim() ?? "");
        }

        // Read data rows
        while (ReadCsvRow(reader) is { } row)
        {
            if (string.IsNullOrWhiteSpace(row))
                continue;

            string?[] fields = ParseCsvLine(row, nullChar);
            
            // Handle rows with fewer fields than headers
            if (fields.Length < headers.Length)
            {
                int originalLength = fields.Length;
                Array.Resize(ref fields, headers.Length);
                for (int i = originalLength; i < headers.Length; i++)
                {
                    fields[i] = string.Empty;
                }
            }

            // Convert string? fields to objects, handling DBNull for null values
            object[] rowValues = new object[fields.Length];
            for (int i = 0; i < fields.Length; i++)
            {
                var value = fields[i];
                rowValues[i] = value == null ? DBNull.Value : value;
            }
            dataTable.Rows.Add(rowValues);
        }

        return dataTable;
    }

    public static string? ReadCsvRow(StreamReader reader)
    {
        StringBuilder row = new StringBuilder();
        bool inQuotes = false;
        int currentChar;

        while ((currentChar = reader.Read()) != -1)
        {
            char c = (char)currentChar;

            if (c == '"')
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
            }
            else if (c == '\n' && !inQuotes)
            {
                // End of row found (not inside quotes)
                // Remove trailing \r if present
                if (row.Length > 0 && row[row.Length - 1] == '\r')
                {
                    row.Length--;
                }
                return row.Length > 0 ? row.ToString() : null;
            }
            else if (c == '\r' && !inQuotes)
            {
                // Check if next char is \n (Windows line ending)
                int nextChar = reader.Peek();
                if (nextChar == '\n')
                {
                    reader.Read(); // Consume the \n
                }
                // Return row (handles both Windows \r\n and Mac \r line endings)
                return row.Length > 0 ? row.ToString() : null;
            }
            else
            {
                row.Append(c);
            }
        }

        // Return the last row if any content remains
        return row.Length > 0 ? row.ToString() : null;
    }

    public static string?[] ParseCsvLine(string line, string? nullChar = "␀")
    {
        List<string?> fields = new List<string?>();
        bool inQuotes = false;
        int fieldStart = 0;

        for (int i = 0; i < line.Length; i++)
        {
            if (line[i] == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    i++; // Escaped quote inside quoted field
                }
                else
                {
                    inQuotes = !inQuotes;
                }
            }
            else if (line[i] == ',' && !inQuotes)
            {
                fields.Add(ExtractField(line, fieldStart, i, nullChar));
                fieldStart = i + 1;
            }
        }

        // Add the last field
        fields.Add(ExtractField(line, fieldStart, line.Length, nullChar));

        return fields.ToArray();
    }

    public static string? ExtractField(string line, int start, int end, string? nullChar = "␀")
    {
        string field = line.Substring(start, end - start).Trim();
        
        // Check if the field is quoted
        bool isQuoted = field.StartsWith('"') && field.EndsWith('"') && field.Length >= 2;
        
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
