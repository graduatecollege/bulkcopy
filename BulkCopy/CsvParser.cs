using System.Data;
using System.Text;

namespace BulkCopy;

public class CsvParser
{
    public static DataTable LoadCsvToDataTable(string filePath)
    {
        DataTable dataTable = new DataTable();
        
        using (StreamReader reader = new StreamReader(filePath))
        {
            // Read first row for headers
            string? firstRow = ReadCsvRow(reader);
            if (string.IsNullOrEmpty(firstRow))
            {
                throw new InvalidOperationException("CSV file is empty or has no header row.");
            }

            // Parse headers and create columns
            string[] headers = ParseCsvLine(firstRow);
            foreach (string header in headers)
            {
                dataTable.Columns.Add(header.Trim());
            }

            // Read data rows
            string? row;
            while ((row = ReadCsvRow(reader)) != null)
            {
                if (string.IsNullOrWhiteSpace(row))
                    continue;

                string[] fields = ParseCsvLine(row);
                
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

                dataTable.Rows.Add(fields);
            }
        }

        return dataTable;
    }

    public static DataTable LoadCsvFromStream(StreamReader reader)
    {
        DataTable dataTable = new DataTable();

        // Read first row for headers
        string? firstRow = ReadCsvRow(reader);
        if (string.IsNullOrEmpty(firstRow))
        {
            throw new InvalidOperationException("CSV file is empty or has no header row.");
        }

        // Parse headers and create columns
        string[] headers = ParseCsvLine(firstRow);
        foreach (string header in headers)
        {
            dataTable.Columns.Add(header.Trim());
        }

        // Read data rows
        string? row;
        while ((row = ReadCsvRow(reader)) != null)
        {
            if (string.IsNullOrWhiteSpace(row))
                continue;

            string[] fields = ParseCsvLine(row);
            
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

            dataTable.Rows.Add(fields);
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

    public static string[] ParseCsvLine(string line)
    {
        List<string> fields = new List<string>();
        bool inQuotes = false;
        int fieldStart = 0;

        for (int i = 0; i < line.Length; i++)
        {
            if (line[i] == '"')
            {
                inQuotes = !inQuotes;
            }
            else if (line[i] == ',' && !inQuotes)
            {
                fields.Add(ExtractField(line, fieldStart, i));
                fieldStart = i + 1;
            }
        }

        // Add the last field
        fields.Add(ExtractField(line, fieldStart, line.Length));

        return fields.ToArray();
    }

    public static string ExtractField(string line, int start, int end)
    {
        string field = line.Substring(start, end - start).Trim();
        
        // Remove surrounding quotes if present
        if (field.StartsWith('"') && field.EndsWith('"') && field.Length >= 2)
        {
            field = field.Substring(1, field.Length - 2);
            // Unescape doubled quotes
            field = field.Replace("\"\"", "\"");
        }

        return field;
    }
}
