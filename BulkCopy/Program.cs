using System.Data;
using Microsoft.Data.SqlClient;

namespace BulkCopy;

class Program
{
    static int Main(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("Usage: BulkCopy <csv-file> <connection-string> <table-name> [batch-size]");
            Console.WriteLine("Example: BulkCopy data.csv \"Server=localhost;Database=mydb;User Id=sa;Password=pass;\" MyTable 1000");
            return 1;
        }

        string csvFilePath = args[0];
        string connectionString = args[1];
        string tableName = args[2];
        int batchSize = 1000;
        
        if (args.Length > 3)
        {
            if (!int.TryParse(args[3], out batchSize) || batchSize <= 0)
            {
                Console.WriteLine("Error: Batch size must be a positive integer.");
                return 1;
            }
        }

        try
        {
            if (!File.Exists(csvFilePath))
            {
                Console.WriteLine($"Error: CSV file not found: {csvFilePath}");
                return 1;
            }

            Console.WriteLine($"Starting bulk copy from {csvFilePath} to {tableName}...");
            
            DataTable dataTable = LoadCsvToDataTable(csvFilePath);
            Console.WriteLine($"Loaded {dataTable.Rows.Count} rows from CSV file.");

            BulkCopyToSqlServer(connectionString, tableName, dataTable, batchSize);
            
            Console.WriteLine($"Successfully imported {dataTable.Rows.Count} rows to {tableName}.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            return 1;
        }
    }

    static DataTable LoadCsvToDataTable(string filePath)
    {
        DataTable dataTable = new DataTable();
        
        using (StreamReader reader = new StreamReader(filePath))
        {
            // Read header line
            string? headerLine = reader.ReadLine();
            if (string.IsNullOrEmpty(headerLine))
            {
                throw new InvalidOperationException("CSV file is empty or has no header row.");
            }

            // Parse headers and create columns
            string[] headers = ParseCsvLine(headerLine);
            foreach (string header in headers)
            {
                dataTable.Columns.Add(header.Trim());
            }

            // Read data lines
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                string[] fields = ParseCsvLine(line);
                
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

    static string[] ParseCsvLine(string line)
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

    static string ExtractField(string line, int start, int end)
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

    static void BulkCopyToSqlServer(string connectionString, string tableName, DataTable dataTable, int batchSize)
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.BulkCopyTimeout = 300; // 5 minutes timeout

                // Map columns by ordinal position
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    bulkCopy.ColumnMappings.Add(i, i);
                }

                // Event handlers for progress tracking
                bulkCopy.SqlRowsCopied += (sender, e) =>
                {
                    Console.WriteLine($"Copied {e.RowsCopied} rows...");
                };

                bulkCopy.NotifyAfter = batchSize;

                bulkCopy.WriteToServer(dataTable);
            }
        }
    }
}
