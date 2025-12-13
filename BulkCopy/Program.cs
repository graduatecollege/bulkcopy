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
            
            DataTable dataTable = CsvParser.LoadCsvToDataTable(csvFilePath);
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
