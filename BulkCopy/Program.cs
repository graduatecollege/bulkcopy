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

            var result = BulkCopyToSqlServer(connectionString, tableName, dataTable, batchSize);
            
            Console.WriteLine($"Import completed. Successfully imported {result.SuccessCount} rows, failed {result.FailedCount} rows.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            return 1;
        }
    }

    static void ConfigureColumnMappings(SqlBulkCopy bulkCopy, int columnCount)
    {
        // Map columns by ordinal position
        for (int i = 0; i < columnCount; i++)
        {
            bulkCopy.ColumnMappings.Add(i, i);
        }
    }

    static (int SuccessCount, int FailedCount) BulkCopyToSqlServer(string connectionString, string tableName, DataTable dataTable, int batchSize)
    {
        int successCount = 0;
        int failedCount = 0;
        
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            connection.Open();

            try
            {
                // Try to copy all rows in batches
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = tableName;
                    bulkCopy.BatchSize = batchSize;
                    bulkCopy.BulkCopyTimeout = 300; // 5 minutes timeout

                    ConfigureColumnMappings(bulkCopy, dataTable.Columns.Count);

                    // Event handlers for progress tracking
                    bulkCopy.SqlRowsCopied += (sender, e) =>
                    {
                        Console.WriteLine($"Copied {e.RowsCopied} rows...");
                    };

                    bulkCopy.NotifyAfter = batchSize;

                    bulkCopy.WriteToServer(dataTable);
                    successCount = dataTable.Rows.Count;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Batch operation failed: {ex.Message}");
                Console.WriteLine("Switching to row-by-row processing with error handling...");
                
                // Process rows with error handling
                var result = ProcessRowsWithErrorHandling(connection, tableName, dataTable, batchSize);
                successCount = result.SuccessCount;
                failedCount = result.FailedCount;
            }
        }
        
        return (successCount, failedCount);
    }

    static (int SuccessCount, int FailedCount) ProcessRowsWithErrorHandling(SqlConnection connection, string tableName, DataTable sourceTable, int batchSize)
    {
        int successCount = 0;
        int failedCount = 0;
        int currentRow = 0; // 0-based index for accessing sourceTable.Rows
        
        while (currentRow < sourceTable.Rows.Count)
        {
            // Create a batch DataTable
            DataTable batchTable = sourceTable.Clone();
            int batchStartRow = currentRow; // 0-based index
            int rowsInBatch = Math.Min(batchSize, sourceTable.Rows.Count - currentRow);
            
            for (int i = 0; i < rowsInBatch; i++)
            {
                batchTable.ImportRow(sourceTable.Rows[currentRow + i]);
            }
            
            try
            {
                // Try to insert the batch
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = tableName;
                    bulkCopy.BatchSize = batchSize;
                    bulkCopy.BulkCopyTimeout = 300;

                    ConfigureColumnMappings(bulkCopy, sourceTable.Columns.Count);

                    bulkCopy.WriteToServer(batchTable);
                    successCount += rowsInBatch;
                    // Use 1-based row numbers in user-facing messages
                    Console.WriteLine($"Batch succeeded: rows {batchStartRow + 1} to {batchStartRow + rowsInBatch}");
                }
                
                currentRow += rowsInBatch;
            }
            catch (Exception batchEx)
            {
                Console.WriteLine($"Batch failed for rows {batchStartRow + 1} to {batchStartRow + rowsInBatch}: {batchEx.Message}");
                Console.WriteLine("Processing batch rows individually...");
                
                // Process this batch row by row
                for (int i = 0; i < rowsInBatch; i++)
                {
                    int rowNumber = batchStartRow + i + 1; // Convert to 1-based for user-facing messages
                    DataTable singleRowTable = sourceTable.Clone();
                    singleRowTable.ImportRow(sourceTable.Rows[batchStartRow + i]);
                    
                    try
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BatchSize = 1;
                            bulkCopy.BulkCopyTimeout = 300;

                            ConfigureColumnMappings(bulkCopy, sourceTable.Columns.Count);

                            bulkCopy.WriteToServer(singleRowTable);
                            successCount++;
                        }
                    }
                    catch (Exception rowEx)
                    {
                        Console.WriteLine($"ERROR: Failed to import row {rowNumber}: {rowEx.Message}");
                        failedCount++;
                    }
                }
                
                currentRow += rowsInBatch;
                // Continue processing remaining rows (still using row-by-row error handling mode)
                if (currentRow < sourceTable.Rows.Count)
                {
                    Console.WriteLine($"Continuing from row {currentRow + 1}...");
                }
            }
        }
        
        return (successCount, failedCount);
    }
}
