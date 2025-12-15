using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace BulkCopy;

public class Program
{
    private static readonly System.Text.RegularExpressions.Regex SqlIdentifierRegex =
        new(@"^[a-zA-Z_][a-zA-Z0-9_]*$",
            System.Text.RegularExpressions.RegexOptions.Compiled);

    static int Main(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine(
                "Usage: BulkCopy <csv-file> <connection-string> <table-name> [batch-size] [--error-database <db>] [--error-table <table>] [--null-char <char>]");
            Console.WriteLine(
                "Example: BulkCopy data.csv \"Server=localhost;Database=mydb;User Id=sa;Password=pass;\" MyTable 1000");
            Console.WriteLine("  --error-database: Optional database name for error logging (uses same connection)");
            Console.WriteLine("  --error-table: Optional table name for error logging (default: BulkCopyErrors)");
            Console.WriteLine(
                "  --null-char: Optional character to treat as null when unquoted (default: UTF-8 null character U+2400)");
            return 1;
        }

        var csvFilePath = args[0];
        var connectionString = args[1];
        var destinationTable = args[2];
        var batchSize = 1000;
        string? errorDatabase = null;
        string? errorTable = null;
        var nullChar = "␀";

        // Parse positional and optional arguments
        var currentArg = 3;
        if (currentArg < args.Length && !args[currentArg].StartsWith("--"))
        {
            if (!int.TryParse(args[currentArg], out batchSize) || batchSize <= 0)
            {
                Console.WriteLine("Error: Batch size must be a positive integer.");
                return 1;
            }

            currentArg++;
        }

        while (currentArg < args.Length)
        {
            switch (args[currentArg])
            {
                case "--error-database" when currentArg + 1 < args.Length:
                    errorDatabase = args[currentArg + 1];
                    break;
                case "--error-table" when currentArg + 1 < args.Length:
                    errorTable = args[currentArg + 1];
                    break;
                case "--null-char" when currentArg + 1 < args.Length:
                    nullChar = args[currentArg + 1];
                    break;
                default:
                    Console.WriteLine($"Error: Unknown argument '{args[currentArg]}'");
                    return 1;
            }

            currentArg += 2;
        }

        // Set default error table name if error database is specified
        if (errorDatabase != null && errorTable == null)
        {
            errorTable = "BulkCopyErrors";
        }

        try
        {
            if (!File.Exists(csvFilePath))
            {
                Console.WriteLine($"Error: CSV file not found: {csvFilePath}");
                return 1;
            }

            Console.WriteLine($"Starting bulk copy from {csvFilePath} to {destinationTable}...");
            if (errorDatabase != null)
            {
                Console.WriteLine($"Error logging enabled: {errorDatabase}.{errorTable}");
            }

            if (nullChar != "␀")
            {
                var nullCharUtfCode = $"U+{char.ConvertToUtf32(nullChar, 0):X4}";
                Console.WriteLine(
                    $"Using custom null character: {(nullChar.Length == 0 ? "(empty string)" : nullCharUtfCode)}");
            }

            var csvLoadStopwatch = Stopwatch.StartNew();
            var dataTable = CsvParser.LoadCsvToDataTable(csvFilePath, nullChar);
            csvLoadStopwatch.Stop();
            Console.WriteLine($"Loaded {dataTable.Rows.Count} rows from CSV file.");
            Console.WriteLine(
                $"CSV loading took {csvLoadStopwatch.Elapsed.TotalSeconds:F2}s ({csvLoadStopwatch.ElapsedMilliseconds}ms).");

            var bulkCopyStopwatch = Stopwatch.StartNew();
            
            var result = BulkCopyToSqlServer(connectionString,
                destinationTable,
                dataTable,
                batchSize,
                errorDatabase,
                errorTable);
            
            bulkCopyStopwatch.Stop();
            Console.WriteLine(
                $"Bulk copy took {bulkCopyStopwatch.Elapsed.TotalSeconds:F2}s ({bulkCopyStopwatch.ElapsedMilliseconds}ms).");
            Console.WriteLine(
                $"Import completed. Successfully imported {result.SuccessCount} rows, failed {result.FailedCount} rows.");
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

    static (int SuccessCount, int FailedCount) BulkCopyToSqlServer(string connectionString,
        string destinationTable,
        DataTable dataTable,
        int batchSize,
        string? errorDatabase,
        string? errorTable
    )
    {
        int successCount = 0;
        int failedCount = 0;

        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            // Get source database from connection
            string destinationDatabase = connection.Database;

            // Ensure error table exists if error logging is enabled
            if (errorDatabase != null && errorTable != null)
            {
                EnsureErrorTableExists(connection, errorDatabase, errorTable);
            }

            // Try to copy all rows in batches
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = destinationTable;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.BulkCopyTimeout = 300;

                ConfigureColumnMappings(bulkCopy, dataTable.Columns.Count);

                var result = ProcessRowsWithErrorHandling(
                    connection,
                    destinationTable,
                    dataTable,
                    batchSize,
                    destinationDatabase,
                    errorDatabase,
                    errorTable);
                successCount = result.SuccessCount;
                failedCount = result.FailedCount;
            }
        }

        return (successCount, failedCount);
    }

    static (int SuccessCount, int FailedCount) ProcessRowsWithErrorHandling(
        SqlConnection connection,
        string destinationTable,
        DataTable sourceTable,
        int batchSize,
        string destinationDatabase,
        string? errorDatabase,
        string? errorTable
    )
    {
        int successCount = 0;
        int failedCount = 0;
        int currentRow = 0;

        var sourceView = sourceTable.DefaultView;

        // Get CSV headers for error logging
        string csvHeaders = string.Join(",", sourceTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName));

        while (currentRow < sourceTable.Rows.Count)
        {
            int batchStartRow = currentRow;
            int rowsInBatch = Math.Min(batchSize, sourceTable.Rows.Count - currentRow);

            // We have to copy the view into rows since SqlBulkCopy doesn't support views
            var batchRows = new DataRow[rowsInBatch];
            for (int i = 0; i < rowsInBatch; i++)
            {
                batchRows[i] = sourceView[batchStartRow + i].Row;
            }

            try
            {
                // Try to insert the batch
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = destinationTable;
                    bulkCopy.BatchSize = batchSize;
                    bulkCopy.BulkCopyTimeout = 300;

                    ConfigureColumnMappings(bulkCopy, sourceTable.Columns.Count);

                    bulkCopy.WriteToServer(batchRows);
                    successCount += rowsInBatch;
                    // Use 1-based row numbers in user-facing messages
                    Console.WriteLine($"Batch succeeded: rows {batchStartRow + 1} to {batchStartRow + rowsInBatch}");
                }

                currentRow += rowsInBatch;
            }
            catch (Exception batchEx)
            {
                Console.WriteLine(
                    $"Batch failed for rows {batchStartRow + 1} to {batchStartRow + rowsInBatch}: {batchEx.Message}");
                Console.WriteLine("Processing batch rows individually...");

                for (int i = 0; i < rowsInBatch; i++)
                {
                    int rowNumber = batchStartRow + i + 1; // Convert to 1-based for user-facing messages
                    var singleRow = sourceView[batchStartRow + i].Row;

                    try
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = destinationTable;
                            bulkCopy.BatchSize = 1;
                            bulkCopy.BulkCopyTimeout = 300;

                            ConfigureColumnMappings(bulkCopy, sourceTable.Columns.Count);

                            bulkCopy.WriteToServer(new[] { singleRow });
                            successCount++;
                        }
                    }
                    catch (Exception rowEx)
                    {
                        Console.WriteLine($"ERROR: Failed to import row {rowNumber}: {rowEx.Message}");
                        failedCount++;

                        if (errorDatabase != null && errorTable != null)
                        {
                            string csvRowData = ConvertRowToCsv(sourceTable.Rows[batchStartRow + i]);
                            LogErrorToTable(connection,
                                errorDatabase,
                                errorTable,
                                destinationDatabase,
                                destinationTable,
                                rowNumber,
                                csvHeaders,
                                csvRowData,
                                rowEx.Message);
                        }
                    }
                }

                currentRow += rowsInBatch;
                if (currentRow < sourceTable.Rows.Count)
                {
                    Console.WriteLine($"Continuing from row {currentRow + 1}...");
                }
            }
        }

        return (successCount, failedCount);
    }

    static void EnsureErrorTableExists(SqlConnection connection, string errorDatabase, string errorTable)
    {
        string sanitizedDatabase = SanitizeSqlIdentifier(errorDatabase);
        string sanitizedTable = SanitizeSqlIdentifier(errorTable);

        string createTableSql = $@"
            IF NOT EXISTS (SELECT * FROM [{sanitizedDatabase}].sys.tables WHERE name = @TableName)
            BEGIN
                CREATE TABLE [{sanitizedDatabase}].[dbo].[{sanitizedTable}] (
                    [Id] INT IDENTITY(1,1) PRIMARY KEY,
                    [SourceDatabase] NVARCHAR(128) NOT NULL,
                    [SourceTable] NVARCHAR(128) NOT NULL,
                    [RowNumber] INT NOT NULL,
                    [CsvHeaders] NVARCHAR(MAX) NOT NULL,
                    [CsvRowData] NVARCHAR(MAX) NOT NULL,
                    [ErrorMessage] NVARCHAR(MAX) NOT NULL,
                    [ErrorTimestamp] DATETIME2 NOT NULL DEFAULT GETDATE()
                );
            END";

        using (var command = new SqlCommand(createTableSql, connection))
        {
            command.Parameters.AddWithValue("@TableName", sanitizedTable);
            command.ExecuteNonQuery();
        }

        Console.WriteLine($"Error table [{sanitizedDatabase}].[dbo].[{sanitizedTable}] is ready.");
    }

    static void LogErrorToTable(SqlConnection connection,
        string errorDatabase,
        string errorTable,
        string sourceDatabase,
        string sourceTable,
        int rowNumber,
        string csvHeaders,
        string csvRowData,
        string errorMessage
    )
    {
        // Validate and sanitize SQL identifiers to prevent SQL injection
        string sanitizedDatabase = SanitizeSqlIdentifier(errorDatabase);
        string sanitizedTable = SanitizeSqlIdentifier(errorTable);

        string insertSql = $@"
            INSERT INTO [{sanitizedDatabase}].[dbo].[{sanitizedTable}] 
                ([SourceDatabase], [SourceTable], [RowNumber], [CsvHeaders], [CsvRowData], [ErrorMessage], [ErrorTimestamp])
            VALUES 
                (@SourceDatabase, @SourceTable, @RowNumber, @CsvHeaders, @CsvRowData, @ErrorMessage, GETDATE())";

        try
        {
            using (var command = new SqlCommand(insertSql, connection))
            {
                command.Parameters.AddWithValue("@SourceDatabase", sourceDatabase);
                command.Parameters.AddWithValue("@SourceTable", sourceTable);
                command.Parameters.AddWithValue("@RowNumber", rowNumber);
                command.Parameters.AddWithValue("@CsvHeaders", csvHeaders);
                command.Parameters.AddWithValue("@CsvRowData", csvRowData);
                command.Parameters.AddWithValue("@ErrorMessage", errorMessage);
                command.ExecuteNonQuery();
            }

            Console.WriteLine($"  Logged error to {sanitizedDatabase}.{sanitizedTable}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  Warning: Failed to log error to table: {ex.Message}");
        }
    }

    public static string ConvertRowToCsv(DataRow row)
    {
        List<string> fields = new List<string>();
        foreach (var item in row.ItemArray)
        {
            string value = item?.ToString() ?? "";
            // Escape quotes and wrap in quotes if contains comma, quote, or newline
            if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            {
                value = "\"" + value.Replace("\"", "\"\"") + "\"";
            }

            fields.Add(value);
        }

        return string.Join(",", fields);
    }

    public static string SanitizeSqlIdentifier(string identifier)
    {
        // SQL identifiers can contain alphanumeric characters, underscores, and must start with a letter or underscore
        // Remove any characters that are not allowed and escape any square brackets
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("SQL identifier cannot be null or empty", nameof(identifier));
        }

        // Remove any existing square brackets to prevent injection
        string sanitized = identifier.Replace("[", "").Replace("]", "");

        // Validate that identifier contains only safe characters
        if (!SqlIdentifierRegex.IsMatch(sanitized))
        {
            throw new ArgumentException(
                $"Invalid SQL identifier: '{identifier}'. Identifiers must start with a letter or underscore and contain only alphanumeric characters and underscores.",
                nameof(identifier));
        }

        return sanitized;
    }
}