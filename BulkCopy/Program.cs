using System.CommandLine;
using System.CommandLine.Help;
using System.CommandLine.Invocation;
using System.Data;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace BulkCopy;

public class Program
{
    private static readonly Regex SqlIdentifierRegex = new("^[a-zA-Z_][a-zA-Z0-9_]*$", RegexOptions.Compiled);

    private static async Task<int> Main(string[] args)
    {
        var csvFileArgument = new Argument<string>("csv-file")
        {
            Description = "The CSV file to import."
        };

        var connectionStringArgument = new Argument<string>("connection-string")
        {
            Description = "The SQL Server connection string."
        };

        var tableNameArgument = new Argument<string>("table-name")
        {
            Description = "The destination table name."
        };

        var batchSizeArgument = new Argument<int>("batch-size")
        {
            Description = "The number of rows to insert per batch.",
            DefaultValueFactory = parseResult => 1000,
            Validators =
            {
                result =>
                {
                    if (result.GetValueOrDefault<int>() < 0)
                    {
                        result.AddError("Batch size must be a positive integer.");
                    }
                }
            }
        };

        var errorDatabaseOption = new Option<string?>("--error-database")
        {
            Description = "Optional database name for error logging (uses same connection)."
        };

        var errorTableOption = new Option<string?>("--error-table")
        {
            Description = "Optional table name for error logging (default: BulkCopyErrors)."
        };

        var nullCharOption = new Option<string>("--null-char")
        {
            Description = "Optional character to treat as null when unquoted.",
            DefaultValueFactory = parseResult => "␀"
        };

        var rootCommand = new RootCommand("Bulk copy CSV data to SQL Server")
        {
            csvFileArgument,
            connectionStringArgument,
            tableNameArgument,
            batchSizeArgument,
            errorDatabaseOption,
            errorTableOption,
            nullCharOption
        };

        foreach (var opt in rootCommand.Options)
        {
            if (opt is not HelpOption defaultHelpOption)
            {
                continue;
            }

            defaultHelpOption.Action = new CustomHelpAction((HelpAction)defaultHelpOption.Action!);
            break;
        }

        rootCommand.SetAction(parseResult =>
        {
            var exitCode = ExecuteBulkCopy(
                parseResult.GetRequiredValue(csvFileArgument),
                parseResult.GetRequiredValue(connectionStringArgument),
                parseResult.GetRequiredValue(tableNameArgument),
                parseResult.GetValue(batchSizeArgument),
                parseResult.GetValue(errorDatabaseOption),
                parseResult.GetValue(errorTableOption),
                parseResult.GetRequiredValue(nullCharOption)
            );

            return exitCode;
        });


        var parseResult = rootCommand.Parse(args);

        return await parseResult.InvokeAsync();
    }

    private static int ExecuteBulkCopy(string csvFilePath,
        string connectionString,
        string destinationTable,
        int batchSize,
        string? errorDatabase,
        string? errorTable,
        string nullChar
    )
    {
        // Default error table
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

            Console.WriteLine("Starting streaming CSV import...");

            var bulkCopyStopwatch = Stopwatch.StartNew();

            var result = BulkCopyToSqlServerStreaming(connectionString,
                destinationTable,
                csvFilePath,
                batchSize,
                errorDatabase,
                errorTable,
                nullChar);

            bulkCopyStopwatch.Stop();
            Console.WriteLine(
                $"Bulk copy took {bulkCopyStopwatch.Elapsed.TotalSeconds:F2}s ({bulkCopyStopwatch.ElapsedMilliseconds}ms).");
            Console.WriteLine(
                $"Import completed: successes={result.SuccessCount} errors={result.FailedCount}");
            return 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            return 1;
        }
    }

    private static void ConfigureColumnMappings(SqlBulkCopy bulkCopy, int columnCount)
    {
        // Map columns by ordinal position
        for (var i = 0; i < columnCount; i++)
        {
            bulkCopy.ColumnMappings.Add(i, i);
        }
    }

    private static (int SuccessCount, int FailedCount) BulkCopyToSqlServerStreaming(
        string connectionString,
        string destinationTable,
        string csvFilePath,
        int batchSize,
        string? errorDatabase,
        string? errorTable,
        string? nullChar
    )
    {
        var successCount = 0;
        var failedCount = 0;

        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            var destinationDatabase = connection.Database;

            // Ensure error table exists if error logging is enabled
            if (errorDatabase != null && errorTable != null)
            {
                EnsureErrorTableExists(connection, errorDatabase, errorTable);
            }

            using (var csvReader = new CsvDataReader(csvFilePath, nullChar))
            using (var batchedReader = new BatchedCsvReader(csvReader, batchSize))
            {
                // Get CSV headers for error logging
                var csvHeaders = string.Join(",", batchedReader.ColumnNames);

                while (batchedReader.HasMoreRows)
                {
                    var batchTable = batchedReader.ReadNextBatch();
                    if (batchTable == null || batchTable.Rows.Count == 0)
                    {
                        break;
                    }

                    var batchStartRow = batchedReader.CurrentRowNumber - batchTable.Rows.Count;

                    var result = ProcessBatchWithErrorHandling(
                        connection,
                        destinationTable,
                        batchTable,
                        batchStartRow,
                        csvHeaders,
                        destinationDatabase,
                        errorDatabase,
                        errorTable);

                    successCount += result.SuccessCount;
                    failedCount += result.FailedCount;
                }
            }
        }

        return (successCount, failedCount);
    }

    private static (int SuccessCount, int FailedCount) ProcessBatchWithErrorHandling(
        SqlConnection connection,
        string destinationTable,
        DataTable batchTable,
        int batchStartRow,
        string csvHeaders,
        string destinationDatabase,
        string? errorDatabase,
        string? errorTable
    )
    {
        var successCount = 0;
        var failedCount = 0;
        var rowsInBatch = batchTable.Rows.Count;

        try
        {
            // Try to insert the batch
            using var bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.DestinationTableName = destinationTable;
            bulkCopy.BatchSize = rowsInBatch;
            bulkCopy.BulkCopyTimeout = 30;

            ConfigureColumnMappings(bulkCopy, batchTable.Columns.Count);

            bulkCopy.WriteToServer(batchTable);
            successCount += rowsInBatch;

            Console.WriteLine($"Batch succeeded: rows {batchStartRow + 1} to {batchStartRow + rowsInBatch}");
        }
        catch (Exception batchEx)
        {
            Console.WriteLine(
                $"Batch failed for rows {batchStartRow + 1} to {batchStartRow + rowsInBatch}: {batchEx.Message}");
            Console.WriteLine("Processing batch rows individually...");

            for (var i = 0; i < rowsInBatch; i++)
            {
                var rowNumber = batchStartRow + i + 1;
                var singleRow = batchTable.Rows[i];

                try
                {
                    using var bulkCopy = new SqlBulkCopy(connection);
                    bulkCopy.DestinationTableName = destinationTable;
                    bulkCopy.BatchSize = 1;
                    bulkCopy.BulkCopyTimeout = 30;

                    ConfigureColumnMappings(bulkCopy, batchTable.Columns.Count);

                    bulkCopy.WriteToServer(new[] { singleRow });
                    successCount++;
                }
                catch (Exception rowEx)
                {
                    Console.WriteLine($"ERROR: Failed to import row {rowNumber}: {rowEx.Message}");
                    failedCount++;

                    if (errorDatabase != null && errorTable != null)
                    {
                        var csvRowData = ConvertRowToCsv(singleRow);
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
        }

        return (successCount, failedCount);
    }

    private static void EnsureErrorTableExists(SqlConnection connection, string errorDatabase, string errorTable)
    {
        var sanitizedDatabase = SanitizeSqlIdentifier(errorDatabase);
        var sanitizedTable = SanitizeSqlIdentifier(errorTable);

        var createTableSql = $@"
            IF NOT EXISTS (SELECT * FROM [{sanitizedDatabase}].sys.tables WHERE name = @TableName)
            BEGIN
                CREATE TABLE [{sanitizedDatabase}].[dbo].[{sanitizedTable}] (
                    [Id] INT IDENTITY(1,1) PRIMARY KEY,
                    [SourceDatabase] NVARCHAR(128) NOT NULL,
                    [SourceTable] NVARCHAR(128) NOT NULL,
                    [RowNumber] INT NOT NULL,
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

    private static void LogErrorToTable(SqlConnection connection,
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
        var sanitizedDatabase = SanitizeSqlIdentifier(errorDatabase);
        var sanitizedTable = SanitizeSqlIdentifier(errorTable);

        var insertSql = $@"
            INSERT INTO [{sanitizedDatabase}].[dbo].[{sanitizedTable}] 
                ([SourceDatabase], [SourceTable], [RowNumber], [CsvRowData], [ErrorMessage], [ErrorTimestamp])
            VALUES 
                (@SourceDatabase, @SourceTable, @RowNumber, @CsvRowData, @ErrorMessage, GETDATE())";

        try
        {
            using (var command = new SqlCommand(insertSql, connection))
            {
                command.Parameters.AddWithValue("@SourceDatabase", sourceDatabase);
                command.Parameters.AddWithValue("@SourceTable", sourceTable);
                command.Parameters.AddWithValue("@RowNumber", rowNumber);
                command.Parameters.AddWithValue("@CsvRowData", csvHeaders + "\n" + csvRowData);
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
        var fields = new List<string>();
        foreach (var item in row.ItemArray)
        {
            var value = item?.ToString() ?? "";
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
        var sanitized = identifier.Replace("[", "").Replace("]", "");

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

internal class CustomHelpAction : SynchronousCommandLineAction
{
    private readonly HelpAction _defaultHelp;

    public CustomHelpAction(HelpAction action) => _defaultHelp = action;

    public override int Invoke(ParseResult parseResult)
    {
        int result = _defaultHelp.Invoke(parseResult);

        Console.WriteLine("""
                          Examples:
                            
                            Bulk copy csv-file.csv to table MyTable in database MyDB on localhost
                            
                              ./BulkCopy csv-file.csv "Server=127.0.0.1,56791;Database=MyDB;User Id=sa;Password=password;TrustServerCertificate=True" MyTable
                              
                            Log errors into ErrorDB.dbo.BulkCopyErrors as well
                            
                              ./BulkCopy csv-file.csv "Server=127.0.0.1,56791;Database=MyDB;User Id=sa;Password=password;TrustServerCertificate=True" MyTable --error-database ErrorDB
                              
                          """);

        return result;
    }
}