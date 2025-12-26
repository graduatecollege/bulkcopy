using System.CommandLine;
using System.CommandLine.Help;
using System.CommandLine.Invocation;
using System.Data;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace BulkCopy;

public partial class Program
{
    [GeneratedRegex("^[a-zA-Z_][a-zA-Z0-9_]*$", RegexOptions.Compiled)]
    private static partial Regex SqlIdentifierRegex();

    private static async Task<int> Main(string[] args)
    {
        var csvFileArgument = new Argument<string>("csv-file")
        {
            Description = "The CSV file to import."
        };

        var connectionStringOption = new Option<string?>("--connection-string")
        {
            Description = "The SQL Server connection string (env:BULKCOPY_CONNECTION_STRING).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_CONNECTION_STRING")
        };

        var batchSizeOption = new Option<int>("--batch-size")
        {
            Description = "The number of rows to insert per batch, default 500 (env:BULKCOPY_BATCH_SIZE).",
            DefaultValueFactory = result =>
            {
                var val = Environment.GetEnvironmentVariable("BULKCOPY_BATCH_SIZE");
                return val != null ? int.Parse(val) : 500;
            }
        };

        var serverOption = new Option<string?>("--server")
        {
            Description = "The destination SQL Server instance name (env:BULKCOPY_DB_SERVER).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_DB_SERVER")
        };

        var usernameOption = new Option<string?>("--username")
        {
            Description = "The SQL Server username (env:BULKCOPY_USERNAME).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_USERNAME")
        };

        var passwordOption = new Option<string?>("--password")
        {
            Description = "The SQL Server password (env:BULKCOPY_PASSWORD).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_PASSWORD")
        };

        var databaseOption = new Option<string?>("--database")
        {
            Description = "The destination database name (env:BULKCOPY_DATABASE).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_DATABASE")
        };

        var tableOption = new Option<string?>("--table")
        {
            Description = "The destination table name (env:BULKCOPY_TABLE).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_TABLE")
        };

        var trustServerCertificateOption = new Option<bool?>("--trust-server-certificate")
        {
            Description = "Trust server certificate (no env variable)."
        };

        var timeoutOption = new Option<int?>("--timeout")
        {
            Description = "Connection timeout in seconds, defaults to 30 (env:BULKCOPY_TIMEOUT).",
            DefaultValueFactory = result =>
            {
                var val = Environment.GetEnvironmentVariable("BULKCOPY_TIMEOUT");
                return val != null ? int.Parse(val) : null;
            }
        };

        var errorDatabaseOption = new Option<string?>("--error-database")
        {
            Description = "Optional database name for error logging on the same server (env:BULKCOPY_ERROR_DATABASE).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_ERROR_DATABASE")
        };

        var errorTableOption = new Option<string>("--error-table")
        {
            Description = "Optional table name for error logging, defaults to BulkCopyErrors (env:BULKCOPY_ERROR_TABLE).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_ERROR_TABLE") ?? "BulkCopyErrors"
        };

        var nullCharOption = new Option<string>("--null-char")
        {
            Description = "Optional character to treat as null when unquoted, defaults to \"␀\" (env:BULKCOPY_NULL_CHAR).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_NULL_CHAR") ?? "␀"
        };

        var rootCommand = new RootCommand("Bulk copy CSV data to SQL Server")
        {
            csvFileArgument,
            connectionStringOption,
            batchSizeOption,
            errorDatabaseOption,
            errorTableOption,
            nullCharOption,
            serverOption,
            usernameOption,
            passwordOption,
            trustServerCertificateOption,
            timeoutOption,
            databaseOption,
            tableOption
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
            var csvFile = parseResult.GetRequiredValue(csvFileArgument);

            var connectionString = parseResult.GetValue(connectionStringOption);
            var batchSize = parseResult.GetRequiredValue(batchSizeOption);
            var server = parseResult.GetValue(serverOption);
            var username = parseResult.GetValue(usernameOption);
            var password = parseResult.GetValue(passwordOption);
            var database = parseResult.GetValue(databaseOption);
            var table = parseResult.GetValue(tableOption);
            var trustServerCertificate = parseResult.GetValue(trustServerCertificateOption);
            var timeout = parseResult.GetValue(timeoutOption);
            var errorDatabase = parseResult.GetValue(errorDatabaseOption);
            var errorTable = parseResult.GetRequiredValue(errorTableOption);
            var nullChar = parseResult.GetRequiredValue(nullCharOption);
            
            var sqlBuilder = new SqlConnectionStringBuilder();
            if (!string.IsNullOrEmpty(connectionString))
            {
                connectionString = ResolveConnectionString(connectionString);
                sqlBuilder = new SqlConnectionStringBuilder(connectionString);
            }

            if (trustServerCertificate.HasValue)
            {
                sqlBuilder.TrustServerCertificate = trustServerCertificate.Value;
            }

            if (timeout.HasValue)
            {
                sqlBuilder.ConnectTimeout = timeout.Value;
            }

            if (!string.IsNullOrEmpty(server))
            {
                sqlBuilder.DataSource = server;
            }

            if (!string.IsNullOrEmpty(username))
            {
                sqlBuilder.UserID = username;
            }

            if (!string.IsNullOrEmpty(password))
            {
                sqlBuilder.Password = password;
            }

            if (!string.IsNullOrEmpty(database))
            {
                sqlBuilder.InitialCatalog = database;
            }

            connectionString = sqlBuilder.ConnectionString;

            if (string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine(
                    "Error: Must provide connection details as options or environment variables. Use either connection string or server, username, password and database options.");
                return 1;
            }

            if (string.IsNullOrEmpty(table))
            {
                Console.WriteLine("Error: Table name must be provided via --table option or BULKCOPY_TABLE.");
                return 1;
            }

            var exitCode = ExecuteBulkCopy(
                csvFile,
                connectionString,
                table,
                batchSize,
                errorDatabase,
                errorTable,
                nullChar
            );

            return exitCode;
        });
        
        var parseResult = rootCommand.Parse(args);

        return await parseResult.InvokeAsync();
    }

    public static string ResolveConnectionString(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            throw new ArgumentException("Connection string cannot be null or empty", nameof(input));
        }

        bool isPath = input.StartsWith('/') ||
                      input.StartsWith('\\') ||
                      input.StartsWith('.') ||
                      (input.Length >= 2 && char.IsLetter(input[0]) && input[1] == ':');

        if (!isPath)
        {
            return input;
        }

        return File.Exists(input)
            ? File.ReadAllText(input).Trim()
            : throw new ArgumentException($"Invalid connection string: '{input}'");
    }

    private static int ExecuteBulkCopy(string csvFilePath,
        string connectionString,
        string destinationTable,
        int batchSize,
        string? errorDatabase,
        string errorTable,
        string nullChar
    )
    {
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
                $"Bulk copy took {bulkCopyStopwatch.ElapsedMilliseconds}ms.");
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
        string errorTable,
        string nullChar
    )
    {
        var successCount = 0;
        var failedCount = 0;

        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            var destinationDatabase = connection.Database;

            // Ensure error table exists if error logging is enabled
            if (errorDatabase != null)
            {
                EnsureErrorTableExists(connection, errorDatabase, errorTable);
            }

            using (var csvReader = new CsvDataReader(csvFilePath, nullChar))
            using (var batchedReader = new BatchedCsvReader(csvReader, batchSize))
            {
                csvReader.ReadHeader();
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

        var createTableSql = $"""
                              IF OBJECT_ID(N'[dbo].[{sanitizedTable}]', 'U') IS NULL
                              BEGIN
                                  CREATE TABLE [{sanitizedDatabase}].[dbo].[{sanitizedTable}] (
                                      [Id] INT IDENTITY PRIMARY KEY,
                                      [SourceDatabase] NVARCHAR(128) NOT NULL,
                                      [SourceTable] NVARCHAR(128) NOT NULL,
                                      [RowNumber] INT NOT NULL,
                                      [CsvRowData] NVARCHAR(MAX) NOT NULL,
                                      [ErrorMessage] NVARCHAR(MAX) NOT NULL,
                                      [ErrorTimestamp] DATETIME2 NOT NULL DEFAULT SYSDATETIME()
                                  );
                              END
                              """;

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

        var insertSql = $"""
                         INSERT INTO [{sanitizedDatabase}].[dbo].[{sanitizedTable}] 
                             ([SourceDatabase], [SourceTable], [RowNumber], [CsvRowData], [ErrorMessage], [ErrorTimestamp])
                         VALUES 
                             (@SourceDatabase, @SourceTable, @RowNumber, @CsvRowData, @ErrorMessage, GETDATE())
                         """;

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
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("SQL identifier cannot be null or empty", nameof(identifier));
        }

        // Remove any existing square brackets to prevent injection
        var sanitized = identifier.Replace("[", "").Replace("]", "");

        if (!SqlIdentifierRegex().IsMatch(sanitized))
        {
            throw new ArgumentException(
                $"Invalid SQL identifier: '{identifier}'. Identifiers must start with a letter or underscore and contain only alphanumeric characters and underscores.",
                nameof(identifier));
        }

        return sanitized;
    }
}

class CustomHelpAction(HelpAction action) : SynchronousCommandLineAction
{
    public override int Invoke(ParseResult parseResult)
    {
        int result = action.Invoke(parseResult);

        Console.WriteLine("""
                          Examples:
                            
                            Bulk copy csv-file.csv to table MyTable in database MyDB on localhost
                            
                              ./BulkCopy csv-file.csv --connection-string "Server=127.0.0.1,1433;Database=MyDB;User Id=sa;Password=password;TrustServerCertificate=True" --table MyTable
                              
                            Log errors into ErrorDB.dbo.BulkCopyErrors as well
                            
                              ./BulkCopy csv-file.csv --connection-string "Server=127.0.0.1,1433;Database=MyDB;User Id=sa;Password=password;TrustServerCertificate=True" --table MyTable --error-database ErrorDB
                              
                          """);

        return result;
    }
}