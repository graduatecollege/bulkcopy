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
            Description = "The number of rows to insert per batch, default 2000 (env:BULKCOPY_BATCH_SIZE).",
            DefaultValueFactory = result =>
            {
                var val = Environment.GetEnvironmentVariable("BULKCOPY_BATCH_SIZE");
                return val != null ? int.Parse(val) : 2000;
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
        
        var schemaOption = new Option<string>("--schema")
        {
            Description = "The destination schema name (env:BULKCOPY_SCHEMA).",
            DefaultValueFactory = result => Environment.GetEnvironmentVariable("BULKCOPY_SCHEMA") ?? "dbo"
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
        
        var emptyOption = new Option<bool?>("--empty")
        {
            Description = "Empty the destination table before importing (no env variable)."
        };

        var allowEmptyCsvOption = new Option<bool?>("--allow-empty-csv")
        {
            Description = "Normally the program exits with an error if the CSV is empty. When this flag is provided, a warning is logged instead. (no env variable)."
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
            emptyOption,
            allowEmptyCsvOption,
            timeoutOption,
            databaseOption,
            schemaOption,
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

        rootCommand.SetAction(async parseResult =>
        {
            var csvFile = parseResult.GetRequiredValue(csvFileArgument);

            var connectionString = parseResult.GetValue(connectionStringOption);
            var batchSize = parseResult.GetRequiredValue(batchSizeOption);
            var server = parseResult.GetValue(serverOption);
            var username = parseResult.GetValue(usernameOption);
            var password = parseResult.GetValue(passwordOption);
            var database = parseResult.GetValue(databaseOption);
            var schema = parseResult.GetRequiredValue(schemaOption);
            var table = parseResult.GetValue(tableOption);
            var trustServerCertificate = parseResult.GetValue(trustServerCertificateOption);
            var empty = parseResult.GetValue(emptyOption);
            var allowEmptyCsv = parseResult.GetValue(allowEmptyCsvOption);
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

            var doEmpty = false;
            if (empty.HasValue && empty.Value)
            {
                doEmpty = true;
            }

            var allowEmptyCsvValue = allowEmptyCsv.HasValue && allowEmptyCsv.Value;

            var exitCode = ExecuteBulkCopy(
                csvFile,
                connectionString,
                SanitizeSqlIdentifier(schema),
                SanitizeSqlIdentifier(table),
                batchSize,
                errorDatabase,
                errorTable,
                nullChar,
                doEmpty,
                allowEmptyCsvValue
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
        string schema,
        string destinationTable,
        int batchSize,
        string? errorDatabase,
        string errorTable,
        string nullChar,
        bool doEmpty,
        bool allowEmptyCsv
    )
    {
        try
        {
            if (!File.Exists(csvFilePath))
            {
                Console.WriteLine($"Error: CSV file not found: {csvFilePath}");
                return 1;
            }

            Console.WriteLine($"Starting bulk copy from {csvFilePath} to {schema}.{destinationTable}...");
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
                schema,
                destinationTable,
                csvFilePath,
                batchSize,
                errorDatabase,
                errorTable,
                nullChar,
                doEmpty,
                allowEmptyCsv);

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

    private static List<(int, int)> LoadColumnMappings(
        SqlConnection connection,
        IReadOnlyList<string> sourceColumnNames,
        string schema,
        string destinationTable)
    {
        const string sql = """
                           SELECT name
                           FROM sys.columns
                           WHERE object_id = OBJECT_ID(@ObjectId)
                           ORDER BY column_id;
                           """;

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@ObjectId", $"[{schema}].[{destinationTable}]");

        using var reader = command.ExecuteReader();
        var columns = new List<string>();
        while (reader.Read())
        {
            columns.Add(reader.GetString(0));
        }

        if (columns.Count == 0)
        {
            throw new InvalidOperationException(
                $"Destination table not found or has no columns: [{schema}].[{destinationTable}]");
        }

        List<(int, int)> mappings = [];
        
        foreach (var (i, sourceColumnName) in sourceColumnNames.Index())
        {
            var columnIndex = columns.IndexOf(sourceColumnName);
            if (columnIndex == -1)
            {
                throw new InvalidOperationException(
                    $"CSV column '{sourceColumnName}' does not exist in destination table '{schema}.{destinationTable}'.");
            }
            mappings.Add((i, columnIndex));
        }

        return mappings;
    }

    private static void ConfigureColumnMappings(
        SqlBulkCopy bulkCopy,
        IReadOnlyList<(int, int)> mappings)
    {
        foreach (var (sourceColumnIndex, destinationColumnIndex) in mappings)
        {
            bulkCopy.ColumnMappings.Add(sourceColumnIndex, destinationColumnIndex);
        }
    }

    private static (int SuccessCount, int FailedCount) BulkCopyToSqlServerStreaming(
        string connectionString,
        string schema,
        string destinationTable,
        string csvFilePath,
        int batchSize,
        string? errorDatabase,
        string errorTable,
        string nullChar,
        bool doEmpty,
        bool allowEmptyCsv
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
            
            if (doEmpty) 
            {
                EmptyTable(connection, destinationDatabase, schema, destinationTable);
            }

            using (var csvReader = new CsvDataReader(csvFilePath, nullChar))
            using (var batchedReader = new BatchedCsvReader(csvReader, batchSize))
            {
                if (allowEmptyCsv)
                {
                    if (!csvReader.TryReadHeader())
                    {
                        Console.WriteLine($"Warning: CSV file is empty: {csvFilePath}");
                        return (0, 0);
                    }
                }
                else
                {
                    csvReader.ReadHeader();
                }
                // Get CSV headers for error logging
                var csvHeaders = string.Join(",", batchedReader.ColumnNames);

                var mappings = LoadColumnMappings(connection, csvReader.ColumnNames, schema, destinationTable);

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
                        schema,
                        destinationTable,
                        batchTable,
                        batchStartRow,
                        csvHeaders,
                        mappings,
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
        string schema,
        string destinationTable,
        DataTable batchTable,
        int batchStartRow,
        string csvHeaders,
        IReadOnlyList<(int, int)> mappings,
        string destinationDatabase,
        string? errorDatabase,
        string? errorTable
    )
    {
        var successCount = 0;
        var failedCount = 0;
        var rowsInBatch = batchTable.Rows.Count;
        var destination = $"[{schema}].[{destinationTable}]";

        try
        {
            // Try to insert the batch
            using var bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.DestinationTableName = destination;
            bulkCopy.BatchSize = rowsInBatch;
            bulkCopy.BulkCopyTimeout = 30;

            ConfigureColumnMappings(bulkCopy, mappings);

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
                    bulkCopy.DestinationTableName = destination;
                    bulkCopy.BatchSize = 1;
                    bulkCopy.BulkCopyTimeout = 30;
                    
                    ConfigureColumnMappings(bulkCopy, mappings);

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
                            destination,
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

    private static void EmptyTable(SqlConnection connection, string database, string schema, string table)
    {
        var destination = $"[{database}].[{schema}].[{table}]";
        Console.WriteLine($"Emptying table {destination}");
        using var command = new SqlCommand($"DELETE FROM {destination}", connection);
        command.ExecuteNonQuery();
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