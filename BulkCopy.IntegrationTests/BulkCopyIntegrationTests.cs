using System.Data;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;
using Xunit;

namespace BulkCopy.IntegrationTests;

public class BulkCopyIntegrationTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlContainer;
    private const string TestDatabase = "TestDB";
    private const string ErrorDatabase = "ErrorLogDB";
    private const string TestTable = "TestTable";
    private const string ErrorTable = "BulkCopyErrors";
    private const string TestCsvFile = "test_data.csv";

    public BulkCopyIntegrationTests()
    {
        _sqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _sqlContainer.StartAsync();
        await CreateTestDatabaseAndTable();
        await CreateErrorDatabase();
        CreateTestCsvFile();
    }

    public async Task DisposeAsync()
    {
        CleanupTestCsvFile();
        await _sqlContainer.DisposeAsync();
    }

    [Fact]
    public async Task BulkCopy_WithValidAndInvalidRows_ImportsValidRowsAndLogsErrors()
    {
        // Arrange
        var connectionString = _sqlContainer.GetConnectionString();

        // Build the application if not already built
        await BuildApplication();

        // Act
        var result = await RunBulkCopy("", connectionString);

        // Assert - Verify data import
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await connection.ChangeDatabaseAsync(TestDatabase);

        var validRowCount = await GetRowCount(connection, TestTable);
        Assert.Equal(20, validRowCount); // 25 total - 5 bad rows

        // Verify bad rows were skipped
        var badRowsCount = await GetCountForIds(connection, TestTable, new[] { 6, 11, 16, 23, 24 });
        Assert.Equal(0, badRowsCount);

        // Verify good rows were inserted
        var goodRowsCount = await GetCountForIds(connection, TestTable, new[] { 1, 5, 10, 15, 20 });
        Assert.Equal(5, goodRowsCount);

        // Assert - Verify error logging
        await connection.ChangeDatabaseAsync(ErrorDatabase);

        // Verify error table exists
        var errorTableExists = await TableExists(connection, ErrorTable);
        Assert.True(errorTableExists, "Error table should exist");

        // Verify correct number of errors logged
        var errorCount = await GetRowCount(connection, ErrorTable);
        Assert.Equal(5, errorCount);

        // Verify error table schema
        var columnCount = await GetColumnCount(connection, ErrorTable);
        Assert.Equal(8, columnCount);

        // Verify error row numbers
        var errorRowNumbers = await GetErrorRowNumbers(connection, ErrorTable);
        Assert.Contains(6, errorRowNumbers);
        Assert.Contains(11, errorRowNumbers);
        Assert.Contains(16, errorRowNumbers);
        Assert.Contains(23, errorRowNumbers);
        Assert.Contains(24, errorRowNumbers);

        // Verify error logs have correct source information
        var sourceDbCheck = await CountRowsWithSource(connection, ErrorTable, TestDatabase, TestTable);
        Assert.Equal(5, sourceDbCheck);

        // Verify CSV headers are present
        var headersCheck = await CountRowsWithPattern(connection, ErrorTable, "CsvHeaders", "%ID,Name,Age%");
        Assert.Equal(5, headersCheck);

        // Verify CSV row data and error messages are present
        var rowDataCheck = await CountRowsWhereColumnHasLength(connection, ErrorTable, "CsvRowData", 10);
        Assert.Equal(5, rowDataCheck);

        var errorMsgCheck = await CountRowsWhereColumnHasLength(connection, ErrorTable, "ErrorMessage", 10);
        Assert.Equal(5, errorMsgCheck);
    }

    private async Task CreateTestDatabaseAndTable()
    {
        await using var connection = new SqlConnection(_sqlContainer.GetConnectionString());
        await connection.OpenAsync();

        // Create test database
        await using var createDbCommand = new SqlCommand($"CREATE DATABASE {TestDatabase};", connection);
        await createDbCommand.ExecuteNonQueryAsync();

        // Create test table
        await connection.ChangeDatabaseAsync(TestDatabase);
        await using var createTableCommand = new SqlCommand($@"
            CREATE TABLE {TestTable} (
                ID INT,
                Name NVARCHAR(100),
                Age INT,
                Salary DECIMAL(10,2),
                IsActive NVARCHAR(1),
                BirthDate datetime2,
                CreatedAt datetime2,
                Score decimal(4,2),
                Description NVARCHAR(MAX),
                Code NVARCHAR(7)
            );", connection);
        await createTableCommand.ExecuteNonQueryAsync();
    }

    private async Task CreateErrorDatabase()
    {
        await using var connection = new SqlConnection(_sqlContainer.GetConnectionString());
        await connection.OpenAsync();

        await using var createDbCommand = new SqlCommand($"CREATE DATABASE {ErrorDatabase};", connection);
        await createDbCommand.ExecuteNonQueryAsync();
    }

    private void CreateTestCsvFile()
    {
        var csvContent = @"ID,Name,Age,Salary,IsActive,BirthDate,CreatedAt,Score,Description,Code
1,Alice Johnson,30,75000.50,1,1993-05-15,2024-01-01 10:00:00,95.5,Excellent employee,EMPL001
2,Bob Smith,25,65000.00,1,1998-08-22,2024-01-02 11:30:00,88.3,Good performer,EMPL002
3,Carol White,35,85000.75,1,1988-12-10,2024-01-03 09:15:00,92.1,Senior staff,EMPL003
4,David Brown,28,70000.00,1,1995-03-18,2024-01-04 14:20:00,89.7,Team player,EMPL004
5,Eve Davis,32,78000.25,1,1991-07-25,2024-01-05 08:45:00,91.2,Reliable worker,EMPL005
6,Frank Miller,BadAge,68000.00,1,1996-11-30,2024-01-06 13:10:00,85.9,Age is invalid,EMPL006
7,Grace Lee,27,72000.50,1,1996-09-14,2024-01-07 10:30:00,90.4,High potential,EMPL007
8,Henry Wilson,31,80000.00,1,1992-04-20,2024-01-08 15:25:00,93.6,Exceptional,EMPL008
9,Iris Taylor,29,71000.75,1,1994-06-08,2024-01-09 09:50:00,87.8,Consistent,EMPL009
10,Jack Anderson,26,67000.00,1,1997-10-12,2024-01-10 11:15:00,86.5,Developing,EMPL010
11,Karen Thomas,InvalidAge,73000.50,1,1990-02-28,2024-01-11 12:40:00,88.9,Another bad age,EMPL011
12,Leo Martinez,33,82000.00,1,1990-01-05,2024-01-12 14:05:00,94.2,Strong performer,EMPL012
13,Mia Jackson,24,64000.50,1,1999-12-19,2024-01-13 10:20:00,84.7,Entry level,EMPL013
14,Noah Garcia,36,87000.75,1,1987-08-16,2024-01-14 08:55:00,95.8,Senior expert,EMPL014
15,Olivia Rodriguez,30,76000.00,1,1993-05-23,2024-01-15 13:30:00,90.1,Mid-level,EMPL015
16,Paul White,InvalidData,InvalidSalary,1,1995-07-11,2024-01-16 09:45:00,NotANumber,Multiple errors,EMPL016
17,Quinn Harris,28,71000.25,1,1995-11-27,2024-01-17 11:50:00,89.3,Good worker,EMPL017
18,Rachel Clark,34,83000.50,1,1989-03-14,2024-01-18 14:15:00,92.7,Valuable asset,EMPL018
19,Sam Lewis,27,69000.00,1,1996-09-30,2024-01-19 10:05:00,87.5,Promising,EMPL019
20,Tina Walker,31,77000.75,1,1992-06-18,2024-01-20 12:35:00,91.8,Dedicated,EMPL020
21,Uma Young,29,70000.00,1,1996-02-10,2024-01-21 10:00:00,88.1,New hire,EMPL021
22,Vincent King,33,81000.00,1,0204-09-15,2024-01-22 11:11:11,90.0,Ancient birthday,EMPL022
23,Wendy Scott,41,91000.00,1,a,2024-01-23 12:12:12,93.3,Bad Date,EMPL023
24,Xavier Adams,27,68000.00,1,1997-07-07,2024-01-24 13:13:13,85.5,Too long code,EMPL024LONG
25,Yara Perez,26,62000.00,1,1998-02-02,2024-01-25 14:14:14,84.0,Recent grad,EMPL025";

        File.WriteAllText(TestCsvFile, csvContent);
    }

    private void CleanupTestCsvFile()
    {
        if (File.Exists(TestCsvFile))
        {
            File.Delete(TestCsvFile);
        }
    }

    private async Task BuildApplication()
    {
        var projectDir = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "BulkCopy");
        
        var process = new System.Diagnostics.Process
        {
            StartInfo = new System.Diagnostics.ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = "build -c Release",
                WorkingDirectory = projectDir,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        process.Start();
        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            var error = await process.StandardError.ReadToEndAsync();
            throw new Exception($"Build failed: {error}");
        }
    }

    private async Task<int> RunBulkCopy(string buildPath, string connectionString)
    {
        var fullConnectionString = $"{connectionString};Database={TestDatabase}";
        var projectDir = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "BulkCopy");
        
        // Determine runtime identifier based on current platform
        var rid = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows)
            ? "win-x64"
            : System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.OSX)
                ? "osx-x64"
                : "linux-x64";
        
        var bulkCopyPath = Path.Combine(projectDir, "bin", "Release", "net10.0", rid, "BulkCopy");
        if (!File.Exists(bulkCopyPath))
        {
            // Fallback: try without RID-specific folder
            bulkCopyPath = Path.Combine(projectDir, "bin", "Release", "net10.0", "BulkCopy");
            if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
            {
                bulkCopyPath += ".exe";
            }
        }
        
        var process = new System.Diagnostics.Process
        {
            StartInfo = new System.Diagnostics.ProcessStartInfo
            {
                FileName = bulkCopyPath,
                Arguments = $"{TestCsvFile} \"{fullConnectionString}\" {TestTable} 10 --error-database {ErrorDatabase} --error-table {ErrorTable}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        process.Start();
        var output = await process.StandardOutput.ReadToEndAsync();
        var error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        Console.WriteLine("BulkCopy Output:");
        Console.WriteLine(output);
        if (!string.IsNullOrEmpty(error))
        {
            Console.WriteLine("BulkCopy Errors:");
            Console.WriteLine(error);
        }

        return process.ExitCode;
    }

    private async Task<int> GetRowCount(SqlConnection connection, string tableName)
    {
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {tableName};", connection);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private async Task<int> GetCountForIds(SqlConnection connection, string tableName, int[] ids)
    {
        var idList = string.Join(",", ids);
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {tableName} WHERE ID IN ({idList});", connection);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private async Task<bool> TableExists(SqlConnection connection, string tableName)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM sys.tables WHERE name = @TableName;", connection);
        command.Parameters.AddWithValue("@TableName", tableName);
        var count = (int)(await command.ExecuteScalarAsync() ?? 0);
        return count > 0;
    }

    private async Task<int> GetColumnCount(SqlConnection connection, string tableName)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(@TableName);", connection);
        command.Parameters.AddWithValue("@TableName", tableName);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private async Task<List<int>> GetErrorRowNumbers(SqlConnection connection, string tableName)
    {
        var rowNumbers = new List<int>();
        await using var command = new SqlCommand($"SELECT RowNumber FROM {tableName} ORDER BY RowNumber;", connection);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rowNumbers.Add(reader.GetInt32(0));
        }
        return rowNumbers;
    }

    private async Task<int> CountRowsWithSource(SqlConnection connection, string tableName, string sourceDb, string sourceTable)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM {tableName} WHERE SourceDatabase = @SourceDb AND SourceTable = @SourceTable;", connection);
        command.Parameters.AddWithValue("@SourceDb", sourceDb);
        command.Parameters.AddWithValue("@SourceTable", sourceTable);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private async Task<int> CountRowsWithPattern(SqlConnection connection, string tableName, string columnName, string pattern)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM {tableName} WHERE {columnName} LIKE @Pattern;", connection);
        command.Parameters.AddWithValue("@Pattern", pattern);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private async Task<int> CountRowsWhereColumnHasLength(SqlConnection connection, string tableName, string columnName, int minLength)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM {tableName} WHERE LEN({columnName}) > @MinLength;", connection);
        command.Parameters.AddWithValue("@MinLength", minLength);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }
}
