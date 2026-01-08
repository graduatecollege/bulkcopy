using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;
using static System.Runtime.InteropServices.OSPlatform;
using static System.Runtime.InteropServices.RuntimeInformation;

namespace BulkCopy.IntegrationTests.Fixtures;

public class TestFixture : IAsyncLifetime
{
    public const string TestDatabase = "TestDB";
    public const string ErrorDatabase = "ErrorLogDB";
    public const string TestTable = "TestTable";
    public const string ErrorTable = "BulkCopyErrors";
    public static readonly string TestCsvFile = Path.GetFullPath("test_data");
    
    private readonly MsSqlContainer _sqlContainer;
    private List<string> _testFiles = [];
    
    public TestFixture()
    {
        _sqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .WithCleanUp(true)
            .Build();
    }

    public string ConnectionString => _sqlContainer.GetConnectionString();
    
    public string RandomTestFileName()
    {
        var path = TestCsvFile + "-" + Path.GetRandomFileName() + ".csv";
        _testFiles.Add(path);
        return path;
    }

    
    public virtual async Task InitializeAsync()
    {
        await _sqlContainer.StartAsync();
        Console.WriteLine("Connection string: " + _sqlContainer.GetConnectionString());
        await CreateTestDatabaseAndTable();
        await CreateErrorDatabase();
    }

    public async Task RunBulkCopy(string? path = null)
    {
        path ??= CreateTestCsvFile();

        await BuildApplication();
        var (exitCode, output, error) = await RunBulkCopyAndGetOutput(path,
            new()
            {
                { "database", TestDatabase },
                { "connection-string", ConnectionString },
                { "table", TestTable },
                { "batch-size", "10" },
                { "error-database", ErrorDatabase },
                { "error-table", ErrorTable },
                { "trust-server-certificate", "true"}
            });
        
        Console.WriteLine(output);
        
        if (exitCode != 0)
        {
            throw new Exception($"BulkCopy exited with code {exitCode}. Output: {output}. Error: {error}");
        }
    }

    public async Task DisposeAsync()
    {
        _testFiles.ForEach(CleanupTestCsvFile);
        await _sqlContainer.DisposeAsync();
    }

    public Task<SqlConnection> OpenConnectionAsync(string database)
    {
        var cs = $"{ConnectionString};Database={database}";
        return OpenConnectionWithConnectionStringAsync(cs);
    }

    public Task<SqlConnection> OpenConnectionToTestDbAsync()
    {
        return OpenConnectionAsync(TestDatabase);
    }

    public Task<SqlConnection> OpenConnectionToErrorDbAsync()
    {
        return OpenConnectionAsync(ErrorDatabase);
    }

    private static async Task<SqlConnection> OpenConnectionWithConnectionStringAsync(string connectionString)
    {
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }

    private async Task CreateTestDatabaseAndTable(string testTableName = TestTable)
    {
        await using var connection = await OpenConnectionWithConnectionStringAsync(ConnectionString);

        await using var createDbCommand = new SqlCommand($"CREATE DATABASE {TestDatabase};", connection);
        await createDbCommand.ExecuteNonQueryAsync();

        await CreateTestTable(testTableName);
    }

    public async Task CreateTestTable(string testTableName = TestTable)
    {
        await using var connection = await OpenConnectionWithConnectionStringAsync(ConnectionString);
        await connection.ChangeDatabaseAsync(TestDatabase);
        await using var createTableCommand = new SqlCommand(
            $"""
             CREATE TABLE {testTableName} (
                 ID INT,
                 Name NVARCHAR(100),
                 Age INT,
                 Salary DECIMAL(10,2),
                 IsActive NVARCHAR(1),
                 BirthDate date,
                 CreatedAt datetime2,
                 Score decimal(4,2),
                 Description NVARCHAR(MAX),
                 Code NVARCHAR(7) not null
             );
             """,
            connection);
        await createTableCommand.ExecuteNonQueryAsync();
    }

    private async Task CreateErrorDatabase()
    {
        await using var connection = await OpenConnectionWithConnectionStringAsync(ConnectionString);

        await using var createDbCommand = new SqlCommand($"CREATE DATABASE {ErrorDatabase};", connection);
        await createDbCommand.ExecuteNonQueryAsync();
    }

    public string CreateTestCsvFile()
    {
        var csvContent = """
                         ID,Name,Age,Salary,IsActive,BirthDate,CreatedAt,Score,Description,Code
                         1,Alice Johnson,30,75000.50,1,1993-05-15,2024-01-01 10:00:00,95.5,Excellent employee,EMPL001
                         2,Bob Smith,25,65000.00,1,1998-08-22,2024-01-02 11:30:00,88.3,Good performer,EMPL002
                         3,Carol White,35,85000.75,1,1988-12-10,2024-01-03 09:15:00,92.1,Senior staff,EMPL003
                         4,David Brown,28,70000.00,1,1995-03-18,2024-01-04 14:20:00,89.7,Team player,EMPL004
                         5,Eve Davis,32,78000.25,1,1991-07-25,2024-01-05 08:45:00,91.2,"Reliable
                         worker",EMPL005
                         6,Frank Miller,BadAge,68000.00,1,1996-11-30,2024-01-06 13:10:00,85.9,Age is invalid,EMPL006
                         7,Grace Lee,27,72000.50,1,1996-09-14,2024-01-07 10:30:00,90.4,High potential,EMPL007
                         8,Henry Wilson,31,80000.00,1,1992-04-20,2024-01-08 15:25:00,93.6,Exceptional,EMPL008
                         9,Iris Taylor,29,71000.75,1,1994-06-08,2024-01-09 09:50:00,87.8,"Consis ""tent"" ",EMPL009
                         10,Jack Anderson,26,67000.00,1,1997-10-12,2024-01-10 11:15:00,86.5,Developing,EMPL010
                         11,Karen Thomas,InvalidAge,73000.50,1,1990-02-28,2024-01-11 12:40:00,88.9,Another bad age,EMPL011
                         12,Leo Martinez,33,82000.00,1,1990-01-05,2024-01-12 14:05:00,94.2,Strong performer,EMPL012
                         13,Mia Jackson,24,␀,1,1999-12-19,2024-01-13 10:20:00,84.7,Entry level,EMPL013
                         14,Noah Garcia,36,87000.75,1,1987-08-16,2024-01-14 08:55:00,95.8,Senior expert,EMPL014
                         15,Olivia Rodriguez,30,76000.00,1,1993-05-23,2024-01-15 13:30:00,90.1,Mid-level,EMPL015
                         16,Paul White,InvalidData,InvalidSalary,1,1995-07-11,2024-01-16 09:45:00,NotANumber,Multiple errors,EMPL016
                         17,Quinn Harris,28,71000.25,1,1995-11-27,2024-01-17 11:50:00,89.3,Good worker,EMPL017
                         18,Rachel Clark,34,83000.50,1,1989-03-14,2024-01-18 14:15:00,92.7,Valuable asset,EMPL018
                         19,Sam Lewis,27,69000.00,1,1996-09-30,2024-01-19 10:05:00,87.5,Promising,EMPL019
                         20,Tina Walker,31,77000.75,1,1992-06-18,2024-01-20 12:35:00,91.8,Dedicated,EMPL020
                         21,Uma Young,29,70000.00,1,1996-02-10,2024-01-21 10:00:00,88.1,New hire,␀
                         22,Vincent King,33,81000.00,1,0204-09-15,2024-01-22 11:11:11,90.0,Ancient birthday,EMPL022
                         23,Wendy Scott,41,91000.00,1,a,2024-01-23 12:12:12,93.3,Bad Date,EMPL023
                         24,Xavier Adams,27,68000.00,1,1997-07-07,2024-01-24 13:13:13,85.5,Too long code,EMPL024LONG
                         25,Yara Perez,26,62000.00,1,1998-02-02,2024-01-25 14:14:14,84.0,Recent grad,EMPL025
                         """;

        var fileName = RandomTestFileName();
        File.WriteAllText(fileName, csvContent);
        return fileName;
    }

    public string CreateEmptyCsvFile()
    {
        var fileName = RandomTestFileName();
        File.WriteAllText(fileName, string.Empty);
        return fileName;
    }

    public string CreateReorderedColumnsTestCsvFile()
    {
        // Keep this dataset simple (no embedded newlines/quotes) so we can safely reorder columns.
        var csvContent = """
                         Name,ID,Age,Salary,IsActive,CreatedAt,Score,Description,Code,BirthDate
                         Alice Johnson,1,30,75000.50,1,2024-01-01 10:00:00,95.5,Excellent employee,EMPL001,1993-05-15
                         Bob Smith,2,25,65000.00,1,2024-01-02 11:30:00,88.3,Good performer,EMPL002,1998-08-22
                         """;

        var fileName = RandomTestFileName();
        File.WriteAllText(fileName, csvContent);
        return fileName;
    }

    private static void CleanupTestCsvFile(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private static async Task BuildApplication()
    {
        var projectDir = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "BulkCopy");

        var process = new Process
        {
            StartInfo = new ProcessStartInfo
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

    public async Task<(int ExitCode, string Output, string Error)> RunBulkCopyAndGetOutput(string path,
        Dictionary<string, string>? bcArgs,
        Dictionary<string, string>? envVars = null,
        string? additionalArgs = null
    )
    {
        var projectDir = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "BulkCopy");

        var rid = IsOSPlatform(Windows)
            ? "win-x64"
            : IsOSPlatform(OSX)
                ? "osx-x64"
                : "linux-x64";

        var bulkCopyPath = Path.Combine(projectDir,
            "bin",
            "Release",
            "net10.0",
            rid,
            "BulkCopy");
        if (IsOSPlatform(Windows))
        {
            bulkCopyPath += ".exe";
        }

        var args = $"\"{path}\"";

        if (bcArgs != null)
        {
            foreach (var (key, value) in bcArgs)
            {
                if (value != "true")
                {
                    args += $" --{key} \"{value}\"";
                }
                else
                {
                    args += $" --{key}";
                }
            }
        }
        
        var env = string.Join("\n  ", envVars != null ? envVars.Select(kvp => $"{kvp.Key}={kvp.Value}") : Enumerable.Empty<string>());
        
        Console.WriteLine($"Invoking BulkCopy");
        Console.WriteLine($"Arguments: {args}");
        Console.WriteLine($"Environment variables: \n  {env}");

        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = bulkCopyPath,
                Arguments = args,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        if (envVars != null)
        {
            foreach (var kvp in envVars)
            {
                process.StartInfo.EnvironmentVariables[kvp.Key] = kvp.Value;
            }
        }

        process.Start();
        var output = await process.StandardOutput.ReadToEndAsync();
        var error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        return (process.ExitCode, output, error);
    }
}