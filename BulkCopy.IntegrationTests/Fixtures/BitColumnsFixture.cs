using Microsoft.Data.SqlClient;

namespace BulkCopy.IntegrationTests.Fixtures;

public sealed class BitColumnsFixture : TestFixture
{
    public const string BitTestTable = "BitTestTable";

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await CreateBitTestTable();
        var path = CreateBitTestCsvFile();
        await RunBulkCopyForBitTest(path);
    }

    private async Task CreateBitTestTable()
    {
        await using var connection = await OpenConnectionWithConnectionStringAsync(ConnectionString);
        await connection.ChangeDatabaseAsync(TestDatabase);
        await using var createTableCommand = new SqlCommand(
            $"""
             CREATE TABLE {BitTestTable} (
                 ID INT,
                 Name NVARCHAR(100),
                 IsActive BIT,
                 IsDeleted BIT,
                 IsVerified BIT
             );
             """,
            connection);
        await createTableCommand.ExecuteNonQueryAsync();
    }

    private string CreateBitTestCsvFile()
    {
        var csvContent = """
                         ID,Name,IsActive,IsDeleted,IsVerified
                         1,Alice,1,0,1
                         2,Bob,0,0,1
                         3,Carol,1,1,0
                         4,David,0,1,0
                         5,Eve,true,false,1
                         """;

        var fileName = RandomTestFileName();
        File.WriteAllText(fileName, csvContent);
        return fileName;
    }

    private async Task RunBulkCopyForBitTest(string path)
    {
        await BuildApplication();
        var (exitCode, output, error) = await RunBulkCopyAndGetOutput(path,
            new()
            {
                { "database", TestDatabase },
                { "connection-string", ConnectionString },
                { "table", BitTestTable },
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

    private static async Task<SqlConnection> OpenConnectionWithConnectionStringAsync(string connectionString)
    {
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }
}
