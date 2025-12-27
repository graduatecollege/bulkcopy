using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;
using BulkCopy.IntegrationTests.Fixtures;
using Xunit;

namespace BulkCopy.IntegrationTests;

public sealed class EnvVarIntegrationTests(TestFixture fixture)
    : IClassFixture<TestFixture>
{
    [Fact]
    public async Task BulkCopy_UsesConnectionStringFromEnv()
    {
        var tableName = "EnvTableConnStr";
        await fixture.CreateTestTable(tableName);
        var path = fixture.CreateTestCsvFile();

        var fullConnectionString = $"{fixture.ConnectionString};Database={TestFixture.TestDatabase}";

        var envVars = new Dictionary<string, string>
        {
            { "BULKCOPY_CONNECTION_STRING", fullConnectionString }
        };

        var (exitCode, output, error) = await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "table", tableName },
                { "trust-server-certificate", "true" }
            },
            envVars);

        if (exitCode != 0 || !output.Contains("successes=19"))
        {
            Assert.Fail($"ExitCode: {exitCode}\nOutput: {output}\nError: {error}");
        }

        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var count = await GetRowCount(connection, tableName);
        Assert.Equal(19, count);
    }

    [Fact]
    public async Task BulkCopy_UsesUserPassFromEnv()
    {
        var tableName = "EnvTableUserPass";
        await fixture.CreateTestTable(tableName);
        var path = fixture.CreateTestCsvFile();

        var builder = new SqlConnectionStringBuilder(fixture.ConnectionString);

        var envVars = new Dictionary<string, string>
        {
            { "BULKCOPY_USERNAME", builder.UserID },
            { "BULKCOPY_PASSWORD", builder.Password },
            { "BULKCOPY_DB_SERVER", builder.DataSource }
        };

        var (exitCode, output, error) = await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "database", TestFixture.TestDatabase },
                { "table", tableName },
                { "trust-server-certificate", "true" }
            },
            envVars);

        if (exitCode != 0 || !output.Contains("successes=19"))
        {
            Assert.Fail($"ExitCode: {exitCode}\nOutput: {output}\nError: {error}");
        }

        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var count = await GetRowCount(connection, tableName);
        Assert.Equal(19, count);
    }

    [Fact]
    public async Task BulkCopy_UsesTableAndDatabaseFromEnv()
    {
        var tableName = "EnvTableAndDb";
        await fixture.CreateTestTable(tableName);
        var path = fixture.CreateTestCsvFile();

        var fullConnectionString = $"{fixture.ConnectionString};Database={TestFixture.TestDatabase}";

        var envVars = new Dictionary<string, string>
        {
            { "BULKCOPY_CONNECTION_STRING", fullConnectionString },
            { "BULKCOPY_TABLE", tableName },
            { "BULKCOPY_DATABASE", TestFixture.TestDatabase }
        };

        // Call without connection string, table name or database arguments/options
        var (exitCode, output, error) = await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "trust-server-certificate", "true" }
            },
            envVars);

        if (exitCode != 0 || !output.Contains("successes=19"))
        {
            Assert.Fail($"ExitCode: {exitCode}\nOutput: {output}\nError: {error}");
        }
    }

    [Fact]
    public async Task BulkCopy_UsesBatchSizeFromEnv()
    {
        var tableName = "EnvBatchSize";
        await fixture.CreateTestTable(tableName);
        var path = fixture.CreateTestCsvFile();

        var fullConnectionString = $"{fixture.ConnectionString};Database={TestFixture.TestDatabase}";

        var envVars = new Dictionary<string, string>
        {
            { "BULKCOPY_CONNECTION_STRING", fullConnectionString },
            { "BULKCOPY_TABLE", tableName },
            { "BULKCOPY_BATCH_SIZE", "5" }
        };

        // Call and check if batch size 5 is used (should see more batch messages)
        var (exitCode, output, error) = await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "trust-server-certificate", "true" }
            },
            envVars);

        // Batch 1 (1-5) succeeds, Batch 2 (6-10) fails because of bad row 6.
        if (exitCode != 0 || !output.Contains("Batch succeeded: rows 1 to 5") ||
            !output.Contains("Batch failed for rows 6 to 10"))
        {
            Assert.Fail($"ExitCode: {exitCode}\nOutput: {output}\nError: {error}");
        }
    }

    [Fact]
    public async Task BulkCopy_OptionTakesPrecedenceOverEnvVar()
    {
        var tableNameEnv = "EnvTableShouldNotUse";
        var tableNameOpt = "OptTableShouldUse";
        await fixture.CreateTestTable(tableNameOpt);
        var path = fixture.CreateTestCsvFile();

        var fullConnectionString = $"{fixture.ConnectionString};Database={TestFixture.TestDatabase}";

        var envVars = new Dictionary<string, string>
        {
            { "BULKCOPY_CONNECTION_STRING", fullConnectionString },
            { "BULKCOPY_TABLE", tableNameEnv }
        };

        await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "table", tableNameOpt },
                { "trust-server-certificate", "true" }
            },
            envVars);

        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var count = await GetRowCount(connection, tableNameOpt);
        Assert.Equal(19, count);
    }

    private async Task<int> GetRowCount(SqlConnection connection, string tableName)
    {
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", connection);
        var value = await command.ExecuteScalarAsync();
        if (value is int count)
        {
            return count;
        }

        throw new InvalidCastException($"Cannot convert {value} to int.");
    }
}