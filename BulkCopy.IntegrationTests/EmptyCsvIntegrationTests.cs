using BulkCopy.IntegrationTests.Fixtures;

namespace BulkCopy.IntegrationTests;

public sealed class EmptyCsvIntegrationTests(TestFixture fixture)
    : IClassFixture<TestFixture>
{
    [Fact]
    public async Task BulkCopy_EmptyCsv_WithAllowEmptyCsv_Succeeds()
    {
        var tableName = "EmptyCsvAllow";
        await fixture.CreateTestTable(tableName);
        var path = fixture.CreateEmptyCsvFile();

        try
        {
            var fullConnectionString = $"{fixture.ConnectionString};Database={TestFixture.TestDatabase}";

            var envVars = new Dictionary<string, string>
            {
                { "BULKCOPY_CONNECTION_STRING", fullConnectionString }
            };

            var result = await fixture.RunBulkCopyAndGetOutput(path,
                new Dictionary<string, string>
                {
                    { "table", tableName },
                    { "trust-server-certificate", "true" },
                    { "allow-empty-csv", "true" }
                },
                envVars);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Warning: CSV file is empty", result.Output);

            await using var connection = await fixture.OpenConnectionToTestDbAsync();
            var count = await IntegrationTests.GetRowCount(connection, tableName);
            Assert.Equal(0, count);
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    [Fact]
    public async Task BulkCopy_EmptyCsv_WithoutAllowEmptyCsv_Fails()
    {
        var tableName = "EmptyCsvDeny";
        await fixture.CreateTestTable(tableName);
        var path = fixture.CreateEmptyCsvFile();

        try
        {
            var fullConnectionString = $"{fixture.ConnectionString};Database={TestFixture.TestDatabase}";

            var envVars = new Dictionary<string, string>
            {
                { "BULKCOPY_CONNECTION_STRING", fullConnectionString }
            };

            var result = await fixture.RunBulkCopyAndGetOutput(path,
                new Dictionary<string, string>
                {
                    { "table", tableName },
                    { "trust-server-certificate", "true" }
                },
                envVars);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("CSV file is empty or has no header row", result.Output);
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }
}