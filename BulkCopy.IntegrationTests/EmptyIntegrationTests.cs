using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;
using BulkCopy.IntegrationTests.Fixtures;
using Xunit;

namespace BulkCopy.IntegrationTests;

public sealed class EmptyIntegrationTests(TestFixture fixture)
    : IClassFixture<TestFixture>
{

    [Fact]
    public async Task BulkCopy_WithEmpty_HasNoDuplicates()
    {
        var tableName = "DoEmpty";
        await fixture.CreateTestTable(tableName);
        var path = fixture.CreateTestCsvFile();

        var fullConnectionString = $"{fixture.ConnectionString};Database={TestFixture.TestDatabase}";

        var envVars = new Dictionary<string, string>
        {
            { "BULKCOPY_CONNECTION_STRING", fullConnectionString }
        };

        await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "table", tableName },
                { "trust-server-certificate", "true" }
            },
            envVars);

        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var count = await IntegrationTests.GetRowCount(connection, tableName);
        Assert.Equal(19, count);
        
        await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "table", tableName },
                { "trust-server-certificate", "true" },
                { "empty", "true" }
            },
            envVars);
        
        count = await IntegrationTests.GetRowCount(connection, tableName);
        Assert.Equal(19, count);
    }
    
    [Fact]
    public async Task BulkCopy_WithoutEmpty_HasDuplicates()
    {
        var tableName = "DoNotEmpty";
        await fixture.CreateTestTable(tableName);
        var path = fixture.CreateTestCsvFile();
        var fullConnectionString = $"{fixture.ConnectionString};Database={TestFixture.TestDatabase}";
        var envVars = new Dictionary<string, string>
        {
            { "BULKCOPY_CONNECTION_STRING", fullConnectionString }
        };

        await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "table", tableName },
                { "trust-server-certificate", "true" }
            },
            envVars);

        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var count = await IntegrationTests.GetRowCount(connection, tableName);
        Assert.Equal(19, count);
        
        await fixture.RunBulkCopyAndGetOutput(path,
            new()
            {
                { "table", tableName },
                { "trust-server-certificate", "true" }
            },
            envVars);
        
        count = await IntegrationTests.GetRowCount(connection, tableName);
        Assert.Equal(38, count);
    }
}