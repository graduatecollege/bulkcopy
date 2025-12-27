using BulkCopy.IntegrationTests.Fixtures;

namespace BulkCopy.IntegrationTests;

public class ReorderedColumnsIntegrationTests(ReorderedColumnsFixture fixture)
    : IClassFixture<ReorderedColumnsFixture>
{
    [Fact]
    public async Task BulkCopy_WithReorderedHeaders_InsertsCorrectData()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();

        var rowCount = await IntegrationTests.GetRowCount(connection, TestFixture.TestTable);
        Assert.Equal(2, rowCount);

        var nameForId1 = await IntegrationTests.GetNameForId(connection, TestFixture.TestTable, 1);
        Assert.Equal("Alice Johnson", nameForId1);
    }
}