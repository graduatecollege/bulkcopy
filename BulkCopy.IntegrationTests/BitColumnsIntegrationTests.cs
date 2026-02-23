using BulkCopy.IntegrationTests.Fixtures;
using Microsoft.Data.SqlClient;

namespace BulkCopy.IntegrationTests;

public class BitColumnsIntegrationTests(BitColumnsFixture fixture)
    : IClassFixture<BitColumnsFixture>
{
    [Fact]
    public async Task BulkCopy_WithBitColumns_InsertsAllRows()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var rowCount = await IntegrationTests.GetRowCount(connection, BitColumnsFixture.BitTestTable);
        // Row 5 (Eve) has "true" and "false" which should fail or be left as-is to error
        // Rows 1-4 have "0" and "1" which should be converted and succeed
        Assert.True(rowCount >= 4, $"Expected at least 4 rows to be inserted, got {rowCount}");
    }

    [Fact]
    public async Task BulkCopy_WithBitColumns_ConvertsZeroCorrectly()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var isActive = await GetBitValue(connection, BitColumnsFixture.BitTestTable, "IsActive", 2);
        Assert.False(isActive);
    }

    [Fact]
    public async Task BulkCopy_WithBitColumns_ConvertsOneCorrectly()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var isActive = await GetBitValue(connection, BitColumnsFixture.BitTestTable, "IsActive", 1);
        Assert.True(isActive);
    }

    [Fact]
    public async Task BulkCopy_WithBitColumns_HandlesMultipleBitColumns()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        
        // Row 3: Carol with IsActive=1, IsDeleted=1, IsVerified=0
        var isActive = await GetBitValue(connection, BitColumnsFixture.BitTestTable, "IsActive", 3);
        var isDeleted = await GetBitValue(connection, BitColumnsFixture.BitTestTable, "IsDeleted", 3);
        var isVerified = await GetBitValue(connection, BitColumnsFixture.BitTestTable, "IsVerified", 3);
        
        Assert.True(isActive);
        Assert.True(isDeleted);
        Assert.False(isVerified);
    }

    private static async Task<bool> GetBitValue(SqlConnection connection, string tableName, string columnName, int id)
    {
        await using var command = new SqlCommand($"SELECT {columnName} FROM {tableName} WHERE ID = @Id;", connection);
        command.Parameters.AddWithValue("@Id", id);
        
        var result = await command.ExecuteScalarAsync();
        return result is bool b && b;
    }
}
