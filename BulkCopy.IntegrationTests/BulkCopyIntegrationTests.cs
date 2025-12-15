using Microsoft.Data.SqlClient;
using Xunit;

namespace BulkCopy.IntegrationTests;

public sealed class BulkCopyIntegrationTests(BulkCopyIntegrationTestFixture fixture)
    : IClassFixture<BulkCopyIntegrationTestFixture>
{
    [Fact]
    public async Task BulkCopy_InsertsExpectedNumberOfValidRows()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var validRowCount = await GetRowCount(connection, BulkCopyIntegrationTestFixture.TestTable);
        Assert.Equal(19, validRowCount); // 25 total - 6 bad rows
    }

    [Fact]
    public async Task BulkCopy_DoesNotInsertKnownBadRows()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var badRowsCount = await GetCountForIds(connection, BulkCopyIntegrationTestFixture.TestTable, new[] { 6, 11, 16, 21, 23, 24 });
        Assert.Equal(0, badRowsCount);
    }

    [Fact]
    public async Task BulkCopy_InsertsKnownGoodRows()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        var goodRowsCount = await GetCountForIds(connection, BulkCopyIntegrationTestFixture.TestTable, new[] { 1, 5, 10, 15, 20 });
        Assert.Equal(5, goodRowsCount);
    }

    [Fact]
    public async Task BulkCopy_PreservesNewlinesInDescriptionColumn()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();

        var description = await GetDescriptionForId(connection, BulkCopyIntegrationTestFixture.TestTable, 5);
        Assert.NotNull(description);

        var normalized = description!.Replace("\r\n", "\n");
        Assert.Equal("Reliable\nworker", normalized);
    }

    [Fact]
    public async Task BulkCopy_PreservesQuotesInDescriptionColumn()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();

        var description = await GetDescriptionForId(connection, BulkCopyIntegrationTestFixture.TestTable, 9);
        Assert.NotNull(description);

        Assert.Equal("Consis \"tent\" ", description);
    }

    [Fact]
    public async Task BulkCopy_InsertsNullValuesForNullChar()
    {
        await using var connection = await fixture.OpenConnectionToTestDbAsync();
        await using var command = new SqlCommand($"SELECT * FROM {BulkCopyIntegrationTestFixture.TestTable} WHERE salary is null;", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var result = await reader.ReadAsync();
        Assert.True(result);
        Assert.Equal(DBNull.Value, reader["Salary"]);
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task BulkCopy_CreatesErrorTable()
    {
        await using var connection = await fixture.OpenConnectionToErrorDbAsync();
        var errorTableExists = await TableExists(connection, BulkCopyIntegrationTestFixture.ErrorTable);
        Assert.True(errorTableExists, "Error table should exist");
    }

    [Fact]
    public async Task BulkCopy_LogsExpectedNumberOfErrors()
    {
        await using var connection = await fixture.OpenConnectionToErrorDbAsync();
        var errorCount = await GetRowCount(connection, BulkCopyIntegrationTestFixture.ErrorTable);
        Assert.Equal(6, errorCount);
    }

    [Fact]
    public async Task BulkCopy_ErrorTableHasExpectedSchema()
    {
        await using var connection = await fixture.OpenConnectionToErrorDbAsync();
        var columnCount = await GetColumnCount(connection, BulkCopyIntegrationTestFixture.ErrorTable);
        Assert.Equal(8, columnCount);
    }

    [Fact]
    public async Task BulkCopy_ErrorRowsHaveExpectedRowNumbers()
    {
        await using var connection = await fixture.OpenConnectionToErrorDbAsync();
        var errorRowNumbers = await GetErrorRowNumbers(connection, BulkCopyIntegrationTestFixture.ErrorTable);
        Assert.Contains(6, errorRowNumbers);
        Assert.Contains(11, errorRowNumbers);
        Assert.Contains(16, errorRowNumbers);
        Assert.Contains(23, errorRowNumbers);
        Assert.Contains(24, errorRowNumbers);
    }

    [Fact]
    public async Task BulkCopy_ErrorRowsIncludeSourceDatabaseAndTable()
    {
        await using var connection = await fixture.OpenConnectionToErrorDbAsync();
        var sourceDbCheck = await CountRowsWithSource(
            connection,
            BulkCopyIntegrationTestFixture.ErrorTable,
            BulkCopyIntegrationTestFixture.TestDatabase,
            BulkCopyIntegrationTestFixture.TestTable);
        Assert.Equal(6, sourceDbCheck);
    }

    [Fact]
    public async Task BulkCopy_ErrorRowsIncludeCsvHeaders()
    {
        await using var connection = await fixture.OpenConnectionToErrorDbAsync();
        var headersCheck = await CountRowsWithPattern(connection, BulkCopyIntegrationTestFixture.ErrorTable, "CsvHeaders", "%ID,Name,Age%");
        Assert.Equal(6, headersCheck);
    }

    [Fact]
    public async Task BulkCopy_ErrorRowsIncludeCsvRowDataAndErrorMessages()
    {
        await using var connection = await fixture.OpenConnectionToErrorDbAsync();
        var rowDataCheck = await CountRowsWhereColumnHasLength(connection, BulkCopyIntegrationTestFixture.ErrorTable, "CsvRowData", 10);
        Assert.Equal(6, rowDataCheck);

        var errorMsgCheck = await CountRowsWhereColumnHasLength(connection, BulkCopyIntegrationTestFixture.ErrorTable, "ErrorMessage", 10);
        Assert.Equal(6, errorMsgCheck);
    }

    private static async Task<int> GetRowCount(SqlConnection connection, string tableName)
    {
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {tableName};", connection);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private static async Task<int> GetCountForIds(SqlConnection connection, string tableName, int[] ids)
    {
        var idList = string.Join(",", ids);
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {tableName} WHERE ID IN ({idList});", connection);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private static async Task<string?> GetDescriptionForId(SqlConnection connection, string tableName, int id)
    {
        await using var command = new SqlCommand($"SELECT Description FROM {tableName} WHERE ID = @Id;", connection);
        command.Parameters.AddWithValue("@Id", id);

        var result = await command.ExecuteScalarAsync();
        return result as string;
    }

    private static async Task<bool> TableExists(SqlConnection connection, string tableName)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM sys.tables WHERE name = @TableName;", connection);
        command.Parameters.AddWithValue("@TableName", tableName);
        var count = (int)(await command.ExecuteScalarAsync() ?? 0);
        return count > 0;
    }

    private static async Task<int> GetColumnCount(SqlConnection connection, string tableName)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(@TableName);", connection);
        command.Parameters.AddWithValue("@TableName", tableName);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private static async Task<List<int>> GetErrorRowNumbers(SqlConnection connection, string tableName)
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

    private static async Task<int> CountRowsWithSource(SqlConnection connection, string tableName, string sourceDb, string sourceTable)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM {tableName} WHERE SourceDatabase = @SourceDb AND SourceTable = @SourceTable;", connection);
        command.Parameters.AddWithValue("@SourceDb", sourceDb);
        command.Parameters.AddWithValue("@SourceTable", sourceTable);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private static async Task<int> CountRowsWithPattern(SqlConnection connection, string tableName, string columnName, string pattern)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM {tableName} WHERE {columnName} LIKE @Pattern;", connection);
        command.Parameters.AddWithValue("@Pattern", pattern);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }

    private static async Task<int> CountRowsWhereColumnHasLength(SqlConnection connection, string tableName, string columnName, int minLength)
    {
        await using var command = new SqlCommand(
            $"SELECT COUNT(*) FROM {tableName} WHERE LEN({columnName}) > @MinLength;", connection);
        command.Parameters.AddWithValue("@MinLength", minLength);
        return (int)(await command.ExecuteScalarAsync() ?? 0);
    }
}
