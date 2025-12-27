using Microsoft.Data.SqlClient;

namespace BulkCopy.IntegrationTests;

public static class IntegrationTests
{
    public static async Task<int> GetRowCount(SqlConnection connection, string tableName)
    {
        await using var command = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", connection);
        var value = await command.ExecuteScalarAsync();
        if (value is int count)
        {
            return count;
        }

        throw new InvalidCastException($"Cannot convert {value} to int.");
    }

    public static async Task<string?> GetNameForId(SqlConnection connection, string tableName, int id)
    {
        await using var command = new SqlCommand($"SELECT Name FROM {tableName} WHERE ID = @Id", connection);
        command.Parameters.AddWithValue("@Id", id);
        return (string?)await command.ExecuteScalarAsync();
    }
}