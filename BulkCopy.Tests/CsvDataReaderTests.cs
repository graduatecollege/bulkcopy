using System.Data;
using System.Text;

namespace BulkCopy.Tests;

public class CsvDataReaderTests
{
    [Fact]
    public void CsvDataReader_SimpleCsv_ReadsCorrectly()
    {
        var csvContent = "Name,Age,Email\nJohn Doe,30,john@example.com\nJane Smith,25,jane@example.com";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);

        Assert.Equal(3, csvReader.FieldCount);
        Assert.Equal("Name", csvReader.GetName(0));
        Assert.Equal("Age", csvReader.GetName(1));
        Assert.Equal("Email", csvReader.GetName(2));

        Assert.True(csvReader.Read());
        Assert.Equal("John Doe", csvReader.GetString(0));
        Assert.Equal("30", csvReader.GetString(1));
        Assert.Equal("john@example.com", csvReader.GetString(2));

        Assert.True(csvReader.Read());
        Assert.Equal("Jane Smith", csvReader.GetString(0));
        Assert.Equal("25", csvReader.GetString(1));
        Assert.Equal("jane@example.com", csvReader.GetString(2));

        Assert.False(csvReader.Read());
    }

    [Fact]
    public void CsvDataReader_WithQuotedFields_ReadsCorrectly()
    {
        var csvContent = "ID,Name,Description\n1,Product A,Simple\n2,Product B,\"Has, comma\"";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);

        Assert.Equal(3, csvReader.FieldCount);

        Assert.True(csvReader.Read());
        Assert.Equal("1", csvReader.GetString(0));
        Assert.Equal("Product A", csvReader.GetString(1));
        Assert.Equal("Simple", csvReader.GetString(2));

        Assert.True(csvReader.Read());
        Assert.Equal("2", csvReader.GetString(0));
        Assert.Equal("Product B", csvReader.GetString(1));
        Assert.Equal("Has, comma", csvReader.GetString(2));

        Assert.False(csvReader.Read());
    }

    [Fact]
    public void CsvDataReader_WithNewlineInQuotedField_ReadsCorrectly()
    {
        var csvContent = "ID,Name,Description\n1,Product A,Simple\n2,Product B,\"Multi-line\ndescription\"";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);

        Assert.True(csvReader.Read());
        Assert.Equal("Product A", csvReader.GetString(1));

        Assert.True(csvReader.Read());
        Assert.Equal("Product B", csvReader.GetString(1));
        Assert.Equal("Multi-line\ndescription", csvReader.GetString(2));

        Assert.False(csvReader.Read());
    }

    [Fact]
    public void CsvDataReader_WithNullValues_ReturnsDBNull()
    {
        var csvContent = "Name,Age,Email\nJohn Doe,NULL,john@example.com\nJane Smith,25,NULL";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader, "NULL");

        Assert.True(csvReader.Read());
        Assert.Equal("John Doe", csvReader.GetString(0));
        Assert.True(csvReader.IsDBNull(1));
        Assert.Equal("john@example.com", csvReader.GetString(2));

        Assert.True(csvReader.Read());
        Assert.Equal("Jane Smith", csvReader.GetString(0));
        Assert.Equal("25", csvReader.GetString(1));
        Assert.True(csvReader.IsDBNull(2));

        Assert.False(csvReader.Read());
    }

    [Fact]
    public void CsvDataReader_EmptyCsv_ThrowsException()
    {
        var csvContent = "";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        Assert.Throws<InvalidOperationException>(() => new CsvDataReader(reader));
    }

    [Fact]
    public void CsvDataReader_GetValue_ReturnsCorrectTypes()
    {
        var csvContent = "Name,Age\nJohn,30";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);

        Assert.True(csvReader.Read());
        var value1 = csvReader.GetValue(0);
        var value2 = csvReader.GetValue(1);

        Assert.IsType<string>(value1);
        Assert.IsType<string>(value2);
        Assert.Equal("John", value1);
        Assert.Equal("30", value2);
    }

    [Fact]
    public void CsvDataReader_GetOrdinal_ReturnsCorrectIndex()
    {
        var csvContent = "Name,Age,Email\nJohn,30,john@example.com";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);

        Assert.Equal(0, csvReader.GetOrdinal("Name"));
        Assert.Equal(1, csvReader.GetOrdinal("Age"));
        Assert.Equal(2, csvReader.GetOrdinal("Email"));
        Assert.Equal(0, csvReader.GetOrdinal("name")); // Case insensitive
    }

    [Fact]
    public void CsvDataReader_GetOrdinal_ThrowsForInvalidColumn()
    {
        var csvContent = "Name,Age\nJohn,30";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);

        Assert.Throws<IndexOutOfRangeException>(() => csvReader.GetOrdinal("InvalidColumn"));
    }

    [Fact]
    public void CsvDataReader_GetSchemaTable_ReturnsCorrectSchema()
    {
        var csvContent = "Name,Age,Email\nJohn,30,john@example.com";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);

        var schema = csvReader.GetSchemaTable();

        Assert.NotNull(schema);
        Assert.Equal(3, schema.Rows.Count);
        Assert.Equal("Name", schema.Rows[0]["ColumnName"]);
        Assert.Equal(0, schema.Rows[0]["ColumnOrdinal"]);
        Assert.Equal(typeof(string), schema.Rows[0]["DataType"]);
    }

    [Fact]
    public void CsvDataReader_FewerFieldsThanHeaders_PadsWithEmptyStrings()
    {
        var csvContent = "Col1,Col2,Col3\nValue1,Value2,Value3\nValue1,Value2";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);

        Assert.True(csvReader.Read());
        Assert.Equal("Value1", csvReader.GetString(0));
        Assert.Equal("Value2", csvReader.GetString(1));
        Assert.Equal("Value3", csvReader.GetString(2));

        Assert.True(csvReader.Read());
        Assert.Equal("Value1", csvReader.GetString(0));
        Assert.Equal("Value2", csvReader.GetString(1));
        Assert.Equal("", csvReader.GetString(2));

        Assert.False(csvReader.Read());
    }

    [Fact]
    public void CsvDataReader_CurrentRowNumber_TracksCorrectly()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3\n4,Item4";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);
        using var batchedReader = new BatchedCsvReader(csvReader, 2);

        Assert.Equal(0, batchedReader.CurrentRowNumber);

        batchedReader.ReadNextBatch();
        Assert.Equal(2, batchedReader.CurrentRowNumber);

        batchedReader.ReadNextBatch();
        Assert.Equal(4, batchedReader.CurrentRowNumber);
    }
}
