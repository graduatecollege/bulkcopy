using System.Text;

namespace BulkCopy.Tests;

public class CsvParserTests
{
    private StreamReader CreateReader(string csvContent, Encoding? encoding = null)
    {
        var stream = new MemoryStream((encoding ?? Encoding.UTF8).GetBytes(csvContent));
        return new StreamReader(stream, encoding ?? Encoding.UTF8);
    }

    [Fact]
    public void ParseCsvLine_SimpleFields_ParsesCorrectly()
    {
        var fields = CsvParser.ParseCsvLine("John Doe,30,john@example.com");

        Assert.Equal(new[] { "John Doe", "30", "john@example.com" }, fields);
    }

    [Fact]
    public void ParseCsvLine_QuotedFieldWithComma_ParsesCorrectly()
    { 
        var fields = CsvParser.ParseCsvLine("Product A,\"Description with, comma\",10.99");

        Assert.Equal("Description with, comma", fields[1]);
    }

    [Fact]
    public void ParseCsvLine_QuotedFieldWithEscapedQuotes_ParsesCorrectly()
    {
        var fields = CsvParser.ParseCsvLine("Product A,\"Has \"\"escaped\"\" quotes\",10.99");

        Assert.Equal("Has \"escaped\" quotes", fields[1]);
    }

    [Fact]
    public void ExtractField_SimpleField_ReturnsUnmodified()
    {
        var line = "Simple field";

        var result = CsvParser.ExtractField(line, 0, line.Length);

        Assert.Equal("Simple field", result);
    }

    [Fact]
    public void ExtractField_QuotedField_RemovesQuotes()
    {
        var line = "\"Quoted field\"";

        var result = CsvParser.ExtractField(line, 0, line.Length);

        Assert.Equal("Quoted field", result);
    }

    [Fact]
    public void ExtractField_EscapedQuotes_UnescapesCorrectly()
    {
        var line = "\"Field with \"\"escaped\"\" quotes\"";

        var result = CsvParser.ExtractField(line, 0, line.Length);

        Assert.Equal("Field with \"escaped\" quotes", result);
    }

    [Fact]
    public void LoadCsvFromStream_SimpleCsv_SetsColumns()
    { 
        using var reader = CreateReader("Name,Age,Email\nJohn Doe,30,john@example.com");

        var result = CsvParser.LoadCsvFromStream(reader);

        Assert.Equal(3, result.Columns.Count);
        Assert.Equal("Name", result.Columns[0].ColumnName);
    }

    [Fact]
    public void LoadCsvFromStream_SimpleCsv_SetsRows()
    {
        using var reader = CreateReader("Name,Age,Email\nJohn Doe,30,john@example.com");

        var result = CsvParser.LoadCsvFromStream(reader);

        Assert.Equal(1, result.Rows.Count);
        Assert.Equal("John Doe", result.Rows[0]["Name"]);
    }

    [Fact]
    public void LoadCsvFromStream_CsvWithNewlineInQuotedField_LoadsCorrectly()
    { 
        using var reader = CreateReader("ID,Name,Description\n1,Product B,\"Multi-line\ndescription\"");

        var result = CsvParser.LoadCsvFromStream(reader);

        Assert.Equal("Multi-line\ndescription", result.Rows[0]["Description"]);
    }

    [Fact]
    public void LoadCsvFromStream_ComplexCsv_LoadsCorrectly()
    {
        using var reader = CreateReader("ID,Name,Description\n" +
                                        "2,Product B,\"Multi-line\ndescription\"\n" +
                                        "3,Product C,\"Field with, comma\"\n" +
                                        "4,Product D,\"Field with \"\"quotes\"\"\"");

        var result = CsvParser.LoadCsvFromStream(reader);

        Assert.Equal(3, result.Rows.Count);
        Assert.Equal("Multi-line\ndescription", result.Rows[0]["Description"]);
        Assert.Equal("Field with, comma", result.Rows[1]["Description"]);
        Assert.Equal("Field with \"quotes\"", result.Rows[2]["Description"]);
    }

    [Fact]
    public void LoadCsvFromStream_WindowsLineEndings_LoadsCorrectly()
    { 
        using var reader = CreateReader("Name,Age\r\nJohn,30");

        var result = CsvParser.LoadCsvFromStream(reader);

        Assert.Equal("John", result.Rows[0]["Name"]);
    }

    [Fact]
    public void LoadCsvFromStream_MacLineEndings_LoadsCorrectly()
    { 
        using var reader = CreateReader("Name,Age\rJohn,30");

        var result = CsvParser.LoadCsvFromStream(reader);

        Assert.Equal("John", result.Rows[0]["Name"]);
    }

    [Fact]
    public void LoadCsvFromStream_FewerFieldsThanHeaders_PadsWithEmptyStrings()
    {
        using var reader = CreateReader("Col1,Col2,Col3\nValue1,Value2");

        var result = CsvParser.LoadCsvFromStream(reader);

        Assert.Equal("", result.Rows[0]["Col3"]);
    }

    [Fact]
    public void LoadCsvFromStream_EmptyCsv_ThrowsException()
    {
        using var reader = CreateReader("");
        Assert.Throws<InvalidOperationException>(() => CsvParser.LoadCsvFromStream(reader));
    }

    [Fact]
    public void ReadCsvRow_SimpleRow_ReadsLine()
    {
        using var reader = CreateReader("field1,field2\nnextrow");

        Assert.Equal("field1,field2", CsvParser.ReadCsvRow(reader));
        Assert.Equal("nextrow", CsvParser.ReadCsvRow(reader));
    }

    [Fact]
    public void ReadCsvRow_RowWithNewlineInQuotes_ReadsAsSingleRow()
    {
        using var reader = CreateReader("field1,\"field2\nwith newline\"\nnextrow");

        Assert.Equal("field1,\"field2\nwith newline\"", CsvParser.ReadCsvRow(reader));
        Assert.Equal("nextrow", CsvParser.ReadCsvRow(reader));
    }

    [Fact]
    public void ExtractField_CustomNullCharacter_ReturnsNull()
    {
        var line = "NULL";
        Assert.Null(CsvParser.ExtractField(line, 0, line.Length, "NULL"));
    }

    [Fact]
    public void ParseCsvLine_WithNullCharacter_ReturnsNull()
    {
        var line = "Value1,NULL,Value3";
        var fields = CsvParser.ParseCsvLine(line, "NULL");
        Assert.Null(fields[1]);
    }

    [Fact]
    public void LoadCsvFromStream_WithNullCharacter_LoadsAsDBNull()
    {
        using var reader = CreateReader("Name,Age\nJohn Doe,NULL");

        var result = CsvParser.LoadCsvFromStream(reader, "NULL");

        Assert.Equal(DBNull.Value, result.Rows[0]["Age"]);
    }

    [Fact]
    public void LoadCsvFromStream_Utf8Fields_LoadsCorrectly()
    {
        using var reader = CreateReader("Name,City,Emoji\nJosé,São Paulo,😀");

        var result = CsvParser.LoadCsvFromStream(reader);

        Assert.Equal("José", result.Rows[0]["Name"]);
        Assert.Equal("São Paulo", result.Rows[0]["City"]);
        Assert.Equal("😀", result.Rows[0]["Emoji"]);
    }
}