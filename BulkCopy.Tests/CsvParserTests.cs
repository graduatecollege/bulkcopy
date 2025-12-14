using System.Data;
using System.Text;
using Xunit;

namespace BulkCopy.Tests;

public class CsvParserTests
{
    [Fact]
    public void ParseCsvLine_SimpleFields_ParsesCorrectly()
    {
        // Arrange
        string line = "John Doe,30,john@example.com";

        // Act
        string[] fields = CsvParser.ParseCsvLine(line);

        // Assert
        Assert.Equal(3, fields.Length);
        Assert.Equal("John Doe", fields[0]);
        Assert.Equal("30", fields[1]);
        Assert.Equal("john@example.com", fields[2]);
    }

    [Fact]
    public void ParseCsvLine_QuotedFieldWithComma_ParsesCorrectly()
    {
        // Arrange
        string line = "Product A,\"Description with, comma\",10.99";

        // Act
        string[] fields = CsvParser.ParseCsvLine(line);

        // Assert
        Assert.Equal(3, fields.Length);
        Assert.Equal("Product A", fields[0]);
        Assert.Equal("Description with, comma", fields[1]);
        Assert.Equal("10.99", fields[2]);
    }

    [Fact]
    public void ParseCsvLine_QuotedFieldWithEscapedQuotes_ParsesCorrectly()
    {
        // Arrange
        string line = "Product A,\"Has \"\"escaped\"\" quotes\",10.99";

        // Act
        string[] fields = CsvParser.ParseCsvLine(line);

        // Assert
        Assert.Equal(3, fields.Length);
        Assert.Equal("Product A", fields[0]);
        Assert.Equal("Has \"escaped\" quotes", fields[1]);
        Assert.Equal("10.99", fields[2]);
    }

    [Fact]
    public void ExtractField_SimpleField_ReturnsUnmodified()
    {
        // Arrange
        string line = "Simple field";

        // Act
        string result = CsvParser.ExtractField(line, 0, line.Length);

        // Assert
        Assert.Equal("Simple field", result);
    }

    [Fact]
    public void ExtractField_QuotedField_RemovesQuotes()
    {
        // Arrange
        string line = "\"Quoted field\"";

        // Act
        string result = CsvParser.ExtractField(line, 0, line.Length);

        // Assert
        Assert.Equal("Quoted field", result);
    }

    [Fact]
    public void ExtractField_EscapedQuotes_UnescapesCorrectly()
    {
        // Arrange
        string line = "\"Field with \"\"escaped\"\" quotes\"";

        // Act
        string result = CsvParser.ExtractField(line, 0, line.Length);

        // Assert
        Assert.Equal("Field with \"escaped\" quotes", result);
    }

    [Fact]
    public void LoadCsvFromStream_SimpleCsv_LoadsCorrectly()
    {
        // Arrange
        string csvContent = "Name,Age,Email\nJohn Doe,30,john@example.com\nJane Smith,25,jane@example.com";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(3, result.Columns.Count);
        Assert.Equal("Name", result.Columns[0].ColumnName);
        Assert.Equal("Age", result.Columns[1].ColumnName);
        Assert.Equal("Email", result.Columns[2].ColumnName);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("John Doe", result.Rows[0]["Name"]);
        Assert.Equal("30", result.Rows[0]["Age"]);
        Assert.Equal("jane@example.com", result.Rows[1]["Email"]);
    }

    [Fact]
    public void LoadCsvFromStream_CsvWithNewlineInQuotedField_LoadsCorrectly()
    {
        // Arrange
        string csvContent = "ID,Name,Description\n1,Product A,Simple description\n2,Product B,\"Multi-line\ndescription with newline\"\n3,Product C,Another simple one";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(3, result.Columns.Count);
        Assert.Equal(3, result.Rows.Count);
        Assert.Equal("Product B", result.Rows[1]["Name"]);
        Assert.Equal("Multi-line\ndescription with newline", result.Rows[1]["Description"]);
        Assert.Equal("Product C", result.Rows[2]["Name"]);
    }

    [Fact]
    public void LoadCsvFromStream_CsvWithMultipleNewlinesInField_LoadsCorrectly()
    {
        // Arrange
        string csvContent = "ID,Name,Description,Notes\n1,Product A,Simple,No notes\n2,Product B,\"Multi-line\ndescription with\nmultiple newlines\",Still one field";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(4, result.Columns.Count);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("Multi-line\ndescription with\nmultiple newlines", result.Rows[1]["Description"]);
        Assert.Equal("Still one field", result.Rows[1]["Notes"]);
    }

    [Fact]
    public void LoadCsvFromStream_CsvWithEscapedQuotesAndNewlines_LoadsCorrectly()
    {
        // Arrange
        string csvContent = "ID,Name,Description\n1,Product A,Simple\n2,Product B,\"Has \"\"escaped\"\" quotes\"\n3,Product C,\"Multi-line\nwith \"\"quotes\"\"\"";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(3, result.Columns.Count);
        Assert.Equal(3, result.Rows.Count);
        Assert.Equal("Has \"escaped\" quotes", result.Rows[1]["Description"]);
        Assert.Equal("Multi-line\nwith \"quotes\"", result.Rows[2]["Description"]);
    }

    [Fact]
    public void LoadCsvFromStream_ComplexCsvWithAllFeatures_LoadsCorrectly()
    {
        // Arrange
        string csvContent = "ID,Name,Description,Notes\n" +
                          "1,Product A,Simple description,No notes\n" +
                          "2,Product B,\"Multi-line\ndescription with\nmultiple newlines\",Still one field\n" +
                          "3,Product C,\"Field with, comma\",Normal\n" +
                          "4,Product D,\"Field with \"\"quotes\"\"\",Normal\n" +
                          "5,\"Product E\",\"Has newline\nand comma, together\",\"Complex\nfield\"";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(4, result.Columns.Count);
        Assert.Equal(5, result.Rows.Count);
        
        // Row 1: Simple
        Assert.Equal("Product A", result.Rows[0]["Name"]);
        Assert.Equal("Simple description", result.Rows[0]["Description"]);
        
        // Row 2: Multiple newlines
        Assert.Equal("Multi-line\ndescription with\nmultiple newlines", result.Rows[1]["Description"]);
        
        // Row 3: Comma in field
        Assert.Equal("Field with, comma", result.Rows[2]["Description"]);
        
        // Row 4: Quotes in field
        Assert.Equal("Field with \"quotes\"", result.Rows[3]["Description"]);
        
        // Row 5: Complex combination
        Assert.Equal("Product E", result.Rows[4]["Name"]);
        Assert.Equal("Has newline\nand comma, together", result.Rows[4]["Description"]);
        Assert.Equal("Complex\nfield", result.Rows[4]["Notes"]);
    }

    [Fact]
    public void LoadCsvFromStream_WindowsLineEndings_LoadsCorrectly()
    {
        // Arrange
        string csvContent = "Name,Age\r\nJohn,30\r\nJane,25";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(2, result.Columns.Count);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("John", result.Rows[0]["Name"]);
        Assert.Equal("Jane", result.Rows[1]["Name"]);
    }

    [Fact]
    public void LoadCsvFromStream_MacLineEndings_LoadsCorrectly()
    {
        // Arrange
        string csvContent = "Name,Age\rJohn,30\rJane,25";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(2, result.Columns.Count);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("John", result.Rows[0]["Name"]);
        Assert.Equal("Jane", result.Rows[1]["Name"]);
    }

    [Fact]
    public void LoadCsvFromStream_FewerFieldsThanHeaders_PadsWithEmptyStrings()
    {
        // Arrange
        string csvContent = "Col1,Col2,Col3\nValue1,Value2,Value3\nValue1,Value2";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(3, result.Columns.Count);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("Value1", result.Rows[1]["Col1"]);
        Assert.Equal("Value2", result.Rows[1]["Col2"]);
        Assert.Equal("", result.Rows[1]["Col3"]);
    }

    [Fact]
    public void LoadCsvFromStream_EmptyCsv_ThrowsException()
    {
        // Arrange
        string csvContent = "";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => CsvParser.LoadCsvFromStream(reader));
    }

    [Fact]
    public void ReadCsvRow_SimpleRow_ReadsCorrectly()
    {
        // Arrange
        string csvContent = "field1,field2,field3\nnextrow";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        string? row1 = CsvParser.ReadCsvRow(reader);
        string? row2 = CsvParser.ReadCsvRow(reader);

        // Assert
        Assert.Equal("field1,field2,field3", row1);
        Assert.Equal("nextrow", row2);
    }

    [Fact]
    public void ReadCsvRow_RowWithNewlineInQuotes_ReadsAsSingleRow()
    {
        // Arrange
        string csvContent = "field1,\"field2\nwith newline\",field3\nnextrow";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        string? row1 = CsvParser.ReadCsvRow(reader);
        string? row2 = CsvParser.ReadCsvRow(reader);

        // Assert
        Assert.Equal("field1,\"field2\nwith newline\",field3", row1);
        Assert.Equal("nextrow", row2);
    }

    [Fact]
    public void ReadCsvRow_RowWithEscapedQuotes_ReadsCorrectly()
    {
        // Arrange
        string csvContent = "field1,\"field2 with \"\"quotes\"\"\",field3";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        string? row = CsvParser.ReadCsvRow(reader);

        // Assert
        Assert.Equal("field1,\"field2 with \"\"quotes\"\"\",field3", row);
    }

    [Fact]
    public void ExtractField_DefaultNullCharacter_ReturnsNull()
    {
        // Arrange
        string line = "\0";

        // Act
        string? result = CsvParser.ExtractField(line, 0, line.Length, "\0");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void ExtractField_CustomNullCharacter_ReturnsNull()
    {
        // Arrange
        string line = "NULL";

        // Act
        string? result = CsvParser.ExtractField(line, 0, line.Length, "NULL");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void ExtractField_QuotedNullCharacter_ReturnsValue()
    {
        // Arrange
        string line = "\"NULL\"";

        // Act
        string? result = CsvParser.ExtractField(line, 0, line.Length, "NULL");

        // Assert
        Assert.Equal("NULL", result);
    }

    [Fact]
    public void ParseCsvLine_WithNullCharacter_ReturnsNullForMatchingFields()
    {
        // Arrange
        string line = "Value1,NULL,Value3";

        // Act
        string?[] fields = CsvParser.ParseCsvLine(line, "NULL");

        // Assert
        Assert.Equal(3, fields.Length);
        Assert.Equal("Value1", fields[0]);
        Assert.Null(fields[1]);
        Assert.Equal("Value3", fields[2]);
    }

    [Fact]
    public void ParseCsvLine_WithQuotedNullCharacter_ReturnsValue()
    {
        // Arrange
        string line = "Value1,\"NULL\",Value3";

        // Act
        string?[] fields = CsvParser.ParseCsvLine(line, "NULL");

        // Assert
        Assert.Equal(3, fields.Length);
        Assert.Equal("Value1", fields[0]);
        Assert.Equal("NULL", fields[1]);
        Assert.Equal("Value3", fields[2]);
    }

    [Fact]
    public void LoadCsvFromStream_WithNullCharacter_LoadsAsDBNull()
    {
        // Arrange
        string csvContent = "Name,Age,Email\nJohn Doe,NULL,john@example.com\nJane Smith,25,NULL";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader, "NULL");

        // Assert
        Assert.Equal(3, result.Columns.Count);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("John Doe", result.Rows[0]["Name"]);
        Assert.Equal(DBNull.Value, result.Rows[0]["Age"]);
        Assert.Equal("john@example.com", result.Rows[0]["Email"]);
        Assert.Equal("Jane Smith", result.Rows[1]["Name"]);
        Assert.Equal("25", result.Rows[1]["Age"]);
        Assert.Equal(DBNull.Value, result.Rows[1]["Email"]);
    }

    [Fact]
    public void LoadCsvFromStream_WithDefaultNullCharacter_LoadsAsDBNull()
    {
        // Arrange
        string csvContent = "Name,Age,Email\nJohn Doe,\0,john@example.com\nJane Smith,25,\0";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);

        // Act
        DataTable result = CsvParser.LoadCsvFromStream(reader);

        // Assert
        Assert.Equal(3, result.Columns.Count);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("John Doe", result.Rows[0]["Name"]);
        Assert.Equal(DBNull.Value, result.Rows[0]["Age"]);
        Assert.Equal("john@example.com", result.Rows[0]["Email"]);
        Assert.Equal("Jane Smith", result.Rows[1]["Name"]);
        Assert.Equal("25", result.Rows[1]["Age"]);
        Assert.Equal(DBNull.Value, result.Rows[1]["Email"]);
    }

    [Fact]
    public void ParseCsvLine_WithNoNullChar_DoesNotConvertNulls()
    {
        // Arrange
        string line = "Value1,NULL,Value3";

        // Act - passing null as nullChar disables null conversion
        string?[] fields = CsvParser.ParseCsvLine(line, null);

        // Assert
        Assert.Equal(3, fields.Length);
        Assert.Equal("Value1", fields[0]);
        Assert.Equal("NULL", fields[1]);
        Assert.Equal("Value3", fields[2]);
    }
}
