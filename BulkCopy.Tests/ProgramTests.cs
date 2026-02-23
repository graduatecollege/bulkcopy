using System.Data;

namespace BulkCopy.Tests;

public class ProgramTests
{
    [Fact]
    public void ConvertRowToCsv_SimpleData_ReturnsCorrectCsv()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Age", typeof(string));
        table.Rows.Add("1", "John Doe", "30");

        var csv = Program.ConvertRowToCsv(table.Rows[0]);

        Assert.Equal("1,John Doe,30", csv);
    }

    [Fact]
    public void ConvertRowToCsv_DataWithComma_QuotesValue()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add("1", "Doe, John");

        var csv = Program.ConvertRowToCsv(table.Rows[0]);

        Assert.Equal("1,\"Doe, John\"", csv);
    }

    [Fact]
    public void ConvertRowToCsv_DataWithQuotes_EscapesQuotes()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Description", typeof(string));
        table.Rows.Add("1", "He said \"Hello\"");

        var csv = Program.ConvertRowToCsv(table.Rows[0]);

        Assert.Equal("1,\"He said \"\"Hello\"\"\"", csv);
    }

    [Fact]
    public void ConvertRowToCsv_DataWithNewline_QuotesValue()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Description", typeof(string));
        table.Rows.Add("1", "Line1\nLine2");

        var csv = Program.ConvertRowToCsv(table.Rows[0]);

        Assert.Equal("1,\"Line1\nLine2\"", csv);
    }

    [Fact]
    public void ConvertRowToCsv_NullValue_HandlesGracefully()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add("1", DBNull.Value);

        var csv = Program.ConvertRowToCsv(table.Rows[0]);

        Assert.Equal("1,", csv);
    }

    [Fact]
    public void SanitizeSqlIdentifier_ValidIdentifier_ReturnsUnchanged()
    {
        var result = Program.SanitizeSqlIdentifier("MyDatabase");

        Assert.Equal("MyDatabase", result);
    }

    [Fact]
    public void SanitizeSqlIdentifier_ValidIdentifierWithUnderscore_ReturnsUnchanged()
    {
        var result = Program.SanitizeSqlIdentifier("My_Database_123");

        Assert.Equal("My_Database_123", result);
    }

    [Fact]
    public void SanitizeSqlIdentifier_IdentifierWithBrackets_RemovesBrackets()
    {
        var result = Program.SanitizeSqlIdentifier("[MyDatabase]");

        Assert.Equal("MyDatabase", result);
    }

    [Fact]
    public void SanitizeSqlIdentifier_SqlInjectionAttempt_ThrowsException()
    {
        Assert.Throws<ArgumentException>(() =>
            Program.SanitizeSqlIdentifier("mydb]; DROP TABLE users; --"));
    }

    [Fact]
    public void SanitizeSqlIdentifier_EmptyString_ThrowsException()
    {
        Assert.Throws<ArgumentException>(() =>
            Program.SanitizeSqlIdentifier(""));
    }

    [Fact]
    public void SanitizeSqlIdentifier_IdentifierWithSpaces_ThrowsException()
    {
        Assert.Throws<ArgumentException>(() =>
            Program.SanitizeSqlIdentifier("My Database"));
    }

    [Fact]
    public void SanitizeSqlIdentifier_IdentifierStartingWithNumber_ThrowsException()
    {
        Assert.Throws<ArgumentException>(() =>
            Program.SanitizeSqlIdentifier("123Database"));
    }

    [Fact]
    public void ResolveConnectionString_LiteralString_ReturnsUnchanged()
    {
        var input = "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;";
        var result = Program.ResolveConnectionString(input);
        Assert.Equal(input, result);
    }

    [Fact]
    public void ResolveConnectionString_FilePath_ReadsFromFile()
    {
        var tempFile = Path.GetTempFileName();
        var expected = "Server=myServerFromFile;";
        try
        {
            File.WriteAllText(tempFile, expected);
            
            var result = Program.ResolveConnectionString(tempFile);
            Assert.Equal(expected, result);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void ResolveConnectionString_RelativeFilePath_ReadsFromFile()
    {
        var filename = "test_conn.txt";
        var expected = "Server=myRelativeServer;";
        File.WriteAllText(filename, expected);
        try
        {
            var result = Program.ResolveConnectionString(Path.Join(".", filename));
            Assert.Equal(expected, result);
            
            result = Program.ResolveConnectionString(Path.Join(".", filename));
            Assert.Equal(expected, result);
        }
        finally
        {
            File.Delete(filename);
        }
    }

    [Fact]
    public void ResolveConnectionString_NonExistentFilePath_ThrowsException()
    {
        var input = Path.Join(".", "nonexistent.txt");
        Assert.Throws<ArgumentException>(() => Program.ResolveConnectionString(input));
    }

    [Fact]
    public void ConvertBitColumnsInDataTable_WithZeroString_ConvertsToNumericZero()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("IsActive", typeof(string));
        table.Rows.Add("1", "0");

        var columnMetadata = new List<ColumnInfo>
        {
            new("ID", "int"),
            new("IsActive", "bit")
        };
        var result = Program.ConvertBitColumnsInDataTable(table, columnMetadata);

        var value = result.Rows[0]["IsActive"];
        Assert.IsType<int>(value);
        Assert.Equal(0, value);
    }

    [Fact]
    public void ConvertBitColumnsInDataTable_WithOneString_ConvertsToNumericOne()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("IsActive", typeof(string));
        table.Rows.Add("1", "1");

        var columnMetadata = new List<ColumnInfo>
        {
            new("ID", "int"),
            new("IsActive", "bit")
        };
        var result = Program.ConvertBitColumnsInDataTable(table, columnMetadata);

        var value = result.Rows[0]["IsActive"];
        Assert.IsType<int>(value);
        Assert.Equal(1, value);
    }

    [Fact]
    public void ConvertBitColumnsInDataTable_WithOtherString_LeavesUnchanged()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("IsActive", typeof(string));
        table.Rows.Add("1", "true");

        var columnMetadata = new List<ColumnInfo>
        {
            new("ID", "int"),
            new("IsActive", "bit")
        };
        var result = Program.ConvertBitColumnsInDataTable(table, columnMetadata);

        Assert.Equal("true", result.Rows[0]["IsActive"]);
    }

    [Fact]
    public void ConvertBitColumnsInDataTable_WithMultipleBitColumns_ConvertsAll()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("IsActive", typeof(string));
        table.Columns.Add("IsDeleted", typeof(string));
        table.Rows.Add("1", "1", "0");

        var columnMetadata = new List<ColumnInfo>
        {
            new("ID", "int"),
            new("IsActive", "bit"),
            new("IsDeleted", "bit")
        };
        var result = Program.ConvertBitColumnsInDataTable(table, columnMetadata);

        var isActiveValue = result.Rows[0]["IsActive"];
        var isDeletedValue = result.Rows[0]["IsDeleted"];
        Assert.IsType<int>(isActiveValue);
        Assert.IsType<int>(isDeletedValue);
        Assert.Equal(1, isActiveValue);
        Assert.Equal(0, isDeletedValue);
    }

    [Fact]
    public void ConvertBitColumnsInDataTable_WithEmptyBitColumns_ReturnsOriginalTable()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("IsActive", typeof(string));
        table.Rows.Add("1", "1");

        var columnMetadata = new List<ColumnInfo>
        {
            new("ID", "int"),
            new("IsActive", "nvarchar")
        };
        var result = Program.ConvertBitColumnsInDataTable(table, columnMetadata);

        Assert.Same(table, result);
        Assert.Equal("1", result.Rows[0]["IsActive"]);
    }

    [Fact]
    public void ConvertBitColumnsInDataTable_WithNonStringValue_LeavesUnchanged()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("IsActive", typeof(int));
        table.Rows.Add("1", 42);

        var columnMetadata = new List<ColumnInfo>
        {
            new("ID", "int"),
            new("IsActive", "bit")
        };
        var result = Program.ConvertBitColumnsInDataTable(table, columnMetadata);

        Assert.Equal(42, result.Rows[0]["IsActive"]);
    }

    [Fact]
    public void ConvertBitColumnsInDataTable_CaseInsensitiveColumnName_ConvertsCorrectly()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("IsActive", typeof(string));
        table.Rows.Add("1", "1");

        var columnMetadata = new List<ColumnInfo>
        {
            new("ID", "int"),
            new("isactive", "bit")  // lowercase column name
        };
        var result = Program.ConvertBitColumnsInDataTable(table, columnMetadata);

        var value = result.Rows[0]["IsActive"];
        Assert.IsType<int>(value);
        Assert.Equal(1, value);
    }

    [Fact]
    public void ConvertBitColumnsInDataTable_WithMultipleRows_ConvertsAll()
    {
        var table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("IsActive", typeof(string));
        table.Rows.Add("1", "1");
        table.Rows.Add("2", "0");
        table.Rows.Add("3", "invalid");

        var columnMetadata = new List<ColumnInfo>
        {
            new("ID", "int"),
            new("IsActive", "bit")
        };
        var result = Program.ConvertBitColumnsInDataTable(table, columnMetadata);

        var value1 = result.Rows[0]["IsActive"];
        var value2 = result.Rows[1]["IsActive"];
        var value3 = result.Rows[2]["IsActive"];
        Assert.IsType<int>(value1);
        Assert.IsType<int>(value2);
        Assert.IsType<string>(value3);
        Assert.Equal(1, value1);
        Assert.Equal(0, value2);
        Assert.Equal("invalid", value3);
    }
}