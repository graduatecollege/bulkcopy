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
            var result = Program.ResolveConnectionString("./" + filename);
            Assert.Equal(expected, result);
            
            result = Program.ResolveConnectionString(".\\" + filename);
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
        var input = "./non-existent-file.txt";
        Assert.Throws<ArgumentException>(() => Program.ResolveConnectionString(input));
    }
}