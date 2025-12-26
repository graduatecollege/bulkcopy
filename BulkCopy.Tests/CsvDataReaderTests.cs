using System.IO.Compression;
using System.Text;

namespace BulkCopy.Tests;

public class CsvDataReaderTests
{
    private const string JohnJaneCsv = "Name,Age,Email\nJohn Doe,30,john@example.com\nJane Smith,25,jane@example.com";
    
    private CsvDataReader CreateReader(string csvContent, string? nullValue = null)
    {
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        var reader = new StreamReader(stream);
        var csvReader = new CsvDataReader(reader, nullValue);
        csvReader.ReadHeader();
        return csvReader;
    }

    private static string CreateTempGzipFile(string csvContent, string extension)
    {
        var filePath = Path.Combine(Path.GetTempPath(), $"bulkcopy_{Guid.NewGuid():N}.csv{extension}");

        using var fileStream = File.Open(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        using var gzipStream = new GZipStream(fileStream, CompressionLevel.SmallestSize);
        using var writer = new StreamWriter(gzipStream, Encoding.UTF8);
        writer.Write(csvContent);

        return filePath;
    }

    [Fact]
    public void CsvDataReader_FieldCount_IsCorrect()
    {
        using var csvReader = CreateReader(JohnJaneCsv);

        Assert.Equal(3, csvReader.FieldCount);
    }

    [Fact]
    public void CsvDataReader_GetName_ReturnsHeaders()
    {
        using var csvReader = CreateReader(JohnJaneCsv);

        Assert.Equal("Name", csvReader.GetName(0));
        Assert.Equal("Age", csvReader.GetName(1));
        Assert.Equal("Email", csvReader.GetName(2));
    }

    [Fact]
    public void CsvDataReader_Read_ReadsRowData()
    {
        using var csvReader = CreateReader(JohnJaneCsv);

        Assert.True(csvReader.Read());
        Assert.Equal("John Doe", csvReader.GetValue(0));
        Assert.Equal("30", csvReader.GetValue(1));
        Assert.Equal("john@example.com", csvReader.GetValue(2));
    }

    [Fact]
    public void CsvDataReader_WithQuotedFields_ParsesCorrectly()
    {
        var csvContent = "ID,Name,Description\n1,Product A,\"Has, comma\"";
        using var csvReader = CreateReader(csvContent);

        csvReader.Read();

        Assert.Equal("Has, comma", csvReader.GetValue(2));
    }

    [Fact]
    public void CsvDataReader_WithNewlineInQuotedField_ReadsAsSingleField()
    {
        var csvContent = "ID,Name,Description\n1,Product A,\"Multi-line\ndescription\"";
        using var csvReader = CreateReader(csvContent);

        csvReader.Read();

        Assert.Equal("Multi-line\ndescription", csvReader.GetValue(2));
    }

    [Fact]
    public void CsvDataReader_WithNullValues_ReturnsNull()
    {
        var csvContent = "Name,Age,Email\nJohn Doe,NULL,john@example.com";
        using var csvReader = CreateReader(csvContent, "NULL");

        csvReader.Read();

        Assert.Null(csvReader.GetValue(1));
    }

    [Fact]
    public void CsvDataReader_EmptyCsv_ThrowsException()
    {
        var csvContent = "";
        Assert.Throws<FormatException>(() => CreateReader(csvContent));
    }

    [Fact]
    public void CsvDataReader_GetValue_ReturnsCorrectValue()
    {
        var csvContent = "Name,Age\nJohn,30";
        using var csvReader = CreateReader(csvContent);

        csvReader.Read();

        Assert.Equal("John", csvReader.GetValue(0));
        Assert.Equal("30", csvReader.GetValue(1));
    }

    [Fact]
    public void CsvDataReader_GetOrdinal_ReturnsCorrectIndex()
    {
        using var csvReader = CreateReader(JohnJaneCsv);

        Assert.Equal(0, csvReader.GetOrdinal("Name"));
        Assert.Equal(1, csvReader.GetOrdinal("Age"));
        Assert.Equal(2, csvReader.GetOrdinal("Email"));
    }

    [Fact]
    public void CsvDataReader_GetOrdinal_IsCaseInsensitive()
    {
        var csvContent = "Name,Age\nJohn,30";
        using var csvReader = CreateReader(csvContent);

        Assert.Equal(0, csvReader.GetOrdinal("name"));
    }

    [Fact]
    public void CsvDataReader_GetOrdinal_ThrowsForInvalidColumn()
    {
        var csvContent = "Name,Age\nJohn,30";
        using var csvReader = CreateReader(csvContent);

        Assert.Throws<IndexOutOfRangeException>(() => csvReader.GetOrdinal("InvalidColumn"));
    }

    [Fact]
    public void CsvDataReader_FewerFieldsThanHeaders_PadsWithEmptyStrings()
    {
        var csvContent = "Col1,Col2,Col3\nValue1,Value2";
        using var csvReader = CreateReader(csvContent);

        csvReader.Read();

        Assert.Equal("", csvReader.GetValue(2));
    }

    [Theory]
    [InlineData(".gz")]
    [InlineData(".gzip")]
    public void CsvDataReader_PathConstructor_LoadsGzippedCsv(string extension)
    {
        var filePath = CreateTempGzipFile(JohnJaneCsv, extension);
        try
        {
            using var csvReader = new CsvDataReader(filePath);
            csvReader.ReadHeader();

            Assert.Equal(3, csvReader.FieldCount);
            Assert.True(csvReader.Read());
            Assert.Equal("John Doe", csvReader.GetValue(0));
            Assert.Equal("30", csvReader.GetValue(1));
            Assert.Equal("john@example.com", csvReader.GetValue(2));
        }
        finally
        {
            File.Delete(filePath);
        }
    }
}