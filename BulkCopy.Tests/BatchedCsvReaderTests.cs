using System.Text;

namespace BulkCopy.Tests;

public class BatchedCsvReaderTests
{
    private const string JohnJaneCsv = "Name,Age,Email\nJohn Doe,30,john@example.com\nJane Smith,25,jane@example.com";

    private BatchedCsvReader CreateReader(string csvContent, int batchSize = 10, string? nullValue = null)
    {
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        var reader = new StreamReader(stream);
        var csvReader = new CsvDataReader(reader, nullValue);
        return new BatchedCsvReader(csvReader, batchSize);
    }

    [Fact]
    public void BatchedCsvReader_ReadsSingleBatch()
    {
        using var batchedReader = CreateReader(JohnJaneCsv);

        var batch = batchedReader.ReadNextBatch();

        Assert.NotNull(batch);
        Assert.Equal(2, batch.Rows.Count);
    }

    [Fact]
    public void BatchedCsvReader_ReadsSingleBatch_SetsCorrectNames()
    {
        using var batchedReader = CreateReader(JohnJaneCsv);

        var batch = batchedReader.ReadNextBatch();

        Assert.Equal("John Doe", batch!.Rows[0]["Name"]);
        Assert.Equal("Jane Smith", batch.Rows[1]["Name"]);
    }
    
    [Fact]
    public void BatchedCsvReader_ReadsSingleBatch_SetsCorrectAges()
    {
        using var batchedReader = CreateReader(JohnJaneCsv);

        var batch = batchedReader.ReadNextBatch();

        Assert.Equal("30", batch!.Rows[0]["Age"]);
        Assert.Equal("25", batch.Rows[1]["Age"]);
    }
    
    [Fact]
    public void BatchedCsvReader_ReadsSingleBatch_SetsCorrectEmails()
    {
        using var batchedReader = CreateReader(JohnJaneCsv);

        var batch = batchedReader.ReadNextBatch();

        Assert.Equal("john@example.com", batch!.Rows[0]["Email"]);
        Assert.Equal("jane@example.com", batch.Rows[1]["Email"]);
    }

    [Fact]
    public void BatchedCsvReader_ReadsSingleBatch_UpdatesCurrentRowNumber()
    {
        using var batchedReader = CreateReader(JohnJaneCsv);

        batchedReader.ReadNextBatch();

        Assert.Equal(2, batchedReader.CurrentRowNumber);
    }

    [Fact]
    public void BatchedCsvReader_FirstBatch_IsCorrect()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3\n4,Item4\n5,Item5";
        using var batchedReader = CreateReader(csvContent, 2);

        var batch = batchedReader.ReadNextBatch();

        Assert.Equal(2, batch!.Rows.Count);
        Assert.Equal("1", batch.Rows[0]["ID"]);
        Assert.Equal("2", batch.Rows[1]["ID"]);
    }

    [Fact]
    public void BatchedCsvReader_MiddleBatch_IsCorrect()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3\n4,Item4\n5,Item5";
        using var batchedReader = CreateReader(csvContent, 2);

        batchedReader.ReadNextBatch(); // Skip first
        var batch = batchedReader.ReadNextBatch();

        Assert.Equal(2, batch!.Rows.Count);
        Assert.Equal("3", batch.Rows[0]["ID"]);
        Assert.Equal("4", batch.Rows[1]["ID"]);
    }

    [Fact]
    public void BatchedCsvReader_LastPartialBatch_IsCorrect()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3\n4,Item4\n5,Item5";
        using var batchedReader = CreateReader(csvContent, 2);

        batchedReader.ReadNextBatch(); // 1-2
        batchedReader.ReadNextBatch(); // 3-4
        var batch = batchedReader.ReadNextBatch();

        Assert.Equal(1, batch!.Rows.Count);
        Assert.Equal("5", batch.Rows[0]["ID"]);
        Assert.False(batchedReader.HasMoreRows);
    }

    [Fact]
    public void BatchedCsvReader_EmptyFile_ReturnsNull()
    {
        var csvContent = "Name,Age";
        using var batchedReader = CreateReader(csvContent);

        var batch = batchedReader.ReadNextBatch();

        Assert.Null(batch);
        Assert.False(batchedReader.HasMoreRows);
    }

    [Fact]
    public void BatchedCsvReader_ColumnNames_ReturnsCorrectHeaders()
    {
        var csvContent = "Name,Age,Email\nJohn,30,john@example.com";
        using var batchedReader = CreateReader(csvContent);

        var columnNames = batchedReader.ColumnNames;

        Assert.Equal(new[] { "Name", "Age", "Email" }, columnNames);
    }

    [Fact]
    public void BatchedCsvReader_BatchSizeOne_ReadsAllRows()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3";
        using var batchedReader = CreateReader(csvContent, 1);

        Assert.NotNull(batchedReader.ReadNextBatch());
        Assert.NotNull(batchedReader.ReadNextBatch());
        Assert.NotNull(batchedReader.ReadNextBatch());
        Assert.Null(batchedReader.ReadNextBatch());
    }

    [Fact]
    public void BatchedCsvReader_WithNullValues_HandlesCorrectly()
    {
        var csvContent = "Name,Age,Email\nJohn,NULL,john@example.com\nJane,25,NULL";
        using var batchedReader = CreateReader(csvContent, 10, "NULL");

        var batch = batchedReader.ReadNextBatch();

        Assert.Equal(DBNull.Value, batch!.Rows[0]["Age"]);
        Assert.Equal(DBNull.Value, batch.Rows[1]["Email"]);
    }

    [Fact]
    public void BatchedCsvReader_LargeBatchSize_ReadsAllRowsInOneBatch()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3";
        using var batchedReader = CreateReader(csvContent, 1000);

        var batch = batchedReader.ReadNextBatch();

        Assert.Equal(3, batch!.Rows.Count);
        Assert.False(batchedReader.HasMoreRows);
    }

    [Fact]
    public void BatchedCsvReader_CurrentRowNumber_InitialIsZero()
    {
        var csvContent = "ID,Name\n1,Item1";
        using var batchedReader = CreateReader(csvContent);

        Assert.Equal(0, batchedReader.CurrentRowNumber);
    }

    [Fact]
    public void BatchedCsvReader_CurrentRowNumber_TracksCorrectlyAfterRead()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3\n4,Item4";
        using var batchedReader = CreateReader(csvContent, 2);

        batchedReader.ReadNextBatch();
        Assert.Equal(2, batchedReader.CurrentRowNumber);

        batchedReader.ReadNextBatch();
        Assert.Equal(4, batchedReader.CurrentRowNumber);
    }
}