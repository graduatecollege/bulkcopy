using System.Data;
using System.Text;

namespace BulkCopy.Tests;

public class BatchedCsvReaderTests
{
    [Fact]
    public void BatchedCsvReader_ReadsSingleBatch_Correctly()
    {
        var csvContent = "Name,Age,Email\nJohn Doe,30,john@example.com\nJane Smith,25,jane@example.com";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);
        using var batchedReader = new BatchedCsvReader(csvReader, 10);

        var batch = batchedReader.ReadNextBatch();

        Assert.NotNull(batch);
        Assert.Equal(2, batch.Rows.Count);
        Assert.Equal(3, batch.Columns.Count);
        Assert.Equal("John Doe", batch.Rows[0]["Name"]);
        Assert.Equal("Jane Smith", batch.Rows[1]["Name"]);
        Assert.Equal(2, batchedReader.CurrentRowNumber);
    }

    [Fact]
    public void BatchedCsvReader_ReadsMultipleBatches_Correctly()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3\n4,Item4\n5,Item5";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);
        using var batchedReader = new BatchedCsvReader(csvReader, 2);

        // First batch
        var batch1 = batchedReader.ReadNextBatch();
        Assert.NotNull(batch1);
        Assert.Equal(2, batch1.Rows.Count);
        Assert.Equal("1", batch1.Rows[0]["ID"]);
        Assert.Equal("2", batch1.Rows[1]["ID"]);
        Assert.Equal(2, batchedReader.CurrentRowNumber);
        Assert.True(batchedReader.HasMoreRows);

        // Second batch
        var batch2 = batchedReader.ReadNextBatch();
        Assert.NotNull(batch2);
        Assert.Equal(2, batch2.Rows.Count);
        Assert.Equal("3", batch2.Rows[0]["ID"]);
        Assert.Equal("4", batch2.Rows[1]["ID"]);
        Assert.Equal(4, batchedReader.CurrentRowNumber);
        Assert.True(batchedReader.HasMoreRows);

        // Third batch (partial)
        var batch3 = batchedReader.ReadNextBatch();
        Assert.NotNull(batch3);
        Assert.Equal(1, batch3.Rows.Count);
        Assert.Equal("5", batch3.Rows[0]["ID"]);
        Assert.Equal(5, batchedReader.CurrentRowNumber);
        Assert.False(batchedReader.HasMoreRows);

        // No more batches
        var batch4 = batchedReader.ReadNextBatch();
        Assert.Null(batch4);
    }

    [Fact]
    public void BatchedCsvReader_EmptyFile_ReturnsNull()
    {
        var csvContent = "Name,Age";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);
        using var batchedReader = new BatchedCsvReader(csvReader, 10);

        var batch = batchedReader.ReadNextBatch();
        Assert.Null(batch);
        Assert.False(batchedReader.HasMoreRows);
    }

    [Fact]
    public void BatchedCsvReader_ColumnNames_ReturnsCorrectHeaders()
    {
        var csvContent = "Name,Age,Email\nJohn,30,john@example.com";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);
        using var batchedReader = new BatchedCsvReader(csvReader, 10);

        var columnNames = batchedReader.ColumnNames;

        Assert.Equal(3, columnNames.Length);
        Assert.Equal("Name", columnNames[0]);
        Assert.Equal("Age", columnNames[1]);
        Assert.Equal("Email", columnNames[2]);
    }

    [Fact]
    public void BatchedCsvReader_BatchSizeOne_ReadsOneRowAtATime()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);
        using var batchedReader = new BatchedCsvReader(csvReader, 1);

        var batch1 = batchedReader.ReadNextBatch();
        Assert.NotNull(batch1);
        Assert.Equal(1, batch1.Rows.Count);
        Assert.Equal("1", batch1.Rows[0]["ID"]);

        var batch2 = batchedReader.ReadNextBatch();
        Assert.NotNull(batch2);
        Assert.Equal(1, batch2.Rows.Count);
        Assert.Equal("2", batch2.Rows[0]["ID"]);

        var batch3 = batchedReader.ReadNextBatch();
        Assert.NotNull(batch3);
        Assert.Equal(1, batch3.Rows.Count);
        Assert.Equal("3", batch3.Rows[0]["ID"]);

        var batch4 = batchedReader.ReadNextBatch();
        Assert.Null(batch4);
    }

    [Fact]
    public void BatchedCsvReader_WithNullValues_HandlesCorrectly()
    {
        var csvContent = "Name,Age,Email\nJohn,NULL,john@example.com\nJane,25,NULL";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader, "NULL");
        using var batchedReader = new BatchedCsvReader(csvReader, 10);

        var batch = batchedReader.ReadNextBatch();

        Assert.NotNull(batch);
        Assert.Equal(2, batch.Rows.Count);
        Assert.Equal(DBNull.Value, batch.Rows[0]["Age"]);
        Assert.Equal(DBNull.Value, batch.Rows[1]["Email"]);
    }

    [Fact]
    public void BatchedCsvReader_LargeBatchSize_ReadsAllRowsInOneBatch()
    {
        var csvContent = "ID,Name\n1,Item1\n2,Item2\n3,Item3";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvContent));
        using var reader = new StreamReader(stream);
        using var csvReader = new CsvDataReader(reader);
        using var batchedReader = new BatchedCsvReader(csvReader, 1000);

        var batch1 = batchedReader.ReadNextBatch();
        Assert.NotNull(batch1);
        Assert.Equal(3, batch1.Rows.Count);
        Assert.False(batchedReader.HasMoreRows);

        var batch2 = batchedReader.ReadNextBatch();
        Assert.Null(batch2);
    }

    [Fact]
    public void BatchedCsvReader_CurrentRowNumber_TracksCorrectly()
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
