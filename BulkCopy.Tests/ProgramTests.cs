using System.Data;
using Xunit;

namespace BulkCopy.Tests;

public class ProgramTests
{
    [Fact]
    public void DataTable_WithMixedData_CreatesCorrectly()
    {
        // Arrange
        DataTable table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Age", typeof(string));
        
        // Act - Add some rows with valid and potentially problematic data
        table.Rows.Add("1", "John Doe", "30");
        table.Rows.Add("2", "Jane Smith", "25");
        table.Rows.Add("3", "Bob Johnson", "invalid_age"); // This would fail in a numeric column
        table.Rows.Add("4", "Alice Brown", "35");
        
        // Assert
        Assert.Equal(4, table.Rows.Count);
        Assert.Equal("invalid_age", table.Rows[2]["Age"]);
    }
    
    [Fact]
    public void DataTable_Clone_PreservesStructure()
    {
        // Arrange
        DataTable original = new DataTable();
        original.Columns.Add("Col1", typeof(string));
        original.Columns.Add("Col2", typeof(string));
        original.Rows.Add("A", "B");
        original.Rows.Add("C", "D");
        
        // Act
        DataTable cloned = original.Clone();
        
        // Assert
        Assert.Equal(original.Columns.Count, cloned.Columns.Count);
        Assert.Equal(0, cloned.Rows.Count); // Clone doesn't copy rows, just structure
        Assert.Equal("Col1", cloned.Columns[0].ColumnName);
        Assert.Equal("Col2", cloned.Columns[1].ColumnName);
    }
    
    [Fact]
    public void DataTable_ImportRow_CopiesRowData()
    {
        // Arrange
        DataTable source = new DataTable();
        source.Columns.Add("Col1", typeof(string));
        source.Columns.Add("Col2", typeof(string));
        source.Rows.Add("A", "B");
        source.Rows.Add("C", "D");
        
        DataTable target = source.Clone();
        
        // Act
        target.ImportRow(source.Rows[0]);
        
        // Assert
        Assert.Equal(1, target.Rows.Count);
        Assert.Equal("A", target.Rows[0]["Col1"]);
        Assert.Equal("B", target.Rows[0]["Col2"]);
    }
    
    [Fact]
    public void DataTable_BatchCreation_WorksCorrectly()
    {
        // Arrange
        DataTable source = new DataTable();
        source.Columns.Add("ID", typeof(string));
        source.Columns.Add("Value", typeof(string));
        
        for (int i = 0; i < 10; i++)
        {
            source.Rows.Add($"{i}", $"Value{i}");
        }
        
        int batchSize = 3;
        int currentRow = 0;
        
        // Act - Create first batch
        DataTable batch = source.Clone();
        int rowsInBatch = Math.Min(batchSize, source.Rows.Count - currentRow);
        
        for (int i = 0; i < rowsInBatch; i++)
        {
            batch.ImportRow(source.Rows[currentRow + i]);
        }
        
        // Assert
        Assert.Equal(3, batch.Rows.Count);
        Assert.Equal("0", batch.Rows[0]["ID"]);
        Assert.Equal("1", batch.Rows[1]["ID"]);
        Assert.Equal("2", batch.Rows[2]["ID"]);
    }
    
    [Fact]
    public void ConvertRowToCsv_SimpleData_ReturnsCorrectCsv()
    {
        // Arrange
        DataTable table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Age", typeof(string));
        table.Rows.Add("1", "John Doe", "30");
        
        // Act
        string csv = Program.ConvertRowToCsv(table.Rows[0]);
        
        // Assert
        Assert.Equal("1,John Doe,30", csv);
    }
    
    [Fact]
    public void ConvertRowToCsv_DataWithComma_QuotesValue()
    {
        // Arrange
        DataTable table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add("1", "Doe, John");
        
        // Act
        string csv = Program.ConvertRowToCsv(table.Rows[0]);
        
        // Assert
        Assert.Equal("1,\"Doe, John\"", csv);
    }
    
    [Fact]
    public void ConvertRowToCsv_DataWithQuotes_EscapesQuotes()
    {
        // Arrange
        DataTable table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Description", typeof(string));
        table.Rows.Add("1", "He said \"Hello\"");
        
        // Act
        string csv = Program.ConvertRowToCsv(table.Rows[0]);
        
        // Assert
        Assert.Equal("1,\"He said \"\"Hello\"\"\"", csv);
    }
    
    [Fact]
    public void ConvertRowToCsv_DataWithNewline_QuotesValue()
    {
        // Arrange
        DataTable table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Description", typeof(string));
        table.Rows.Add("1", "Line1\nLine2");
        
        // Act
        string csv = Program.ConvertRowToCsv(table.Rows[0]);
        
        // Assert
        Assert.Equal("1,\"Line1\nLine2\"", csv);
    }
    
    [Fact]
    public void ConvertRowToCsv_NullValue_HandlesGracefully()
    {
        // Arrange
        DataTable table = new DataTable();
        table.Columns.Add("ID", typeof(string));
        table.Columns.Add("Name", typeof(string));
        table.Rows.Add("1", DBNull.Value);
        
        // Act
        string csv = Program.ConvertRowToCsv(table.Rows[0]);
        
        // Assert
        Assert.Equal("1,", csv);
    }
}
