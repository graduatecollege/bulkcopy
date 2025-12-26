using System.Data;

namespace BulkCopy;

/// <summary>
///     Reads a limited number of rows at a time to support batch-based error handling.
/// </summary>
public sealed class BatchedCsvReader(CsvDataReader csvReader, int batchSize) : IDisposable
{
    public string[] ColumnNames => csvReader.ColumnNames;

    public bool HasMoreRows { get; private set; } = true;

    public int CurrentRowNumber { get; private set; }

    public void Dispose()
    {
        csvReader.Dispose();
    }

    /// <summary>
    ///     Reads the next batch of rows into a DataTable.
    ///     Returns null when no more rows are available.
    /// </summary>
    public DataTable? ReadNextBatch()
    {
        if (!HasMoreRows)
        {
            return null;
        }

        var dataTable = new DataTable();
        foreach (var columnName in csvReader.ColumnNames)
        {
            dataTable.Columns.Add(columnName);
        }

        var rowsRead = 0;
        while (rowsRead < batchSize && csvReader.Read())
        {
            CurrentRowNumber++;
            var row = dataTable.NewRow();
            for (var i = 0; i < csvReader.FieldCount; i++)
            {
                row[i] = csvReader.GetValue(i);
            }

            dataTable.Rows.Add(row);
            rowsRead++;
        }

        if (rowsRead == 0)
        {
            HasMoreRows = false;
            return null;
        }

        // If we filled the entire batch, assume more rows are available
        // The caller should check for null on the next call
        HasMoreRows = rowsRead == batchSize;

        return dataTable;
    }
}