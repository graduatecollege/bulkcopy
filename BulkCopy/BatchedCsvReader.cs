using System.Data;

namespace BulkCopy;

/// <summary>
///     Wraps a CsvDataReader to provide batch reading capabilities.
///     Reads a limited number of rows at a time to support batch-based error handling.
/// </summary>
public class BatchedCsvReader(CsvDataReader csvReader, int batchSize) : IDisposable
{
    public string[] ColumnNames
    {
        get
        {
            var names = new string[csvReader.FieldCount];
            for (var i = 0; i < csvReader.FieldCount; i++)
            {
                names[i] = csvReader.GetName(i);
            }

            return names;
        }
    }

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

        // Create columns from the CSV reader
        for (var i = 0; i < csvReader.FieldCount; i++)
        {
            dataTable.Columns.Add(csvReader.GetName(i));
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

    public void Close()
    {
        csvReader.Close();
    }
}