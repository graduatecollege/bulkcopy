using System.Data;

namespace BulkCopy;

/// <summary>
/// Wraps a CsvDataReader to provide batch reading capabilities.
/// Reads a limited number of rows at a time to support batch-based error handling.
/// </summary>
public class BatchedCsvReader : IDisposable
{
    private readonly CsvDataReader _csvReader;
    private readonly int _batchSize;
    private bool _hasMoreRows = true;
    private int _currentRowNumber = 0;

    public BatchedCsvReader(CsvDataReader csvReader, int batchSize)
    {
        _csvReader = csvReader;
        _batchSize = batchSize;
    }

    public int FieldCount => _csvReader.FieldCount;

    public string[] ColumnNames
    {
        get
        {
            var names = new string[_csvReader.FieldCount];
            for (int i = 0; i < _csvReader.FieldCount; i++)
            {
                names[i] = _csvReader.GetName(i);
            }
            return names;
        }
    }

    public bool HasMoreRows => _hasMoreRows;

    public int CurrentRowNumber => _currentRowNumber;

    /// <summary>
    /// Reads the next batch of rows into a DataTable.
    /// Returns null when no more rows are available.
    /// </summary>
    public DataTable? ReadNextBatch()
    {
        if (!_hasMoreRows)
            return null;

        var dataTable = new DataTable();

        // Create columns from the CSV reader
        for (int i = 0; i < _csvReader.FieldCount; i++)
        {
            dataTable.Columns.Add(_csvReader.GetName(i));
        }

        int rowsRead = 0;
        while (rowsRead < _batchSize && _csvReader.Read())
        {
            _currentRowNumber++;
            var row = dataTable.NewRow();
            for (int i = 0; i < _csvReader.FieldCount; i++)
            {
                row[i] = _csvReader.GetValue(i);
            }
            dataTable.Rows.Add(row);
            rowsRead++;
        }

        if (rowsRead == 0)
        {
            _hasMoreRows = false;
            return null;
        }

        // Check if there are more rows
        _hasMoreRows = rowsRead == _batchSize; // Assume more rows if we filled the batch

        return dataTable;
    }

    public void Close()
    {
        _csvReader.Close();
    }

    public void Dispose()
    {
        _csvReader.Dispose();
    }
}
