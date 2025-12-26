using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO.Compression;
using System.Text;

namespace BulkCopy;

/// <summary>
///     A streaming data reader for CSV files.
/// </summary>
public sealed class CsvDataReader(StreamReader reader, string? nullChar = "␀") : IDisposable
{
    private string[] _columnNames = [];
    private bool _headerRead;
    private string?[]? _currentRow;

    public CsvDataReader(string filePath, string? nullChar = "␀")
        : this(OpenFileAsStreamReader(filePath), nullChar)
    {
    }

    private static StreamReader OpenFileAsStreamReader(string filePath)
    {
        var fileStream = File.OpenRead(filePath);

        if (filePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase) ||
            filePath.EndsWith(".gzip", StringComparison.OrdinalIgnoreCase))
        {
            var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
            return new StreamReader(gzipStream, Encoding.UTF8);
        }

        return new StreamReader(fileStream, Encoding.UTF8);
    }

    public int FieldCount => !_headerRead ? throw new InvalidOperationException("Header must be read before accessing field count.") : _columnNames.Length;
    
    public string[] ColumnNames => !_headerRead ? throw new InvalidOperationException("Header must be read before accessing column names.") : _columnNames;

    public bool IsClosed { get; private set; }

    public void ReadHeader()
    {
        if (_headerRead)
        {
            throw new InvalidOperationException("Header has already been read.");
        }

        var headerRow = CsvParser.ReadCsvRow(reader);
        if (string.IsNullOrEmpty(headerRow))
        {
            throw new FormatException("CSV file is empty or has no header row.");
        }

        _columnNames = CsvParser.ParseCsvLine(headerRow, null)
            .Select(h => h?.Trim() ?? "")
            .ToArray();
        _headerRead = true;
    }

    public bool Read()
    {
        if (IsClosed)
        {
            throw new ObjectDisposedException(nameof(CsvDataReader));
        }

        if (!_headerRead)
        {
            throw new InvalidOperationException("ReadHeader() must be called before Read().");
        }

        string? row;
        // Skip empty rows
        do
        {
            row = CsvParser.ReadCsvRow(reader);
            if (row == null)
            {
                return false;
            }
        } while (string.IsNullOrWhiteSpace(row));

        _currentRow = CsvParser.ParseCsvLine(row, nullChar);

        // Handle rows with fewer fields than headers
        if (_currentRow.Length >= _columnNames.Length)
        {
            return true;
        }

        var originalLength = _currentRow.Length;
        Array.Resize(ref _currentRow, _columnNames.Length);
        for (var i = originalLength; i < _columnNames.Length; i++)
        {
            _currentRow[i] = string.Empty;
        }

        return true;
    }

    public string GetName(int i)
    {
        if (!_headerRead)
        {
            throw new InvalidOperationException("ReadHeader() must be called before GetName().");
        }

        if (i < 0 || i >= _columnNames.Length)
        {
            throw new IndexOutOfRangeException();
        }

        return _columnNames[i];
    }

    public int GetOrdinal(string name)
    {
        if (!_headerRead)
        {
            throw new InvalidOperationException("ReadHeader() must be called before GetOrdinal().");
        }

        for (var i = 0; i < _columnNames.Length; i++)
        {
            if (_columnNames[i].Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public string? GetValue(int i)
    {
        if (_currentRow == null)
        {
            throw new InvalidOperationException("No data available. Call Read() first.");
        }

        if (i < 0 || i >= _currentRow.Length)
        {
            throw new IndexOutOfRangeException();
        }

        return _currentRow[i];
    }

    public void Close()
    {
        if (IsClosed)
        {
            return;
        }

        reader.Dispose();
        IsClosed = true;
    }

    public void Dispose()
    {
        Close();
    }
}