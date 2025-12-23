using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace BulkCopy;

/// <summary>
///     A streaming IDataReader implementation for CSV files.
/// </summary>
public class CsvDataReader : IDataReader
{
    private readonly string[] _columnNames;
    private readonly string? _nullChar;
    private readonly StreamReader _reader;
    private string?[]? _currentRow;

    public CsvDataReader(string filePath, string? nullChar = "␀")
        : this(new StreamReader(filePath, Encoding.UTF8), nullChar)
    {
    }

    public CsvDataReader(StreamReader reader, string? nullChar = "␀")
    {
        _reader = reader;
        _nullChar = nullChar;
        IsClosed = false;

        // Read header row
        var firstRow = CsvParser.ReadCsvRow(_reader);
        if (string.IsNullOrEmpty(firstRow))
        {
            throw new InvalidOperationException("CSV file is empty or has no header row.");
        }

        _columnNames = CsvParser.ParseCsvLine(firstRow, null)
            .Select(h => h?.Trim() ?? "")
            .ToArray();
    }

    public int FieldCount => _columnNames.Length;

    public int Depth => 0;

    public bool IsClosed { get; private set; }

    public int RecordsAffected => -1;

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(GetOrdinal(name));

    public bool Read()
    {
        if (IsClosed)
        {
            return false;
        }

        string? row;
        // Skip empty rows
        do
        {
            row = CsvParser.ReadCsvRow(_reader);
            if (row == null)
            {
                return false;
            }
        } while (string.IsNullOrWhiteSpace(row));

        _currentRow = CsvParser.ParseCsvLine(row, _nullChar);

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
        if (i < 0 || i >= _columnNames.Length)
        {
            throw new IndexOutOfRangeException();
        }

        return _columnNames[i];
    }

    public int GetOrdinal(string name)
    {
        for (var i = 0; i < _columnNames.Length; i++)
        {
            if (_columnNames[i].Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public object GetValue(int i)
    {
        if (_currentRow == null)
        {
            throw new InvalidOperationException("No data available. Call Read() first.");
        }

        if (i < 0 || i >= _currentRow.Length)
        {
            throw new IndexOutOfRangeException();
        }

        var value = _currentRow[i];
        return value == null ? DBNull.Value : value;
    }

    public bool IsDBNull(int i)
    {
        return GetValue(i) == DBNull.Value;
    }

    public string GetString(int i)
    {
        var value = GetValue(i);
        if (value == DBNull.Value)
        {
            throw new InvalidCastException($"Cannot convert DBNull to string for column {i}.");
        }

        return (string)value;
    }

    public void Close()
    {
        if (!IsClosed)
        {
            _reader.Dispose();
            IsClosed = true;
        }
    }

    public void Dispose()
    {
        Close();
    }

    public DataTable GetSchemaTable()
    {
        var schemaTable = new DataTable();
        schemaTable.Columns.Add("ColumnName", typeof(string));
        schemaTable.Columns.Add("ColumnOrdinal", typeof(int));

        for (var i = 0; i < _columnNames.Length; i++)
        {
            var row = schemaTable.NewRow();
            row["ColumnName"] = _columnNames[i];
            row["ColumnOrdinal"] = i;
            schemaTable.Rows.Add(row);
        }

        return schemaTable;
    }

    // Required IDataReader methods - minimal implementations
    public bool NextResult()
    {
        return false;
    }

    public int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, FieldCount);
        for (var i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }

        return count;
    }

    public string GetDataTypeName(int i)
    {
        return "string";
    }

    [return:
        DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties |
                                   DynamicallyAccessedMemberTypes.PublicFields)]
    public Type GetFieldType(int i)
    {
        return typeof(string);
    }

    public bool GetBoolean(int i)
    {
        return Convert.ToBoolean(GetValue(i));
    }

    public byte GetByte(int i)
    {
        return Convert.ToByte(GetValue(i));
    }

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    {
        throw new NotSupportedException();
    }

    public char GetChar(int i)
    {
        return Convert.ToChar(GetValue(i));
    }

    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
    {
        throw new NotSupportedException();
    }

    public Guid GetGuid(int i)
    {
        return (Guid)GetValue(i);
    }

    public short GetInt16(int i)
    {
        return Convert.ToInt16(GetValue(i));
    }

    public int GetInt32(int i)
    {
        return Convert.ToInt32(GetValue(i));
    }

    public long GetInt64(int i)
    {
        return Convert.ToInt64(GetValue(i));
    }

    public float GetFloat(int i)
    {
        return Convert.ToSingle(GetValue(i));
    }

    public double GetDouble(int i)
    {
        return Convert.ToDouble(GetValue(i));
    }

    public decimal GetDecimal(int i)
    {
        return Convert.ToDecimal(GetValue(i));
    }

    public DateTime GetDateTime(int i)
    {
        return Convert.ToDateTime(GetValue(i));
    }

    public IDataReader GetData(int i)
    {
        throw new NotSupportedException();
    }
}