# bulkcopy

A C# SqlBulkCopy binary tailored for Grad College data pipelines.

## Description

A .NET console application that uses SqlBulkCopy to efficiently import CSV files into SQL Server database tables.

This tool replaces dbatools' Import-DbaCsv to allow for more control over the import process.

## Features

- Usual benefits and caveats of SqlBulkCopy:
  - High performance
  - Constraints and triggers are not used
- Skips bad rows, optionally inserting failed rows to an error table
- Insert null values with a custom null character
- Supports arbitrarily large files

## Limitations

- The target table must exist before running the import.
- CSV file must have a header row, and be UTF-8 encoded.
- At this time the CSV format details are not configurable. Must use comma delimiters, `"` quotes.

## Usage

```bash
BulkCopy <csv-file> <connection-string> <table-name>
```

### Parameters
- `csv-file`: Path to the CSV file to import (must have header row)
- `connection-string`: SQL Server connection string
- `table-name`: Name of the destination table in SQL Server
- `batch-size`: (Optional) Number of rows per batch (default: 1000)
- `--error-database`: (Optional) Database name for error logging (uses same connection credentials)
- `--error-table`: (Optional) Table name for error logging (default: BulkCopyErrors if --error-database is specified)
- `--null-char`: (Optional) Character to treat as null when unquoted (default: `␀` null UTF-8 character)

### Example

```bash
./BulkCopy data.csv \
  "Server=myserver;Database=mydb;User Id=myuser;Password=mypass;TrustServerCertificate=True;" \
  MyTable
```

### Example with Error Logging

```bash
./BulkCopy data.csv \
  "Server=myserver;Database=mydb;User Id=myuser;Password=mypass;TrustServerCertificate=True;" \
  MyTable \
  --error-database ErrorsDB
```

## CSV Format

- First row must contain column headers
- Column headers should match the target table column names
- Supports standard CSV format with comma delimiters
- Supports quoted fields with embedded commas and newlines
- Empty lines are skipped
- Unquoted fields matching the null character (default: `␀`) are imported as SQL NULL values
- To import the literal ␀ character value, wrap it in quotes

## Error Logging

When the `--error-database` option is specified, the tool will automatically
create an error table (default name: `BulkCopyErrors`) and insert failed rows
into it.

### Error Table Schema

The error table includes the following columns:

- `Id`: Auto-incrementing primary key
- `SourceDatabase`: The database where the import was attempted
- `SourceTable`: The table where the import was attempted
- `RowNumber`: The row number (1-based) in the CSV file that failed
- `CsvRowData`: CSV representation of the failed row, including the header row
- `ErrorMessage`: The error message explaining why the row failed
- `ErrorTimestamp`: When the error occurred

## Support

This product is supported by the Graduate College on a best-effort basis.

As of the last update to this README, the expected End-of-Life and End-of-Support dates of this product are 2028-11-24.

End-of-Life was decided upon based on these dependencies:

- .NET Core 10 (2028-11-24)
