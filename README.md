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

Usage:

```bash
BulkCopy <csv-file> [options]
```

Arguments:
- `<csv-file>` The CSV file to import.

Options:
  - `--connection-string <connection-string>` The SQL Server connection string (env:BULKCOPY_CONNECTION_STRING).
  - `--batch-size <batch-size>` The number of rows to insert per batch, default 500 (env:BULKCOPY_BATCH_SIZE).
  - `--error-database <error-database>` Optional database name for error logging on the same server (env:BULKCOPY_ERROR_DATABASE).
  - `--error-table <error-table>` Optional table name for error logging, defaults to BulkCopyErrors (env:BULKCOPY_ERROR_TABLE).
  - `--null-char <null-char>` Optional character to treat as null when unquoted, defaults to "␀" (env:BULKCOPY_NULL_CHAR).
  - `--server <server>` The destination SQL Server instance name (env:BULKCOPY_DB_SERVER).
  - `--username <username>` The SQL Server username (env:BULKCOPY_USERNAME).
  - `--password <password>` The SQL Server password (env:BULKCOPY_PASSWORD).
  - `--trust-server-certificate` Trust server certificate (no env variable).
  - `--timeout <timeout>` Connection timeout in seconds, defaults to 30 (env:BULKCOPY_TIMEOUT).
  - `--database <database>` The destination database name (env:BULKCOPY_DATABASE).
  - `--table <table>` The destination table name (env:BULKCOPY_TABLE).
  - `--help` Show help and usage information
  - `--version` Show version information

### Example

```bash
./BulkCopy data.csv \
  --connection-string "Server=myserver;Database=mydb;User Id=myuser;Password=mypass;TrustServerCertificate=True;" \
  --table MyTable
```

### Example with Error Logging

```bash
./BulkCopy data.csv \
  --connection-string "Server=myserver;Database=mydb;User Id=myuser;Password=mypass;TrustServerCertificate=True;" \
  --table MyTable \
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
