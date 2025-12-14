# bulkcopy
A C# SqlBulkCopy binary tailored for Grad College data pipelines.

## Description
A .NET 10 console application that uses SqlBulkCopy to efficiently import CSV files into SQL Server database tables. Built as a self-contained Linux binary for easy deployment.

## Features
- Fast bulk import of CSV files to SQL Server
- Support for quoted CSV fields with embedded commas
- Configurable batch size for optimal performance
- Progress reporting during import
- Self-contained Linux binary (no .NET runtime required on target system)
- Optional error logging to a database table for failed rows

## Usage

```bash
BulkCopy <csv-file> <connection-string> <table-name> [batch-size] [--error-database <db>] [--error-table <table>]
```

### Parameters
- `csv-file`: Path to the CSV file to import (must have header row)
- `connection-string`: SQL Server connection string
- `table-name`: Name of the destination table in SQL Server
- `batch-size`: (Optional) Number of rows per batch (default: 1000)
- `--error-database`: (Optional) Database name for error logging (uses same connection credentials)
- `--error-table`: (Optional) Table name for error logging (default: BulkCopyErrors if --error-database is specified)

### Example

```bash
./BulkCopy data.csv "Server=myserver;Database=mydb;User Id=myuser;Password=mypass;TrustServerCertificate=True;" MyTable 1000
```

### Example with Error Logging

```bash
./BulkCopy data.csv "Server=myserver;Database=mydb;User Id=myuser;Password=mypass;TrustServerCertificate=True;" MyTable 1000 --error-database ErrorsDB --error-table ImportErrors
```

## CSV Format
- First row must contain column headers
- Column headers should match the target table column names (or use ordinal position mapping)
- Supports standard CSV format with comma delimiters
- Supports quoted fields with embedded commas and newlines
- Empty lines are skipped

## Building from Source

### Build for development
```bash
cd BulkCopy
dotnet build
```

### Run tests
```bash
cd BulkCopy.Tests
dotnet test
```

All 34 unit tests cover:
- Simple and complex CSV parsing
- Quoted fields with commas, newlines, and escaped quotes
- Different line ending formats (Unix, Windows, Mac)
- Edge cases like empty files and variable-length rows
- CSV row conversion with proper escaping
- SQL identifier validation and sanitization

### Publish as self-contained Linux binary
```bash
cd BulkCopy
dotnet publish -c Release -r linux-x64 --self-contained true
```

The binary will be available at: `bin/Release/net10.0/linux-x64/publish/BulkCopy`

## Requirements
- .NET 10 SDK (for building)
- No runtime dependencies on target Linux system (self-contained)
- SQL Server database (2012 or later)

## Error Logging

When the `--error-database` option is specified, the tool will:
1. Automatically create an error table if it doesn't exist
2. Log all failed rows to the error table with detailed information

### Error Table Schema

The error table includes the following columns:
- `Id`: Auto-incrementing primary key
- `SourceDatabase`: The database where the import was attempted
- `SourceTable`: The table where the import was attempted
- `RowNumber`: The row number (1-based) in the CSV file that failed
- `CsvHeaders`: Comma-separated column names from the CSV header
- `CsvRowData`: CSV representation of the failed row
- `ErrorMessage`: The error message explaining why the row failed
- `ErrorTimestamp`: When the error occurred

This allows you to:
- Review all failed imports in one place
- Understand patterns in data quality issues
- Re-attempt imports after fixing data issues
- Maintain an audit trail of import problems

## Notes
- The target table must exist before running the import
- Column mapping is done by ordinal position (first CSV column maps to first table column, etc.)
- Connection timeout is set to 5 minutes for large imports
- The application provides progress updates every batch
- When errors occur during batch operations, the tool automatically switches to row-by-row processing
- Failed rows are logged to the error table (if configured) and do not stop the import process
