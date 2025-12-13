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

## Usage

```bash
BulkCopy <csv-file> <connection-string> <table-name> [batch-size]
```

### Parameters
- `csv-file`: Path to the CSV file to import (must have header row)
- `connection-string`: SQL Server connection string
- `table-name`: Name of the destination table in SQL Server
- `batch-size`: (Optional) Number of rows per batch (default: 1000)

### Example

```bash
./BulkCopy data.csv "Server=myserver;Database=mydb;User Id=myuser;Password=mypass;TrustServerCertificate=True;" MyTable 1000
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

## Notes
- The target table must exist before running the import
- Column mapping is done by ordinal position (first CSV column maps to first table column, etc.)
- Connection timeout is set to 5 minutes for large imports
- The application provides progress updates every batch
