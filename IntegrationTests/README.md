# BulkCopy Integration Tests

This directory contains integration tests for the BulkCopy application that verify error handling with real SQL Server instances.

## Prerequisites

- Docker installed and running
- .NET 10 SDK (for building the application)
- Bash shell (Linux/macOS or WSL on Windows)

## Running the Integration Test

```bash
cd IntegrationTests
./run-integration-test.sh
```

## What the Test Does

The integration test script (`run-integration-test.sh`) performs the following:

1. **Spins up SQL Server** - Starts a SQL Server 2022 container using Docker
2. **Creates test database** - Creates a test database and table with diverse column types:
   - INT (ID, Age) - Tests numeric type conversion and validation
   - NVARCHAR (Name, Description, IsActive, BirthDate, CreatedAt, Score, Code)
   - DECIMAL (Salary) - Tests decimal type conversion

3. **Creates error logging database** - Creates a separate database for error logging

4. **Generates test CSV** - Creates a CSV file with 25 rows:
   - **Good rows**: 20 rows with valid data
   - **Bad rows**: 5 rows with invalid data (rows 6, 11, 16, 23, 24)
     - Row 6: Invalid age ("BadAge")
     - Row 11: Invalid age ("InvalidAge")
     - Row 16: Multiple errors (invalid age, salary, and score)
     - Row 23: Invalid birth date
     - Row 24: Code too long for column

5. **Runs BulkCopy with error logging** - Executes the BulkCopy application with:
   - Batch size of 10
   - Error logging enabled (`--error-database` and `--error-table` parameters)

6. **Verifies data import results**:
   - Confirms exactly 20 valid rows were inserted
   - Verifies bad rows (6, 11, 16, 23, 24) were skipped
   - Confirms sample good rows (1, 5, 10, 15, 20) were inserted
   - Displays sample data from the table

7. **Verifies error logging functionality**:
   - Confirms error table was automatically created
   - Verifies exactly 5 errors were logged
   - Checks error table schema has all required columns:
     - Id, SourceDatabase, SourceTable, RowNumber
     - CsvHeaders, CsvRowData, ErrorMessage, ErrorTimestamp
   - Verifies error logs contain correct source database and table
   - Confirms error row numbers are correct (6, 11, 16, 23, 24)
   - Validates CSV headers and row data are present in error logs
   - Ensures error messages are captured

## Expected Output

The test demonstrates the error handling and logging behavior:
- Batches with only good data are processed quickly in bulk
- Batches containing bad rows fall back to row-by-row processing
- Bad rows are logged with specific row numbers and error details to console
- **Error rows are also written to the error table with full details**
- Processing continues after skipping bad rows
- Final summary shows: "Successfully imported 20 rows, failed 5 rows."
- Error logging verification confirms all 5 errors were captured with complete information

## Test Data Layout

With batch size 10:
- **Batch 1** (rows 1-10): Contains 1 bad row (row 6)
- **Batch 2** (rows 11-20): Contains 2 bad rows (rows 11, 16)
- **Batch 3** (rows 21-25): Contains 2 bad rows (rows 23, 24)

This tests the error handling and logging across multiple batches.

## Cleanup

The script automatically cleans up:
- Stops and removes the SQL Server container
- Removes temporary CSV file

Cleanup occurs even if the test fails (using trap).

## Troubleshooting

If the test fails:

1. **Docker not running**: Ensure Docker daemon is running
2. **Port 1433 in use**: Another SQL Server might be using the port
3. **Insufficient wait time**: SQL Server might need more time to start (adjust WAIT_TIME in script)
4. **Build errors**: Ensure .NET 10 SDK is installed and project builds successfully

## Manual Testing

To manually inspect the results:

```bash
# Start the container (modify the script to comment out the cleanup trap)
./run-integration-test.sh

# In another terminal, query the database:
docker exec bulkcopy-test-sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "YourStrong!Passw0rd" -d TestDB -C \
    -Q "SELECT * FROM TestTable ORDER BY ID;"

# When done, cleanup:
docker stop bulkcopy-test-sqlserver
docker rm bulkcopy-test-sqlserver
```
