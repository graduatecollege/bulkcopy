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

3. **Generates test CSV** - Creates a CSV file with 20 rows:
   - **Good rows**: 17 rows with valid data
   - **Bad rows**: 3 rows with invalid data (rows 6, 11, 16)
     - Row 6: Invalid age ("BadAge")
     - Row 11: Invalid age ("InvalidAge")
     - Row 16: Multiple errors (invalid age, salary, and score)

4. **Runs BulkCopy** - Executes the BulkCopy application with batch size of 10

5. **Verifies results**:
   - Confirms exactly 17 valid rows were inserted
   - Verifies bad rows (6, 11, 16) were skipped
   - Confirms sample good rows (1, 5, 10, 15, 20) were inserted
   - Displays sample data from the table

## Expected Output

The test demonstrates the error handling behavior:
- Batches with only good data are processed quickly in bulk
- Batches containing bad rows fall back to row-by-row processing
- Bad rows are logged with specific row numbers and error details
- Processing continues after skipping bad rows
- Final summary shows: "Successfully imported 17 rows, failed 3 rows."

## Test Data Layout

With batch size 10:
- **Batch 1** (rows 1-10): Contains 1 bad row (row 6)
- **Batch 2** (rows 11-20): Contains 2 bad rows (rows 11, 16)

This tests the error handling across multiple batches.

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
