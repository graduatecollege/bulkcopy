#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}BulkCopy Integration Test${NC}"
echo -e "${YELLOW}========================================${NC}"

# Configuration
CONTAINER_NAME="bulkcopy-test-sqlserver"
SA_PASSWORD="YourStrong!Passw0rd"
DB_NAME="TestDB"
TABLE_NAME="TestTable"
CSV_FILE="test_data.csv"
WAIT_TIME=30

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    rm -f $CSV_FILE
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Stop and remove existing container if it exists
echo -e "${YELLOW}Removing any existing test containers...${NC}"
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

# Start SQL Server container
echo -e "${YELLOW}Starting SQL Server container...${NC}"
docker run -d \
    --name $CONTAINER_NAME \
    -e "ACCEPT_EULA=Y" \
    -e "SA_PASSWORD=$SA_PASSWORD" \
    -p 1433:1433 \
    mcr.microsoft.com/mssql/server:2022-latest

# Wait for SQL Server to be ready
echo -e "${YELLOW}Waiting for SQL Server to be ready (${WAIT_TIME}s)...${NC}"
sleep $WAIT_TIME

# Test SQL Server connection
echo -e "${YELLOW}Testing SQL Server connection...${NC}"
for i in {1..10}; do
    if docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P "$SA_PASSWORD" -C \
        -Q "SELECT 1" &>/dev/null; then
        echo -e "${GREEN}✓ SQL Server is ready${NC}"
        break
    fi
    if [ $i -eq 10 ]; then
        echo -e "${RED}✗ Failed to connect to SQL Server${NC}"
        exit 1
    fi
    echo "Attempt $i failed, retrying..."
    sleep 3
done

# Create database and table with various column types
echo -e "${YELLOW}Creating database and table...${NC}"
docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -C \
    -Q "CREATE DATABASE $DB_NAME;"

docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "CREATE TABLE $TABLE_NAME (
        ID INT,
        Name NVARCHAR(100),
        Age INT,
        Salary DECIMAL(10,2),
        IsActive NVARCHAR(1),
        BirthDate datetime2,
        CreatedAt datetime2,
        Score decimal(4,2),
        Description NVARCHAR(MAX),
        Code NVARCHAR(7)
    );"

echo -e "${GREEN}✓ Database and table created${NC}"

# Create CSV file with good batches and bad rows
echo -e "${YELLOW}Creating test CSV file...${NC}"
cat > $CSV_FILE << 'EOF'
ID,Name,Age,Salary,IsActive,BirthDate,CreatedAt,Score,Description,Code
1,Alice Johnson,30,75000.50,1,1993-05-15,2024-01-01 10:00:00,95.5,Excellent employee,EMPL001
2,Bob Smith,25,65000.00,1,1998-08-22,2024-01-02 11:30:00,88.3,Good performer,EMPL002
3,Carol White,35,85000.75,1,1988-12-10,2024-01-03 09:15:00,92.1,Senior staff,EMPL003
4,David Brown,28,70000.00,1,1995-03-18,2024-01-04 14:20:00,89.7,Team player,EMPL004
5,Eve Davis,32,78000.25,1,1991-07-25,2024-01-05 08:45:00,91.2,Reliable worker,EMPL005
6,Frank Miller,BadAge,68000.00,1,1996-11-30,2024-01-06 13:10:00,85.9,Age is invalid,EMPL006
7,Grace Lee,27,72000.50,1,1996-09-14,2024-01-07 10:30:00,90.4,High potential,EMPL007
8,Henry Wilson,31,80000.00,1,1992-04-20,2024-01-08 15:25:00,93.6,Exceptional,EMPL008
9,Iris Taylor,29,71000.75,1,1994-06-08,2024-01-09 09:50:00,87.8,Consistent,EMPL009
10,Jack Anderson,26,67000.00,1,1997-10-12,2024-01-10 11:15:00,86.5,Developing,EMPL010
11,Karen Thomas,InvalidAge,73000.50,1,1990-02-28,2024-01-11 12:40:00,88.9,Another bad age,EMPL011
12,Leo Martinez,33,82000.00,1,1990-01-05,2024-01-12 14:05:00,94.2,Strong performer,EMPL012
13,Mia Jackson,24,64000.50,1,1999-12-19,2024-01-13 10:20:00,84.7,Entry level,EMPL013
14,Noah Garcia,36,87000.75,1,1987-08-16,2024-01-14 08:55:00,95.8,Senior expert,EMPL014
15,Olivia Rodriguez,30,76000.00,1,1993-05-23,2024-01-15 13:30:00,90.1,Mid-level,EMPL015
16,Paul White,InvalidData,InvalidSalary,1,1995-07-11,2024-01-16 09:45:00,NotANumber,Multiple errors,EMPL016
17,Quinn Harris,28,71000.25,1,1995-11-27,2024-01-17 11:50:00,89.3,Good worker,EMPL017
18,Rachel Clark,34,83000.50,1,1989-03-14,2024-01-18 14:15:00,92.7,Valuable asset,EMPL018
19,Sam Lewis,27,69000.00,1,1996-09-30,2024-01-19 10:05:00,87.5,Promising,EMPL019
20,Tina Walker,31,77000.75,1,1992-06-18,2024-01-20 12:35:00,91.8,Dedicated,EMPL020
21,Uma Young,29,70000.00,1,1996-02-10,2024-01-21 10:00:00,88.1,New hire,EMPL021
22,Vincent King,33,81000.00,1,0204-09-15,2024-01-22 11:11:11,90.0,Ancient birthday,EMPL022
23,Wendy Scott,41,91000.00,1,a,2024-01-23 12:12:12,93.3,Bad Date,EMPL023
24,Xavier Adams,27,68000.00,1,1997-07-07,2024-01-24 13:13:13,85.5,Too long code,EMPL024LONG
25,Yara Perez,26,62000.00,1,1998-02-02,2024-01-25 14:14:14,84.0,Recent grad,EMPL025
EOF

echo -e "${GREEN}✓ CSV file created with 25 rows (5 bad rows: 6, 11, 16, 23, 24)${NC}"

# Build the BulkCopy application
echo -e "${YELLOW}Building BulkCopy application...${NC}"
cd ../BulkCopy
dotnet build -c Release > /dev/null 2>&1
cd ../IntegrationTests
echo -e "${GREEN}✓ Application built${NC}"

# Run BulkCopy with the test data
echo -e "${YELLOW}Running BulkCopy...${NC}"
CONNECTION_STRING="Server=localhost,1433;Database=$DB_NAME;User Id=sa;Password=$SA_PASSWORD;TrustServerCertificate=True;"

../BulkCopy/bin/Release/net10.0/linux-x64/BulkCopy \
    $CSV_FILE \
    "$CONNECTION_STRING" \
    $TABLE_NAME \
    10

# Query the database to verify results
echo -e "\n${YELLOW}Verifying results...${NC}"
ACTUAL_COUNT=$(docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM $TABLE_NAME;" -h -1 | tr -d '[:space:]')

EXPECTED_COUNT=20  # 25 total rows - 5 bad rows

echo -e "${YELLOW}Expected rows: $EXPECTED_COUNT${NC}"
echo -e "${YELLOW}Actual rows inserted: $ACTUAL_COUNT${NC}"

if [ "$ACTUAL_COUNT" -eq "$EXPECTED_COUNT" ]; then
    echo -e "${GREEN}✓ SUCCESS: All $EXPECTED_COUNT valid rows were inserted!${NC}"
else
    echo -e "${RED}✗ FAILURE: Expected $EXPECTED_COUNT rows but got $ACTUAL_COUNT${NC}"
    exit 1
fi

# Display sample of inserted data
echo -e "\n${YELLOW}Sample of inserted data:${NC}"
docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "SELECT TOP 5 ID, Name, Age, Salary FROM $TABLE_NAME ORDER BY ID;"

# Verify specific rows that should NOT be in the table (the bad rows)
echo -e "\n${YELLOW}Verifying bad rows were skipped:${NC}"
BAD_ROW_6=$(docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM $TABLE_NAME WHERE ID = 6;" -h -1 | tr -d '[:space:]')

BAD_ROW_11=$(docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM $TABLE_NAME WHERE ID = 11;" -h -1 | tr -d '[:space:]')

BAD_ROW_16=$(docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM $TABLE_NAME WHERE ID = 16;" -h -1 | tr -d '[:space:]')

BAD_ROW_23=$(docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM $TABLE_NAME WHERE ID = 23;" -h -1 | tr -d '[:space:]')

BAD_ROW_24=$(docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM $TABLE_NAME WHERE ID = 24;" -h -1 | tr -d '[:space:]')

if [ "$BAD_ROW_6" -eq "0" ] && [ "$BAD_ROW_11" -eq "0" ] && [ "$BAD_ROW_16" -eq "0" ] && [ "$BAD_ROW_23" -eq "0" ] && [ "$BAD_ROW_24" -eq "0" ]; then
    echo -e "${GREEN}✓ Bad rows (6, 11, 16, 23, 24) were correctly skipped${NC}"
else
    echo -e "${RED}✗ Some bad rows were incorrectly inserted${NC}"
    exit 1
fi

# Verify specific good rows that should be in the table
echo -e "\n${YELLOW}Verifying good rows were inserted:${NC}"
GOOD_ROWS=$(docker exec $CONTAINER_NAME /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$SA_PASSWORD" -d $DB_NAME -C \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM $TABLE_NAME WHERE ID IN (1,5,10,15,20);" -h -1 | tr -d '[:space:]')

if [ "$GOOD_ROWS" -eq "5" ]; then
    echo -e "${GREEN}✓ Sample good rows (1,5,10,15,20) were correctly inserted${NC}"
else
    echo -e "${RED}✗ Expected 5 sample good rows but found $GOOD_ROWS${NC}"
    exit 1
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✓ ALL INTEGRATION TESTS PASSED!${NC}"
echo -e "${GREEN}========================================${NC}"

exit 0
