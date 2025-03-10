# SQL Server to BigQuery Transfer Utility

A high-performance data transfer utility for moving large datasets from Microsoft SQL Server to Google BigQuery using Polars.

## Features

- **Memory-efficient**: Uses Polars and Apache Arrow for optimal memory usage
- **Chunked processing**: Handles large tables by processing data in configurable chunks
- **Robust error handling**: Comprehensive logging and error management
- **Performance monitoring**: Tracks and reports transfer rates
- **Flexible authentication**: Supports both Windows and SQL authentication

## Prerequisites

- Python 3.8 or higher
- Microsoft ODBC Driver for SQL Server
- Access to a SQL Server database
- Google Cloud service account with BigQuery permissions

## Installation

### 1. Install UV Package Manager

UV is a fast, reliable Python package installer and resolver. Install it using one of the following methods:

**Using pip:**

```bash
pip install uv
```

**Using the official installer:**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**On Windows with PowerShell:**

```powershell
irm https://astral.sh/uv/install.ps1 | iex
```

### 2. Clone the Repository

```bash
git clone https://github.com/dmreiland/sql-to-bq-transfer.git
cd sql-to-bq-transfer
```

### 3. Create a Virtual Environment and Install Dependencies

```bash
# Create and activate a virtual environment using UV
uv venv

# On Windows:
.\.venv\Scripts\activate

# On macOS/Linux:
source .venv/bin/activate

# Install the package and its dependencies
uv pip install -e .

# Install development dependencies (for running tests)
uv pip install -e ".[dev]"
```

## Configuration

### SQL Server ODBC Driver

Ensure you have the Microsoft ODBC Driver for SQL Server installed:

**Windows:**
Download from the [Microsoft Download Center](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

**Ubuntu/Debian:**

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
apt-get install -y msodbcsql17
```

**RHEL/CentOS:**

```bash
curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo
yum install -y msodbcsql17
```

### Google Cloud Service Account

1. Create a service account in the Google Cloud Console with the following roles:
   - BigQuery Data Editor
   - BigQuery Job User

2. Create and download a JSON key file for the service account

3. Store the key file securely on the machine running the transfer script

## Testing

Run the test suite to verify that everything is working correctly:

```bash
# Run all tests
uv run pytest

# Run tests with coverage report
uv run pytest --cov=sql_to_bq

# Run a specific test file
uv run pytest tests/test_transfer.py
```

## Usage

### Command Line Interface

```bash
# Basic usage
uv run sql-to-bq \
  --sql-server "your-server" \
  --sql-database "your-database" \
  --sql-table "your_table" \
  --bq-project "your-gcp-project" \
  --bq-dataset "your_dataset" \
  --bq-table "your_table" \
  --key-path "/path/to/service-account-key.json"

# With additional options
uv run sql-to-bq \
  --sql-server "your-server" \
  --sql-database "your-database" \
  --sql-table "your_table" \
  --sql-username "your_username" \
  --sql-password "your_password" \
  --bq-project "your-gcp-project" \
  --bq-dataset "your_dataset" \
  --bq-table "your_table" \
  --key-path "/path/to/service-account-key.json" \
  --chunk-size 50000

# With custom query
uv run sql-to-bq \
  --sql-server "your-server" \
  --sql-database "your-database" \
  --sql-query "SELECT * FROM your_table WHERE create_date > '2023-01-01' AND status = 'active'" \
  --bq-project "your-gcp-project" \
  --bq-dataset "your_dataset" \
  --bq-table "your_table" \
  --key-path "/path/to/service-account-key.json"
```

#### Limiting Records with Custom Queries

You can use custom SQL queries to limit which records are transferred:

```bash
# Transfer only active customers created after 2023
uv run -m sql_to_bq.cli \
  --sql-server "your-server" \
  --sql-database "your-database" \
  --sql-query "SELECT * FROM customers WHERE create_date > '2023-01-01' AND status = 'active'" \
  --bq-project "your-gcp-project" \
  --bq-dataset "your_dataset" \
  --bq-table "active_customers_2023" \
  --key-path "/path/to/service-account-key.json"
```

### As a Python Module

You can also use the transfer utility programmatically in your Python code:

```python
from sql_to_bq.transfer import SQLServerToBigQueryTransfer

# Initialize the transfer
transfer = SQLServerToBigQueryTransfer(
    sql_server="your-server",
    sql_database="your-database",
    sql_table="your_table",
    bq_project="your-gcp-project",
    bq_dataset="your_dataset",
    bq_table="your_table",
    key_path="/path/to/service-account-key.json",
    chunk_size=100000
)

# Execute the transfer
result = transfer.transfer_data()

# Check results
if result["success"]:
    print(f"Successfully transferred {result['rows_transferred']} rows")
    print(f"Transfer rate: {result['rows_per_second']:.2f} rows/second")
else:
    print(f"Transfer failed: {result['error']}")
```

## Performance Tuning

For optimal performance with large datasets:

1. **Adjust chunk size**: The default chunk size is 100,000 rows. Increase for faster machines with more memory, decrease for memory-constrained environments.

2. **Memory monitoring**: Watch memory usage during transfers and adjust chunk size accordingly.

3. **Provide total row count**: For large tables, counting the total number of rows can be a performance bottleneck. If you know the approximate number of rows, you can provide it to avoid the overhead:

```python
# Without row count (will count rows first, which can be slow for large tables)
transfer = SQLServerToBigQueryTransfer(
    sql_server="your-server",
    sql_database="your-db",
    sql_table="large_table",
    # other parameters...
)

# With row count (skips the counting step)
transfer = SQLServerToBigQueryTransfer(
    sql_server="your-server",
    sql_database="your-db",
    sql_table="large_table",
    total_rows=1000000,  # Provide approximate row count
    # other parameters...
)
```

```bash
# CLI use with row count
uv run -m sql_to_bq.cli \
  --sql-server "your-server" \
  --sql-database "your-database" \
  --sql-query "SELECT * FROM customers WHERE create_date > '2023-01-01' AND status = 'active'" \
  --bq-project "your-gcp-project" \
  --bq-dataset "your_dataset" \
  --bq-table "active_customers_2023" \
  --total_rows 1000000 \
  --key-path "/path/to/service-account-key.json"
```

## Troubleshooting

### Common Issues

1. **SQL Server connection errors**:
   - Verify ODBC driver installation
   - Check server name, credentials, and network connectivity
   - Ensure the user has appropriate permissions

2. **BigQuery authentication errors**:
   - Verify the service account key file path
   - Check that the service account has the required permissions
   - Ensure the project ID is correct

3. **Memory errors**:
   - Reduce the chunk size
   - Monitor memory usage during transfer
   - Consider running on a machine with more RAM

### Logs

Check the ```transfer.log``` file for detailed information about the transfer process, including any errors that occurred.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Polars](https://github.com/pola-rs/polars) - Fast DataFrame library
- [PyArrow](https://arrow.apache.org/docs/python/) - Python implementation of Apache Arrow
- [Google Cloud BigQuery](https://cloud.google.com/bigquery) - Google's serverless data warehouse
- [UV](https://github.com/astral-sh/uv) - Fast Python package installer and resolver

---
