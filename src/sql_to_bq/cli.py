"""Command-line interface for SQL Server to BigQuery transfer."""

import argparse
import sys
from .transfer import SQLServerToBigQueryTransfer, logger

def main():
    """Run the transfer from command line."""
    parser = argparse.ArgumentParser(description='Transfer data from SQL Server to BigQuery')
    parser.add_argument('--sql-server', required=True, help='SQL Server hostname')
    parser.add_argument('--sql-database', required=True, help='SQL Server database name')
    parser.add_argument('--sql-table', help='SQL Server table name')
    parser.add_argument('--sql-query', help='Custom SQL query to select data')
    parser.add_argument('--sql-username', help='SQL Server username (if not using Windows auth)')
    parser.add_argument('--sql-password', help='SQL Server password (if not using Windows auth)')
    parser.add_argument('--bq-project', required=True, help='BigQuery project ID')
    parser.add_argument('--bq-dataset', required=True, help='BigQuery dataset name')
    parser.add_argument('--bq-table', required=True, help='BigQuery table name')
    parser.add_argument('--key-path', required=True, help='Path to service account key file')
    parser.add_argument('--chunk-size', type=int, default=100000, help='Chunk size for processing')
    parser.add_argument('--total-rows', type=int,help='Total rows of returned by the table or sql query')
    parser.add_argument('--write-mode', type=str, default='truncate_append', help='Write mode for BigQuery table (truncate_append or append)')

    args = parser.parse_args()

    transfer = SQLServerToBigQueryTransfer(
        sql_server=args.sql_server,
        sql_database=args.sql_database,
        sql_table=args.sql_table,
        sql_query=args.sql_query,
        sql_username=args.sql_username,
        sql_password=args.sql_password,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        bq_table=args.bq_table,
        key_path=args.key_path,
        chunk_size=args.chunk_size,
        total_rows=args.total_rows,
        write_mode=args.write_mode
    )

    result = transfer.transfer_data()

    if result["success"]:
        logger.info(f"Successfully transferred {result['rows_transferred']} rows in {result['time_taken']:.2f} seconds")
        logger.info(f"Performance: {result['rows_per_second']:.2f} rows/second")
    else:
        logger.error(f"Transfer failed: {result['error']}")
        sys.exit(1)

if __name__ == "__main__":
    main()
