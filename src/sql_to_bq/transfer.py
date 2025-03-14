import polars as pl
import pyodbc
import time
import logging
import os
import uuid
import tempfile
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Optional, Dict, Any
import sys
import gc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transfer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("sql-to-bq-transfer")

class SQLServerToBigQueryTransfer:
    """Transfer data from SQL Server to BigQuery using Polars."""

    def __init__(
        self,
        sql_server: str,
        sql_database: str,
        sql_query: Optional[str] = None,
        sql_table: Optional[str] = None,
        bq_project: str = None,
        bq_dataset: str = None,
        bq_table: str = None,
        key_path: str = None,
        total_rows: Optional[int] = None,
        chunk_size: int = 100000,
        sql_username: Optional[str] = None,
        sql_password: Optional[str] = None,
        sql_driver: str = "ODBC Driver 17 for SQL Server",
        write_mode: str = "truncate_append"
    ):
        """Initialize the transfer with connection parameters."""
        self.sql_server = sql_server
        self.sql_database = sql_database
        self.sql_query = sql_query
        self.sql_table = sql_table
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
        self.key_path = key_path
        self.chunk_size = chunk_size
        self.total_rows = total_rows
        self.sql_username = sql_username
        self.sql_password = sql_password
        self.sql_driver = sql_driver
        self.write_mode = write_mode

        if not sql_query and not sql_table:
            raise ValueError("Either sql_query or sql_table name must be provided")

        # Full BigQuery table reference
        self.bq_table_ref = f"{self.bq_project}.{self.bq_dataset}.{self.bq_table}"

        self.user_provided_total_rows = total_rows

        # Initialize connections
        self._init_connections()

    def _init_connections(self):
        """Initialize SQL Server and BigQuery connections."""
        # SQL Server connection string
        if self.sql_username and self.sql_password:
            self.conn_str = (
                f'DRIVER={{{self.sql_driver}}};'
                f'SERVER={self.sql_server};'
                f'DATABASE={self.sql_database};'
                f'UID={self.sql_username};'
                f'PWD={self.sql_password}'
            )
        else:
            self.conn_str = (
                f'DRIVER={{{self.sql_driver}}};'
                f'SERVER={self.sql_server};'
                f'DATABASE={self.sql_database};'
                f'Trusted_Connection=yes'
            )

        # Test SQL connection
        try:
            self.sql_conn = pyodbc.connect(self.conn_str)
            logger.info("SQL Server connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            raise

        # BigQuery connection
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                self.key_path
            )
            self.bq_client = bigquery.Client.from_service_account_json(
                self.key_path,
                project=self.bq_project
            )
            logger.info("BigQuery connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {e}")
            raise

    def _get_total_rows(self) -> int:
        """Get the total number of rows to transfer."""
        if self.user_provided_total_rows is not None:
            logger.info(f"Using user-provided total row count: {self.user_provided_total_rows}")
            return self.user_provided_total_rows

        try:
            cursor = self.sql_conn.cursor()

            if self.sql_query:
                count_query = f"SELECT COUNT(*) FROM ({self.sql_query}) AS subquery"
            else:
                count_query = f"SELECT COUNT(*) FROM {self.sql_table}"

            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]
            return total_rows
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            raise
        finally:
            cursor.close()

    def _read_chunk(self, offset: int, limit: int) -> pl.DataFrame:
        """Read a chunk of data from SQL Server."""
        if self.sql_query:
            chunk_query = f"""
            SELECT subquery.* FROM ({self.sql_query}) AS subquery
            ORDER BY (SELECT NULL)
            OFFSET {offset} ROWS
            FETCH NEXT {limit} ROWS ONLY
            """
        else:
            chunk_query = f"""
            SELECT * FROM {self.sql_table}
            ORDER BY (SELECT NULL)
            OFFSET {offset} ROWS
            FETCH NEXT {limit} ROWS ONLY
            """

        try:
            df = pl.read_database(query=chunk_query, connection=self.sql_conn)
            return df
        except Exception as e:
            logger.error(f"Error reading chunk offset {offset}, limit: {limit}: {e}")
            try:
                self.sql_conn = pyodbc.connect(self.conn_str)
            except:
                pass
            raise

    def _upload_to_bigquery(self, df: pl.DataFrame, is_first_chunk: bool) -> None:
        """Upload a Polars DataFrame to BigQuery with improved file handling."""
        if df.is_empty():
            logger.info("Skipping empty chunk")
            return

        temp_dir = tempfile.gettempdir()
        unique_id = str(uuid.uuid4())
        temp_file = os.path.join(temp_dir, f"bq_upload_{unique_id}.parquet")

        try:
            if self.write_mode == "truncate_append":
                write_disposition = (
                    bigquery.WriteDisposition.WRITE_TRUNCATE if is_first_chunk 
                    else bigquery.WriteDisposition.WRITE_APPEND
                )
            else:
                write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition,
                autodetect=True,
            )

            df.write_parquet(temp_file)

            with open(temp_file, "rb") as source_file:
                job = self.bq_client.load_table_from_file(
                    source_file,
                    self.bq_table_ref,
                    job_config=job_config
                )

            job.result()

            logger.info(f"Uploaded {df.shape[0]} rows to BigQuery")

        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {e}")
            raise

        finally:
            try:
                if os.path.exists(temp_file):
                    df = None
                    gc.collect()

                    for i in range(5):
                        try:
                            os.remove(temp_file)
                            break
                        except PermissionError:
                            if i < 4:
                                time.sleep(0.5)
                            else:
                                logger.warning(f"Could not remove temporary file: {temp_file}")
            except Exception as cleanup_error:
                logger.warning(f"Error during cleanup of temporary file: {cleanup_error}")

    def transfer_data(self) -> Dict[str, Any]:
        """Transfer data from SQL Server to BigQuery in chunks."""
        start_time = time.time()
        bytes_transferred = 0
        rows_transferred = 0

        try:
            # Get total rows
            total_rows = self._get_total_rows()
            logger.info(f"Starting transfer of {total_rows} rows with chunk size {self.chunk_size}")

            # Process in chunks
            first_chunk = True

            for offset in range(0, total_rows, self.chunk_size):
                chunk_start_time = time.time()
                limit = min(self.chunk_size, total_rows - offset)

                logger.info(f"Processing chunk at offset {offset} with limit {limit}")

                # Read chunk
                df_chunk = self._read_chunk(offset, limit)

                # Upload to BigQuery if not empty
                if not df_chunk.is_empty():
                    chunk_rows = df_chunk.shape[0]
                    try:
                        chunk_bytes = df_chunk.estimated_size()
                    except:
                        # Fallback if estimated_size() is not available
                        chunk_bytes = chunk_rows * 1000  # Rough estimate: 1KB per row

                    bytes_transferred += chunk_bytes

                    logger.info(f"Read {chunk_rows} rows ({chunk_bytes / 1024 / 1024:.2f} MB)")

                    self._upload_to_bigquery(df_chunk, first_chunk)

                    rows_transferred += chunk_rows
                    first_chunk = False

                    if chunk_rows < limit:
                        logger.warning(f"Received {chunk_rows} rows when expecting {limit}, reached end of data")
                        break
                else:
                    logger.info("Chunk is empty, skipping upload")

                chunk_time = time.time() - chunk_start_time
                logger.info(f"Chunk processed in {chunk_time:.2f} seconds")

            # Get final statistics
            total_time = time.time() - start_time
            mb_transferred = bytes_transferred / (1024 * 1024)

            result = {
                "success": True,
                "rows_transferred": rows_transferred,
                "total_rows": total_rows,
                "time_taken": total_time,
                "rows_per_second": rows_transferred / total_time if total_time > 0 else 0,
                "mb_transferred": mb_transferred,
                "mb_per_second": mb_transferred / total_time if total_time > 0 else 0,
            }

            logger.info(f"Transfer completed: {rows_transferred} rows in {total_time:.2f} seconds")
            return result

        except Exception as e:
            logger.error(f"Transfer failed: {e}")
            total_time = time.time() - start_time
            return {
                "success": False,
                "error": str(e),
                "time_taken": total_time,
                "rows_transferred": rows_transferred,
                "mb_transferred": bytes_transferred / (1024 * 1024)
            }
        finally:
            # Close connections
            try:
                self.sql_conn.close()
                logger.info("SQL Server connection closed")
            except:
                pass
