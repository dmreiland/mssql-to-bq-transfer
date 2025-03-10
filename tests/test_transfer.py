import unittest
from unittest.mock import patch, MagicMock, call
import tempfile
import json
from sql_to_bq.transfer import SQLServerToBigQueryTransfer

class TestSQLServerToBigQueryTransfer(unittest.TestCase):
    def setUp(self):
        # Create a temporary key file
        self.temp_key_file = tempfile.NamedTemporaryFile(delete=False)

        # Write dummy service account key content
        key_content = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC7VJTUt9Us8cKj\nMzEfYyjiWA4R4/M2bS1GB4t7NXp98C3SC6dVMvDuictGeurT8jNbvJZHtCSuYEvu\nNMoSfm76oqFvAp8Gy0iz5sxjZmSnXyCdPEovGhLa0VzMaQ8s+CLOyS56YyCFGeJZ\n-----END PRIVATE KEY-----\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
            "client_id": "123456789",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test-project.iam.gserviceaccount.com"
        }
        self.temp_key_file.write(json.dumps(key_content).encode())
        self.temp_key_file.flush()

        # Set up common test parameters
        self.params = {
            'sql_server': 'test-server',
            'sql_database': 'test-db',
            'sql_username': 'test-user',
            'sql_password': 'test-pass',
            'sql_table': 'test_table',
            'key_path': self.temp_key_file.name,
            'bq_project': 'test-project',
            'bq_dataset': 'test_dataset',
            'bq_table': 'test_table',
            'chunk_size': 1000
        }

    def tearDown(self):
        # Clean up the temporary file
        self.temp_key_file.close()

    @patch('pyodbc.connect')
    @patch('google.oauth2.service_account.Credentials.from_service_account_file')
    @patch('google.cloud.bigquery.Client.from_service_account_json')
    def test_init_connections(self, mock_bq_client_from_json, mock_credentials, mock_pyodbc_connect):
        # Setup mocks
        mock_conn = MagicMock()
        mock_pyodbc_connect.return_value = mock_conn
        mock_credentials.return_value = MagicMock()
        mock_bq_client_from_json.return_value = MagicMock()

        # Create transfer object
        transfer = SQLServerToBigQueryTransfer(**self.params)

        # Verify connections were initialized
        mock_pyodbc_connect.assert_called_once()
        mock_credentials.assert_called_once_with(self.temp_key_file.name)
        mock_bq_client_from_json.assert_called_once_with(
            self.temp_key_file.name,
            project=self.params['bq_project']
        )

    @patch('pyodbc.connect')
    @patch('google.oauth2.service_account.Credentials.from_service_account_file')
    @patch('google.cloud.bigquery.Client.from_service_account_json')
    def test_chunking(self, mock_bq_client_from_json, mock_credentials, mock_pyodbc_connect):
        """Test chunking with various offsets"""
        # Mock SQL connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc_connect.return_value = mock_conn

        # Mock credentials and BQ client
        mock_credentials.return_value = MagicMock()
        mock_bq_client_from_json.return_value = MagicMock()

        # Mock cursor execution and fetchone for total rows
        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = [100]  # 100 total rows

        # Create transfer object
        transfer = SQLServerToBigQueryTransfer(
            sql_server="mock-server",
            sql_database="mock-db",
            sql_table="mock_table",
            chunk_size=25,
            key_path=self.temp_key_file.name,
            bq_project="test-project",
            bq_dataset="test_dataset",
            bq_table="test_table"
        )

        # Mock the read_chunk and upload methods
        with patch.object(transfer, '_read_chunk', wraps=transfer._read_chunk) as spy_read_chunk:
            with patch.object(transfer, '_upload_to_bigquery') as mock_upload:
                with patch('polars.read_database') as mock_read_database:
                    mock_dfs = []
                    # Mock DataFrame returns
                    for i in range(4):
                        mock_df = MagicMock()
                        mock_df.is_empty.return_value = False
                        mock_df.shape = (25, 10)  # 25 rows, 10 columns
                        mock_df.estimated_size.return_value = 1024 * 1024  # 1 MB
                        mock_dfs.append(mock_df)

                    mock_read_database.side_effect = mock_dfs
                    result = transfer.transfer_data()

                    # Verify results
                    assert result["success"] is True
                    assert result["rows_transferred"] == 100
                    assert result["total_rows"] == 100
                    assert spy_read_chunk.call_count == 4

                    # Verify correct offsets were used
                    from unittest.mock import ANY
                    expected_calls = [
                        call(0, 25),
                        call(25, 25),
                        call(50, 25),
                        call(75, 25)
                    ]

                    spy_read_chunk.assert_has_calls(expected_calls, any_order=False)
                    assert mock_upload.call_count == 4
    
    @patch('pyodbc.connect')
    @patch('google.oauth2.service_account.Credentials.from_service_account_file')
    @patch('google.cloud.bigquery.Client.from_service_account_json')
    def test_user_provided_row_count(self, mock_bq_client_from_json, mock_credentials, mock_pyodbc_connect):
        """Test transfer with user-provided row count"""
        # Mock SQL connection
        mock_conn = MagicMock()
        mock_pyodbc_connect.return_value = mock_conn

        # Create transfer object with user-provided row count
        transfer = SQLServerToBigQueryTransfer(
            sql_server="mock-server",
            sql_database="mock-db",
            sql_table="mock_table",
            chunk_size=25,
            key_path=self.temp_key_file.name,
            bq_project="test-project",
            bq_dataset="test_dataset",
            bq_table="test_table",
            total_rows=100
        )

        with patch.object(transfer, '_read_chunk', wraps=transfer._read_chunk) as spy_read_chunk:
            with patch.object(transfer, '_upload_to_bigquery') as mock_upload:
                with patch('polars.read_database') as mock_read_database:
                    mock_dfs = []
                    for i in range(4):
                        mock_df = MagicMock()
                        mock_df.is_empty.return_value = False
                        mock_df.shape = (25, 10)  # 25 rows, 10 columns
                        mock_df.estimated_size.return_value = 1024 * 1024  # 1 MB
                        mock_dfs.append(mock_df)

                    mock_read_database.side_effect = mock_dfs
                    result = transfer.transfer_data()

                    assert result["success"] is True
                    assert result["total_rows"] == 100
                    assert result["rows_transferred"] == 100

                    cursor = mock_conn.cursor.return_value
                    for call_args in cursor.execute.call_args_list:
                        assert "COUNT(*)" not in call_args[0][0]

                    assert spy_read_chunk.call_count == 4
                    assert mock_upload.call_count == 4
