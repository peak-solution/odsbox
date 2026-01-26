"""Tests for chunked download functionality in file_access_download"""

from __future__ import annotations

import os
import tempfile
from unittest import mock

import pytest

from odsbox.con_i import ConI
from odsbox.proto.ods_pb2 import FileIdentifier


@pytest.fixture
def con_i():
    return ConI("https://docker.peak-solution.de:10032/api", ("Demo", "mdm"))


class TestChunkedDownload:
    """Test suite for chunked download functionality"""

    def test_chunked_download_with_default_chunk_size(self, con_i):
        """Test that default chunk size (8192 bytes) is used when not specified"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = os.path.join(temp_dir, "test_file.bin")

            with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                    mock_response = mock.Mock()
                    mock_response.iter_content = mock.Mock(return_value=[b"chunk1", b"chunk2"])
                    mock_response.headers = {}
                    mock_get.return_value = mock_response

                    con_i.file_access_download(file_identifier, target_file, overwrite_existing=True)

                    # Verify iter_content was called with default chunk size
                    mock_response.iter_content.assert_called_once_with(chunk_size=8192)

    def test_chunked_download_with_custom_chunk_size(self, con_i):
        """Test that custom chunk size is used when specified"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        custom_chunk_size = 4096
        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = os.path.join(temp_dir, "test_file.bin")

            with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                    mock_response = mock.Mock()
                    mock_response.iter_content = mock.Mock(return_value=[b"chunk1", b"chunk2"])
                    mock_response.headers = {}
                    mock_get.return_value = mock_response

                    con_i.file_access_download(
                        file_identifier, target_file, overwrite_existing=True, chunk_size=custom_chunk_size
                    )

                    # Verify iter_content was called with custom chunk size
                    mock_response.iter_content.assert_called_once_with(chunk_size=custom_chunk_size)

    def test_chunked_download_writes_all_chunks(self, con_i):
        """Test that all chunks are written to file"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        chunks = [b"chunk1_", b"chunk2_", b"chunk3_"]
        expected_content = b"chunk1_chunk2_chunk3_"

        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = os.path.join(temp_dir, "test_file.bin")

            with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                    mock_response = mock.Mock()
                    mock_response.iter_content = mock.Mock(return_value=chunks)
                    mock_response.headers = {}
                    mock_get.return_value = mock_response

                    con_i.file_access_download(file_identifier, target_file, overwrite_existing=True)

                    # Verify file content matches all chunks
                    with open(target_file, "rb") as f:
                        assert f.read() == expected_content

    def test_chunked_download_filters_empty_chunks(self, con_i):
        """Test that empty chunks (keep-alive chunks) are filtered out"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        # Include empty byte strings (keep-alive chunks)
        chunks = [b"data1", b"", b"data2", b"", b"data3"]
        expected_content = b"data1data2data3"

        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = os.path.join(temp_dir, "test_file.bin")

            with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                    mock_response = mock.Mock()
                    mock_response.iter_content = mock.Mock(return_value=chunks)
                    mock_response.headers = {}
                    mock_get.return_value = mock_response

                    con_i.file_access_download(file_identifier, target_file, overwrite_existing=True)

                    # Verify that only non-empty chunks were written
                    with open(target_file, "rb") as f:
                        assert f.read() == expected_content

    def test_chunked_download_with_stream_true(self, con_i):
        """Test that stream=True is set in the HTTP request"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = os.path.join(temp_dir, "test_file.bin")

            with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                    mock_response = mock.Mock()
                    mock_response.iter_content = mock.Mock(return_value=[b"data"])
                    mock_response.headers = {}
                    mock_get.return_value = mock_response

                    con_i.file_access_download(file_identifier, target_file, overwrite_existing=True)

                    # Verify stream=True is in the call
                    call_kwargs = mock_get.call_args[1]
                    assert call_kwargs["stream"] is True

    def test_chunked_download_large_file(self, con_i):
        """Test downloading a large file with chunked streaming"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        # Simulate a 1MB file downloaded in 8KB chunks
        chunk_size = 8192
        num_chunks = 128
        chunks = [b"x" * chunk_size for _ in range(num_chunks)]

        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = os.path.join(temp_dir, "large_file.bin")

            with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                    mock_response = mock.Mock()
                    mock_response.iter_content = mock.Mock(return_value=chunks)
                    mock_response.headers = {}
                    mock_get.return_value = mock_response

                    con_i.file_access_download(file_identifier, target_file, overwrite_existing=True)

                    # Verify file size is correct
                    file_size = os.path.getsize(target_file)
                    assert file_size == chunk_size * num_chunks

    def test_chunked_download_with_content_disposition(self, con_i):
        """Test chunked download respects Content-Disposition header"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        expected_filename = "server_filename.bin"

        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                    mock_response = mock.Mock()
                    mock_response.iter_content = mock.Mock(return_value=[b"data"])
                    mock_response.headers = {"Content-Disposition": f'attachment; filename="{expected_filename}"'}
                    mock_get.return_value = mock_response

                    result_path = con_i.file_access_download(file_identifier, temp_dir, overwrite_existing=True)

                    # Verify filename matches Content-Disposition
                    assert os.path.basename(result_path) == expected_filename
                    with open(result_path, "rb") as f:
                        assert f.read() == b"data"

    def test_chunked_download_preserves_binary_data(self, con_i):
        """Test that binary data integrity is preserved through chunked download"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        # Create binary data with all byte values
        binary_data = bytes(range(256))
        chunks = [binary_data[i : i + 16] for i in range(0, len(binary_data), 16)]

        with tempfile.TemporaryDirectory() as temp_dir:
            target_file = os.path.join(temp_dir, "binary_file.bin")

            with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                    mock_response = mock.Mock()
                    mock_response.iter_content = mock.Mock(return_value=chunks)
                    mock_response.headers = {}
                    mock_get.return_value = mock_response

                    con_i.file_access_download(file_identifier, target_file, overwrite_existing=True)

                    # Verify binary data is intact
                    with open(target_file, "rb") as f:
                        assert f.read() == binary_data

    def test_chunked_download_chunk_size_variations(self, con_i):
        """Test various chunk sizes"""
        file_identifier = FileIdentifier(aid=4711, iid=1)
        chunk_sizes = [1024, 4096, 8192, 16384, 65536]

        for chunk_size in chunk_sizes:
            with tempfile.TemporaryDirectory() as temp_dir:
                target_file = os.path.join(temp_dir, "test_file.bin")

                with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
                    with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                        mock_response = mock.Mock()
                        mock_response.iter_content = mock.Mock(return_value=[b"x" * chunk_size])
                        mock_response.headers = {}
                        mock_get.return_value = mock_response

                        con_i.file_access_download(
                            file_identifier, target_file, overwrite_existing=True, chunk_size=chunk_size
                        )

                        # Verify correct chunk size was requested
                        mock_response.iter_content.assert_called_once_with(chunk_size=chunk_size)
