"""Integration test for ASAM ODS session"""

import requests
from odsbox.con_i import ConI

import pytest
import os
from unittest import mock
import tempfile
from odsbox.proto.ods_pb2 import FileIdentifier


@pytest.fixture
def con_i():
    return ConI("https://docker.peak-solution.de:10032/api", ("Demo", "mdm"))


def test_file_access_download_success(con_i):
    file_identifier = FileIdentifier(aid=4711, iid=1)
    target_file = "test_download.bin"

    with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
        with mock.patch.object(con_i._ConI__session, "get") as mock_get:
            mock_response = mock.Mock()
            mock_response.content = b"file content"
            mock_response.headers = {"Content-Disposition": "attachment; filename=test_download.bin"}
            mock_get.return_value = mock_response

            downloaded_file = con_i.file_access_download(file_identifier, target_file, overwrite_existing=True)

            assert downloaded_file == target_file
            assert os.path.exists(target_file)
            with open(target_file, "rb") as f:
                assert f.read() == b"file content"

            os.remove(target_file)


def test_file_access_download_success_2(con_i):
    file_identifier = FileIdentifier(aid=4711, iid=1)
    target_file = "test_download.bin"

    with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
        with mock.patch.object(con_i._ConI__session, "get") as mock_get:
            mock_response = mock.Mock()
            mock_response.content = b"file content"
            mock_response.headers = {"Content-Disposition": 'attachment; filename="test_download.bin"'}
            mock_get.return_value = mock_response

            downloaded_file = con_i.file_access_download(file_identifier, target_file, overwrite_existing=True)

            assert downloaded_file == target_file
            assert os.path.exists(target_file)
            with open(target_file, "rb") as f:
                assert f.read() == b"file content"

            os.remove(target_file)


def test_file_access_download_success_3(con_i):
    with tempfile.TemporaryDirectory() as temp_dir:
        file_identifier = FileIdentifier(aid=4711, iid=1)
        target_file = os.path.join(temp_dir, "test_download.bin")

        with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
            with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                mock_response = mock.Mock()
                mock_response.content = b"file content"
                mock_response.headers = {"Content-Disposition": 'attachment; filename="test_download.bin"'}
                mock_get.return_value = mock_response

                downloaded_file = con_i.file_access_download(file_identifier, temp_dir, overwrite_existing=True)

                assert downloaded_file == target_file
                assert os.path.exists(target_file)
                with open(target_file, "rb") as f:
                    assert f.read() == b"file content"


def test_file_access_download_success_4(con_i):
    with tempfile.TemporaryDirectory() as temp_dir:
        file_identifier = FileIdentifier(aid=4711, iid=1)
        target_file = os.path.join(temp_dir, "download.bin")

        with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
            with mock.patch.object(con_i._ConI__session, "get") as mock_get:
                mock_response = mock.Mock()
                mock_response.content = b"file content"
                mock_response.headers = {}
                mock_get.return_value = mock_response

                downloaded_file = con_i.file_access_download(file_identifier, temp_dir, overwrite_existing=True)

                assert downloaded_file == target_file
                assert os.path.exists(target_file)
                with open(target_file, "rb") as f:
                    assert f.read() == b"file content"


def test_file_access_download_no_overwrite(con_i):
    file_identifier = FileIdentifier(aid=4711, iid=1)
    target_file = "test_download_no_overwrite.bin"

    with open(target_file, "wb") as f:
        f.write(b"existing content")

    with mock.patch.object(con_i, "file_access", return_value="http://example.com/file"):
        with mock.patch.object(con_i._ConI__session, "get") as mock_get:
            mock_response = mock.Mock()
            mock_response.content = b"new file content"
            mock_response.headers = {"Content-Disposition": "attachment; filename=test_download_no_overwrite.bin"}
            mock_get.return_value = mock_response

            with pytest.raises(FileExistsError):
                con_i.file_access_download(file_identifier, target_file, overwrite_existing=False)

    os.remove(target_file)


def test_file_access_download_no_session(con_i):
    file_identifier = FileIdentifier(aid=4711, iid=1)
    target_file = "test_download_no_session.bin"

    con_i._ConI__session = None

    with pytest.raises(ValueError, match="No open session!"):
        con_i.file_access_download(file_identifier, target_file)


def test_file_access_upload_success(con_i):
    # Create a temporary file to upload
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"Test content")
        temp_file_path = temp_file.name

    file_identifier = FileIdentifier(aid=4711, iid=1)

    with mock.patch.object(con_i, "file_access", return_value="http://example.com/upload"):
        with mock.patch.object(con_i._ConI__session, "put") as mock_post:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response

            con_i.file_access_upload(file_identifier, temp_file_path)

            mock_post.assert_called_once()
            assert mock_post.call_args[0][0] == "http://example.com/upload"

    os.remove(temp_file_path)


def test_file_access_upload_failure(con_i):
    # Create a temporary file to upload
    with tempfile.NamedTemporaryFile() as temp_file:
        temp_file.write(b"Test content")
        temp_file_path = temp_file.name

        file_identifier = FileIdentifier(aid=4711, iid=1)

        with mock.patch.object(con_i, "file_access", return_value="http://example.com/upload"):
            with mock.patch.object(con_i._ConI__session, "put") as mock_post:
                mock_response = mock.Mock()
                mock_response.status_code = 500
                mock_response.headers = {}
                mock_response.raise_for_status.side_effect = requests.HTTPError("Failed to upload file")
                mock_post.return_value = mock_response

                with pytest.raises(requests.HTTPError, match="Failed to upload file"):
                    con_i.file_access_upload(file_identifier, temp_file_path)


def test_file_access_delete_success(con_i):
    file_identifier = FileIdentifier(aid=4711, iid=1)

    with mock.patch.object(con_i, "file_access", return_value="http://example.com/delete"):
        with mock.patch.object(con_i._ConI__session, "delete") as mock_delete:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_delete.return_value = mock_response

            con_i.file_access_delete(file_identifier)

            mock_delete.assert_called_once()
            assert mock_delete.call_args[0][0] == "http://example.com/delete"


def test_file_access_delete_failure(con_i):
    file_identifier = FileIdentifier(aid=4711, iid=1)

    with mock.patch.object(con_i, "file_access", return_value="http://example.com/delete"):
        with mock.patch.object(con_i._ConI__session, "delete") as mock_delete:
            mock_response = mock.Mock()
            mock_response.status_code = 404
            mock_response.headers = {}
            mock_response.raise_for_status.side_effect = requests.HTTPError("File not found")
            mock_delete.return_value = mock_response

            with pytest.raises(requests.HTTPError, match="File not found"):
                con_i.file_access_delete(file_identifier)
