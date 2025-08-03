# Negative type-check tests for ConI methods
import pytest
from odsbox.con_i import ConI
from odsbox.proto.ods_pb2 import DataMatrix


@pytest.fixture
def dummy_con_i():
    # create without running __init__
    con_i = ConI.__new__(ConI)
    con_i._ConI__session = None  # type: ignore
    return con_i


# Negative type-check tests


def test_data_read_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.data_read("invalid")
    assert "data_read expects 'ods.SelectStatement'" in str(exc.value)


def test_data_create_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.data_create(123)
    assert "data_create expects 'ods.DataMatrices'" in str(exc.value)


def test_data_update_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.data_update(DataMatrix())
    assert "data_update expects 'ods.DataMatrices'" in str(exc.value)


def test_data_delete_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.data_delete([])
    assert "data_delete expects 'ods.DataMatrices'" in str(exc.value)


def test_data_copy_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.data_copy({})
    assert "data_copy expects 'ods.CopyRequest'" in str(exc.value)


def test_n_m_relation_read_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.n_m_relation_read(3.14)
    assert "n_m_relation_read expects 'ods.NtoMRelationIdentifier'" in str(exc.value)


def test_n_m_relation_write_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.n_m_relation_write("wrong")
    assert "n_m_relation_write expects 'ods.NtoMWriteRelatedInstances'" in str(exc.value)


def test_valuematrix_read_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.valuematrix_read(())
    assert "valuematrix_read expects 'ods.ValueMatrixRequestStruct'" in str(exc.value)


def test_model_update_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.model_update([])
    assert "model_update expects 'ods.Model'" in str(exc.value)


def test_model_delete_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.model_delete(0)
    assert "model_delete expects 'ods.Model'" in str(exc.value)


def test_asampath_create_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.asampath_create(42)
    assert "asampath_create expects 'ods.Instance'" in str(exc.value)


def test_asampath_resolve_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.asampath_resolve(None)
    assert "asampath_resolve expects 'ods.AsamPath'" in str(exc.value)


def test_context_update_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.context_update(5)
    assert "context_update expects 'ods.ContextVariables'" in str(exc.value)


def test_password_update_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.password_update({})
    assert "password_update expects 'ods.PasswordUpdate'" in str(exc.value)


def test_file_access_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.file_access(())
    assert "file_access expects 'ods.FileIdentifier'" in str(exc.value)


def test_file_access_download_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.file_access_download((), "/tmp")
    assert "file_access_download expects 'ods.FileIdentifier'" in str(exc.value)


def test_file_access_upload_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.file_access_upload("not-file-id", "/tmp/nonexistent")
    assert "file_access_upload expects 'ods.FileIdentifier'" in str(exc.value)


def test_file_access_delete_type_error(dummy_con_i):
    with pytest.raises(TypeError) as exc:
        dummy_con_i.file_access_delete(123)
    assert "file_access_delete expects 'ods.FileIdentifier'" in str(exc.value)
