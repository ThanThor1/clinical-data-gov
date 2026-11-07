"""Kiểm thử Schema Registry: phiên bản tăng dần và phát hiện thay đổi."""

import json

import pandas as pd
import pytest

from catalog_access.init_db import get_db_connection
from governance.sample_data import make_patient, make_vital
from governance.schema_registry import (
    compare_schemas,
    get_latest_schema,
    get_schema_history,
    logical_type,
    register_schema,
)

BANG = "raw_patient_hospital_B"


def _loai_thay_doi(kq):
    return [t["change_type"] for t in kq["changes"]]


def test_kieu_logic():
    assert logical_type(pd.Series([1, 2])) == "int"
    assert logical_type(pd.Series([1.5, None])) == "float"
    assert logical_type(pd.Series(["45", "> 89", None])) == "string"
    assert logical_type(pd.Series([None, None])) == "unknown"
    assert logical_type(pd.Series([], dtype=object)) == "unknown"


def test_lan_dau_tao_version_1(db_path):
    kq = register_schema(BANG, make_patient("B"), db_path)
    assert kq["version"] == 1
    assert kq["change_type"] == "INITIAL"
    assert kq["changes"] == [] and kq["is_breaking"] is False


def test_khong_doi_thi_khong_tao_version_moi(db_path):
    df = make_patient("B")
    register_schema(BANG, df, db_path)
    kq = register_schema(BANG, df, db_path)
    assert kq["change_type"] == "NO_CHANGE"
    assert kq["version"] == 1
    assert len(get_schema_history(BANG, db_path)) == 1


def test_phat_hien_doi_ten_cot(db_path):
    df = make_patient("B")
    register_schema(BANG, df, db_path)
    kq = register_schema(BANG, df.rename(columns={"admissionweight": "weight"}), db_path)
    assert kq["version"] == 2
    assert kq["is_breaking"] is True
    doi_ten = [t for t in kq["changes"] if t["change_type"] == "COLUMN_RENAMED"]
    assert len(doi_ten) == 1
    assert (doi_ten[0]["column"], doi_ten[0]["new_column"]) == ("admissionweight", "weight")
    # Không bị báo nhầm thành xóa cột + thêm cột
    assert "COLUMN_REMOVED" not in _loai_thay_doi(kq)
    assert "COLUMN_ADDED" not in _loai_thay_doi(kq)


def test_lo_loi_khong_tro_thanh_chuan_moi(db_path):
    """Lô đổi tên (gây hỏng) vẫn được lưu lại, nhưng các lô sau so với phiên bản hợp lệ v1."""
    df = make_patient("B")
    loi = df.rename(columns={"admissionweight": "weight"})
    register_schema(BANG, df, db_path)                     # v1 hợp lệ
    assert register_schema(BANG, loi, db_path)["version"] == 2

    # Gửi lại lô lỗi lần nữa: vẫn báo đổi tên, nhưng không tạo version trùng
    kq = register_schema(BANG, loi, db_path)
    assert kq["version"] == 2 and kq["is_new_version"] is False
    assert [t["change_type"] for t in kq["changes"]] == ["COLUMN_RENAMED"]

    # Viện B sửa lỗi, gửi dữ liệu đúng: không bị báo "weight đổi tên thành admissionweight"
    kq = register_schema(BANG, df, db_path)
    assert kq["changes"] == [] and kq["baseline_version"] == 1
    assert kq["version"] == 3 and "khớp lại phiên bản chuẩn v1" in kq["change_type"]


def test_doi_ten_nhieu_cot_cung_luc_ghep_dung_cap(db_path):
    df = make_patient("A")
    register_schema("raw_patient_hospital_A", df, db_path)
    moi = df.rename(columns={"admissionweight": "w_kg", "admissionheight": "h_cm"})
    kq = register_schema("raw_patient_hospital_A", moi, db_path)
    cap = {(t["column"], t["new_column"]) for t in kq["changes"] if t["change_type"] == "COLUMN_RENAMED"}
    # Ghép theo phân phối giá trị (cân nặng ~80, chiều cao ~170) chứ không theo thứ tự
    assert cap == {("admissionweight", "w_kg"), ("admissionheight", "h_cm")}


def test_phat_hien_xoa_cot(db_path):
    df = make_patient("A")
    register_schema(BANG, df, db_path)
    kq = register_schema(BANG, df.drop(columns=["admissionweight"]), db_path)
    assert _loai_thay_doi(kq) == ["COLUMN_REMOVED"]
    assert kq["is_breaking"] is True


def test_phat_hien_doi_kieu(db_path):
    df = make_patient("A")
    register_schema(BANG, df, db_path)
    loi = df.copy()
    loi["admissionweight"] = loi["admissionweight"].map(lambda x: f"{x} kg")
    kq = register_schema(BANG, loi, db_path)
    assert _loai_thay_doi(kq) == ["TYPE_CHANGED"]
    assert kq["changes"][0]["old_type"] == "float"
    assert kq["changes"][0]["new_type"] == "string"


def test_them_cot_khong_gay_hong(db_path):
    df = make_patient("A")
    register_schema(BANG, df, db_path)
    moi = df.assign(cot_moi=1)
    kq = register_schema(BANG, moi, db_path)
    assert _loai_thay_doi(kq) == ["COLUMN_ADDED"]
    assert kq["is_breaking"] is False


def test_int_va_float_khong_tinh_la_doi_kieu(db_path):
    register_schema("t", pd.DataFrame({"x": [1, 2, 3]}), db_path)
    kq = register_schema("t", pd.DataFrame({"x": [1.0, None, 3.0]}), db_path)
    assert kq["change_type"] == "NO_CHANGE"


def test_bang_rong_cua_vien_B_khong_bao_loi(db_path):
    """Viện B có 0 dòng vital -> kiểu 'unknown'; đợt sau có dữ liệu cũng không bị báo đổi kiểu."""
    register_schema("raw_vital_hospital_B", make_vital("B"), db_path)
    kq = register_schema("raw_vital_hospital_B", make_vital("A"), db_path)
    assert kq["changes"] == []


def test_save_false_khong_ghi_db(db_path):
    register_schema(BANG, make_patient("B"), db_path, save=False)
    assert get_latest_schema(BANG, db_path) == (0, None)


def test_doc_duoc_dinh_dang_cu_cua_du_lieu_mau(db_path):
    """Dữ liệu mẫu của Người 3 lưu {"cot": "REAL"} -> vẫn so sánh được."""
    conn = get_db_connection(db_path)
    conn.execute(
        "INSERT INTO schema_versions (table_name, version, columns_json, change_type, is_breaking) "
        "VALUES (?, 1, ?, 'INITIAL', 0)",
        ("bang_cu", json.dumps({"id": "INTEGER", "ten": "TEXT"})),
    )
    conn.commit()
    conn.close()
    kq = register_schema("bang_cu", pd.DataFrame({"id": [1], "ten": ["a"]}), db_path)
    assert kq["change_type"] == "NO_CHANGE"


def test_compare_schemas_thuan():
    cu = {"a": {"type": "int"}, "b": {"type": "string"}}
    moi = {"a": {"type": "string"}, "b": {"type": "string"}}
    assert [t["change_type"] for t in compare_schemas(cu, moi)] == ["TYPE_CHANGED"]


# ---------------------------------------------------------------------------
# Duyệt thay đổi schema (đổi tên cột)
# ---------------------------------------------------------------------------

def test_duyet_doi_ten_cot_va_ghi_audit(db_path):
    from catalog_access import get_audit_logs
    from governance.schema_registry import approve_rename, get_schema_approvals

    ma = approve_rename("engineer_alice", "raw_patient_hospital_B", "weight", "admissionweight", "đổi tên hợp lệ",
                        db_path)
    assert [(a["id"], a["source_column"], a["target_column"], a["approved_by"])
            for a in get_schema_approvals("raw_patient_hospital_B", db_path)] == [
        (ma, "weight", "admissionweight", "engineer_alice")]
    assert [(d["lam_gi"], d["ket_qua"]) for d in get_audit_logs(db_path=db_path)] == [
        ("approve_rename:weight->admissionweight", "SUCCESS"), ("write:raw", "ALLOWED")]
    # Duyệt lại cùng cột thì cập nhật, không thêm dòng
    assert approve_rename("engineer_alice", "raw_patient_hospital_B", "weight", "admissionweight", db_path=db_path) == ma
    assert len(get_schema_approvals(db_path=db_path)) == 1


def test_khong_co_quyen_ghi_thi_khong_duoc_duyet(db_path):
    from catalog_access import AccessDenied
    from governance.schema_registry import approve_rename, get_schema_approvals

    with pytest.raises(AccessDenied):
        approve_rename("analyst_dave", "raw_patient_hospital_B", "weight", "admissionweight", db_path=db_path)
    assert get_schema_approvals(db_path=db_path) == []


@pytest.mark.parametrize("cot_nguon, cot_dich", [("weight", "khong_co_cot_nay"), ("gender", "admissionweight")])
def test_duyet_sai_cot_bi_tu_choi(db_path, cot_nguon, cot_dich):
    from governance.schema_registry import approve_rename

    with pytest.raises(ValueError):
        approve_rename("engineer_alice", "raw_patient_hospital_B", cot_nguon, cot_dich, db_path=db_path)
