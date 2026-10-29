"""Kiểm thử catalog và lớp đọc/ghi dữ liệu có kiểm soát quyền (read_table / write_table)."""

import os

import pandas as pd
import pytest

from catalog_access import (
    AccessDenied,
    delete_dataset,
    get_audit_logs,
    get_dataset,
    list_datasets,
    read_table,
    register_dataset,
    write_table,
)
from catalog_access.data_access import ROOT, from_catalog_path, to_catalog_path


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def test_dang_ky_lai_giu_thong_tin_cu_va_cung_id(db_path):
    ma_1 = register_dataset("standardized_patients", "Toàn hệ thống", "standardized", "Nhóm KT",
                            "patient_id", db_path=db_path, description="Bệnh nhân chuẩn hóa",
                            contract="standardized_patients v1.1",
                            file_path="data/standardized/standardized_patients.csv", row_count=95)
    ma_2 = register_dataset("standardized_patients", "Toàn hệ thống", "standardized", "Nhóm KT",
                            "patient_id", db_path=db_path, row_count=85)
    assert ma_1 == ma_2
    ds = get_dataset("standardized_patients", db_path)
    assert ds["row_count"] == 85
    assert ds["file_path"] == "data/standardized/standardized_patients.csv"
    assert ds["description"] == "Bệnh nhân chuẩn hóa" and ds["contract"] == "standardized_patients v1.1"
    assert ds["updated_at"]
    assert [d["lam_gi"] for d in get_audit_logs(db_path=db_path)] == ["register_dataset"] * 2


def test_tang_sai_bi_tu_choi(db_path):
    with pytest.raises(ValueError):
        register_dataset("x", "BV", "bronze", "ai đó", db_path=db_path)


def test_liet_ke_va_xoa(db_path):
    register_dataset("raw_patient_hospital_A", "Bệnh viện A", "raw", "IT A", db_path=db_path)
    register_dataset("raw_patient_hospital_B", "Bệnh viện B", "raw", "IT B", db_path=db_path)
    register_dataset("ai_ready_icu_mortality", "Toàn hệ thống", "ai_ready", "AI", db_path=db_path)
    assert len(list_datasets(db_path=db_path)) == 3
    assert [d["name"] for d in list_datasets(tier="raw", db_path=db_path)] == [
        "raw_patient_hospital_A", "raw_patient_hospital_B"]
    assert [d["name"] for d in list_datasets(hospital="viện B", db_path=db_path)] == ["raw_patient_hospital_B"]
    assert len(list_datasets(db_path=db_path, as_df=True)) == 3

    assert delete_dataset("raw_patient_hospital_B", db_path=db_path) is True
    assert delete_dataset("raw_patient_hospital_B", db_path=db_path) is False
    assert get_dataset("raw_patient_hospital_B", db_path) is None
    assert get_audit_logs(db_path=db_path)[0]["lam_gi"] == "delete_dataset"


# ---------------------------------------------------------------------------
# Đường dẫn trong catalog
# ---------------------------------------------------------------------------

def test_duong_dan_catalog_tuong_doi_trong_repo(tmp_path):
    trong_repo = os.path.join(ROOT, "data", "standardized", "x.csv")
    assert to_catalog_path(trong_repo) == os.path.join("data", "standardized", "x.csv")
    assert from_catalog_path(to_catalog_path(trong_repo)) == trong_repo
    ngoai_repo = str(tmp_path / "x.csv")
    assert to_catalog_path(ngoai_repo) == ngoai_repo and from_catalog_path(ngoai_repo) == ngoai_repo


# ---------------------------------------------------------------------------
# read_table / write_table
# ---------------------------------------------------------------------------

@pytest.fixture
def bang_raw_hai_dot(tmp_path, db_path):
    """raw_patient_hospital_A gồm 2 đợt, đăng ký catalog bằng mẫu đường dẫn '*'."""
    for dot, ma in (("dot_1", [1, 2]), ("dot_2", [3])):
        thu_muc = tmp_path / "raw" / "hospital_A" / dot
        thu_muc.mkdir(parents=True)
        pd.DataFrame({"patientunitstayid": ma, "age": ["40", "> 89", "50"][: len(ma)]}).to_csv(
            thu_muc / "patient.csv", index=False)
    register_dataset("raw_patient_hospital_A", "Bệnh viện A", "raw", "IT A", "patientunitstayid",
                     db_path=db_path, file_path=str(tmp_path / "raw" / "hospital_A" / "*" / "patient.csv"))
    return "raw_patient_hospital_A"


def test_read_table_doc_moi_dot_theo_mau(db_path, bang_raw_hai_dot):
    df = read_table("engineer_alice", bang_raw_hai_dot, db_path=db_path)
    assert list(df["patientunitstayid"]) == [1, 2, 3]       # kỹ sư dữ liệu thấy cả cột định danh


def test_read_table_bi_tu_choi_khong_doc_file(db_path, bang_raw_hai_dot):
    with pytest.raises(AccessDenied):
        read_table("researcher_bob", bang_raw_hai_dot, db_path=db_path)
    audit = get_audit_logs(db_path=db_path)[0]
    assert (audit["ai"], audit["lam_gi"], audit["ket_qua"]) == ("researcher_bob", "read:raw", "DENIED")


def test_read_table_an_cot_nhay_cam_va_ghi_audit(tmp_path, db_path):
    duong_dan = tmp_path / "standardized_patients.csv"
    pd.DataFrame({"patient_id": [1, 2], "age": [40, 90], "gender": ["M", "F"]}).to_csv(duong_dan, index=False)
    register_dataset("standardized_patients", "Toàn hệ thống", "standardized", "Nhóm KT", "patient_id",
                     db_path=db_path, file_path=str(duong_dan))

    df = read_table("analyst_dave", "standardized_patients", db_path=db_path)
    assert list(df.columns) == ["age", "gender"]
    assert get_audit_logs(db_path=db_path)[0]["lam_gi"] == "mask:patient_id"
    assert "patient_id" in read_table("engineer_alice", "standardized_patients", db_path=db_path).columns


def test_write_table_kiem_tra_quyen_ghi(tmp_path, db_path):
    df = pd.DataFrame({"patient_hash": ["a" * 64], "age": [40]})
    dich = tmp_path / "ai" / "ai_ready_icu_mortality.csv"
    with pytest.raises(AccessDenied):
        write_table("researcher_bob", "ai_ready_icu_mortality", df, path=str(dich), db_path=db_path)
    assert not dich.exists()

    assert write_table("pipeline_service", "ai_ready_icu_mortality", df, path=str(dich), db_path=db_path) == str(dich)
    assert pd.read_csv(dich).equals(df)
    assert get_audit_logs(db_path=db_path)[0]["lam_gi"] == "write:ai_ready"


def test_write_table_theo_vi_tri_catalog(tmp_path, db_path, bang_raw_hai_dot):
    dich = tmp_path / "std" / "standardized_labs.csv"
    register_dataset("standardized_labs", "Toàn hệ thống", "standardized", "Nhóm KT", db_path=db_path,
                     file_path=str(dich))
    write_table("pipeline_service", "standardized_labs", pd.DataFrame({"a": [1]}), db_path=db_path)
    assert dich.exists()
    # Bảng nhiều đợt (mẫu '*') thì phải chỉ rõ file khi ghi
    with pytest.raises(ValueError):
        write_table("pipeline_service", bang_raw_hai_dot, pd.DataFrame({"a": [1]}), db_path=db_path)


def test_bang_chua_co_vi_tri_file(db_path):
    with pytest.raises(FileNotFoundError):
        read_table("engineer_alice", "standardized_chua_dang_ky", db_path=db_path)
    register_dataset("standardized_rong", "Toàn hệ thống", "standardized", "Nhóm KT", db_path=db_path,
                     file_path="data/khong_ton_tai/*.csv")
    with pytest.raises(FileNotFoundError):
        read_table("engineer_alice", "standardized_rong", db_path=db_path)
