"""
Kiểm thử pipeline đầu-cuối trên dữ liệu GIẢ (governance/sample_data.py), không cần dữ liệu thật:
staging -> raw (3 đợt mỗi viện) -> standardized -> AI-ready, kèm lỗi giả lập của inject_errors.
Mọi thứ nằm trong thư mục tạm và metadata.db tạm (fixture `mt` ở conftest.py gốc repo).
"""

import numpy as np
import pandas as pd
import pytest

from catalog_access import AccessDenied, get_audit_logs, get_dataset, read_table
from governance import provenance as pv
from governance.violation_engine import get_violations
from pipeline import build_ai_ready, inject_errors, standardize

AI = "ai_ready_icu_mortality"


def _chay(mt):
    kq = standardize.main(mt["db"], mt["raw"], mt["std"])
    ai_df = build_ai_ready.main(mt["db"], mt["std"], mt["ai"])
    return kq, ai_df


def _dot_3(mt, vien):
    return pd.read_csv(f"{inject_errors.get_batch_3_dir(vien, mt['raw'])}/patient.csv")


# ---------------------------------------------------------------------------
# Dữ liệu sạch
# ---------------------------------------------------------------------------

def test_dang_ky_9_bang_raw(mt):
    for vien in "ABC":
        ds = get_dataset(f"raw_patient_hospital_{vien}", mt["db"])
        assert ds["tier"] == "raw" and ds["file_path"].endswith("*/patient.csv")
        assert ds["contract"] == "raw_patient v1.0" and "patientunitstayid" in ds["sensitive_columns"]
    assert get_dataset("raw_patient_hospital_A", mt["db"])["row_count"] == 40
    assert get_dataset("raw_vital_hospital_B", mt["db"])["row_count"] == 0


def test_du_lieu_sach_di_qua_3_tang(mt):
    kq, ai_df = _chay(mt)
    assert (kq["so_dot"], kq["so_dot_dat"], kq["bi_chan"], kq["so_dong"]) == (9, 9, [], 95)
    assert get_violations(mt["db"], severity="BREAKING") == []

    assert list(ai_df.columns) == build_ai_ready.COT_AI and len(ai_df) == 95
    assert ai_df["patient_hash"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert ai_df["creatinine_max"].notna().any() and ai_df["glucose_mean"].notna().any()

    std = pd.read_csv(f"{mt['std']}/standardized_patients.csv")
    assert std["source_batch"].str.match(r"hospital_[ABC]/").all() and std["source_batch"].nunique() == 9
    for bang, tang in (("standardized_patients", "standardized"), ("standardized_labs", "standardized"),
                       (AI, "ai_ready")):
        ds = get_dataset(bang, mt["db"])
        assert ds["tier"] == tang and ds["row_count"] > 0 and ds["file_path"].endswith(f"{bang}.csv")
    assert {r["status"] for r in pv.get_runs(db_path=mt["db"])} == {"SUCCESS"}


def test_provenance_truy_nguoc_ve_27_lo(mt):
    _chay(mt)
    phien_ban = pv.get_latest_version(AI, mt["db"])
    lo = pv.source_batches(phien_ban["id"], mt["db"])
    # 3 viện x 3 đợt x 3 loại bảng (vital viện B rỗng vẫn được ghi lại)
    assert len(lo) == 27 and {b["status"] for b in lo} == {"ACCEPTED"}
    assert {b["table_name"] for b in lo if b["row_count"] == 0} == {"raw_vital_hospital_B"}
    assert pv.verify_version(phien_ban["id"], mt["db"]) == {"ok": True, "da_kiem": 30, "van_de": []}


def test_pipeline_doc_ghi_qua_kiem_soat_quyen(mt):
    _chay(mt)
    audit = get_audit_logs(ai="pipeline_service", limit=10_000, db_path=mt["db"])
    lam_gi = {a["lam_gi"] for a in audit}
    assert {"read:raw", "write:standardized", "read:standardized", "write:ai_ready"} <= lam_gi
    assert {a["ket_qua"] for a in audit if ":" in a["lam_gi"] and a["lam_gi"].startswith(("read", "write"))} == {
        "ALLOWED"}


def test_tai_khoan_khong_co_quyen_thi_pipeline_dung(mt, monkeypatch):
    monkeypatch.setattr(standardize, "ACTOR", "researcher_bob")
    with pytest.raises(AccessDenied):
        standardize.main(mt["db"], mt["raw"], mt["std"])
    assert pv.get_runs(db_path=mt["db"])[0]["status"] == "FAILED"
    assert get_audit_logs(ket_qua="DENIED", db_path=mt["db"])[0]["ai"] == "researcher_bob"


def test_sau_khi_chay_nguoi_dung_doc_theo_quyen(mt):
    _chay(mt)
    assert len(read_table("researcher_bob", AI, db_path=mt["db"])) == 95
    assert "patient_id" not in read_table("analyst_dave", "standardized_patients", db_path=mt["db"]).columns
    with pytest.raises(AccessDenied):
        read_table("researcher_bob", "standardized_patients", db_path=mt["db"])


# ---------------------------------------------------------------------------
# Lỗi giả lập (inject_errors) trên dữ liệu giả
# ---------------------------------------------------------------------------

def test_doi_ten_cot_vien_B_bi_chan_dung_lo(mt):
    _chay(mt)
    so_bn_dot_3 = len(_dot_3(mt, "B"))
    inject_errors.gay_loi("doi_ten", mt["raw"], mt["backup"])
    kq, ai_df = _chay(mt)

    assert len(kq["bi_chan"]) == 1 and kq["bi_chan"][0].startswith("raw_patient_hospital_B / ")
    assert len(ai_df) == 95 - so_bn_dot_3
    run_id = kq["run_id"]
    v = get_violations(mt["db"], severity="BREAKING", run_id=run_id)
    assert [(x["error_type"], x["affected_ai_table"]) for x in v] == [("SCHEMA_COLUMN_RENAMED", AI)]
    assert v[0]["batch"].startswith("hospital_B/") and v[0]["batch"].endswith("20-00-00")
    lo_b = {(b["table_name"], b["status"]) for b in pv.get_batches(run_id, mt["db"])
            if b["batch"] == v[0]["batch"]}
    assert lo_b == {("raw_patient_hospital_B", "BLOCKED"), ("raw_vital_hospital_B", "SKIPPED"),
                    ("raw_lab_hospital_B", "SKIPPED")}
    # Lần chạy chuẩn hóa có lô bị chặn nhưng vẫn ra được bảng -> PARTIAL
    assert next(r["status"] for r in pv.get_runs(db_path=mt["db"]) if r["run_id"] == run_id) == "PARTIAL"

    inject_errors.sua_lai("B", mt["raw"], mt["backup"])
    kq, ai_df = _chay(mt)
    assert kq["bi_chan"] == [] and len(ai_df) == 95


def test_nhip_tim_500_chan_lo_vital_giu_benh_nhan(mt):
    _chay(mt)
    inject_errors.gay_loi("nhip_tim_500", mt["raw"], mt["backup"])
    kq, ai_df = _chay(mt)
    assert kq["bi_chan"] == [f"raw_vital_hospital_A / {kq['bi_chan'][0].split(' / ')[1]}"]
    assert len(ai_df) == 95 and ai_df["heartrate_mean"].max() <= 300
    v = get_violations(mt["db"], severity="BREAKING", run_id=kq["run_id"])
    assert [x["error_type"] for x in v] == ["PLAUSIBILITY"]


@pytest.mark.parametrize("loi, bang, loai_vi_pham", [
    ("doi_kieu", "raw_patient_hospital_A", "SCHEMA_TYPE_CHANGED"),
    ("xoa_cot", "raw_patient_hospital_C", "SCHEMA_COLUMN_REMOVED"),
    ("thieu_can_nang", "raw_patient_hospital_B", "COMPLETENESS"),
])
def test_cac_loi_con_lai_deu_bi_chan(mt, loi, bang, loai_vi_pham):
    _chay(mt)
    vien = inject_errors.gay_loi(loi, mt["raw"], mt["backup"])
    so_bn_dot_3 = len(_dot_3(mt, vien))
    kq, ai_df = _chay(mt)
    v = get_violations(mt["db"], severity="BREAKING", run_id=kq["run_id"])
    assert [(x["error_type"], x["affected_table"]) for x in v] == [(loai_vi_pham, bang)]
    assert len(ai_df) == 95 - so_bn_dot_3


def test_file_standardized_sua_tay_bi_chan(mt):
    _chay(mt)
    duong_dan = f"{mt['std']}/standardized_patients.csv"
    df = pd.read_csv(duong_dan)
    df.loc[0, "weight_kg"] = 999.0
    df.to_csv(duong_dan, index=False)
    assert build_ai_ready.main(mt["db"], mt["std"], mt["ai"]) is None
    v = get_violations(mt["db"], severity="BREAKING")[0]
    assert (v["error_type"], v["affected_table"]) == ("PROVENANCE_UNVERIFIED", "standardized_patients")
    assert pv.get_runs(db_path=mt["db"])[0]["status"] == "BLOCKED"


# ---------------------------------------------------------------------------
# Các hàm biến đổi
# ---------------------------------------------------------------------------

def test_clean_patient():
    raw = pd.DataFrame({"patientunitstayid": [1, 2, 3], "age": ["> 89", "45", None],
                        "gender": ["Male", "Female", "Other"], "admissionweight": [70.5, None, 80.0],
                        "unitdischargestatus": ["Alive", "Expired", "Alive"], "hospitalid": [146] * 3})
    df = standardize.clean_patient(raw, "A", "hospital_A/dot_1")
    assert list(df["age"].fillna(-1)) == [90, 45, -1]
    assert list(df["gender"]) == ["M", "F", "U"]
    assert set(df["source_batch"]) == {"hospital_A/dot_1"} and set(df["source_hospital"]) == {"A"}
    assert "patientunitstayid" not in df.columns and "weight_kg" in df.columns


def test_dac_trung_xet_nghiem():
    lab = pd.DataFrame({"patient_id": [1, 1, 1, 2, 3], "lab_name": ["creatinine", "creatinine", "glucose",
                                                                    "glucose", "sodium"],
                        "lab_value": [1.2, 2.5, 100.0, 150.0, 140.0]})
    dt = build_ai_ready.calc_lab_features(lab).set_index("patient_id")
    assert dt.loc[1, "creatinine_max"] == 2.5 and dt.loc[1, "glucose_mean"] == 100.0
    assert np.isnan(dt.loc[2, "creatinine_max"]) and dt.loc[2, "glucose_mean"] == 150.0
    assert 3 not in dt.index                                  # chỉ có sodium -> không có đặc trưng
    assert build_ai_ready.calc_lab_features(None).empty


def test_build_ai_ready_khong_co_lab_va_khong_lo_patient_id():
    std = pd.DataFrame({"patient_id": [1, 2], "age": [40, 90], "gender": ["M", "F"], "weight_kg": [70.0, None],
                        "heartrate_mean": [80.0, None], "unitdischargestatus": ["Alive", "Expired"]})
    ai = build_ai_ready.build_ai_ready(std, None, secret=b"khoa-kiem-thu-0123456789")
    assert list(ai.columns) == build_ai_ready.COT_AI
    assert list(ai["icu_mortality"]) == [0, 1] and ai["creatinine_max"].isna().all()
    assert "patient_id" not in ai.columns and "patient_id" in std.columns   # không sửa df gốc
