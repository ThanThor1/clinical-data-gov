"""Kiểm thử Violation Engine: ghi violations, tìm bảng AI-ready, ghi audit."""

import pytest

from catalog_access import get_audit_logs
from governance.lineage import record_sample_lineage
from governance.violation_engine import (
    get_violations,
    infer_hospital,
    report_violation,
    summarize,
)

AI = "ai_ready_icu_mortality"


def test_suy_ra_benh_vien():
    assert infer_hospital("raw_patient_hospital_B") == "Bệnh viện B"
    assert infer_hospital("raw_vital_hospital_C") == "Bệnh viện C"
    assert infer_hospital("standardized_patients") == "Toàn hệ thống"


def test_ghi_vi_pham_chan_va_tim_bang_ai(db_path, capsys):
    record_sample_lineage(db_path)
    kq = report_violation("SCHEMA_COLUMN_RENAMED", "BREAKING", "raw_patient_hospital_B",
                          "Cột 'admissionweight' bị đổi tên thành 'weight'",
                          column="admissionweight", db_path=db_path)
    assert kq["affected_ai_tables"] == [AI]

    dong = get_violations(db_path)[0]
    assert dong["severity"] == "BREAKING"
    assert dong["hospital"] == "Bệnh viện B"
    assert dong["affected_table"] == "raw_patient_hospital_B"
    assert dong["affected_ai_table"] == AI
    assert dong["action"] == "đã chặn"
    assert "admissionweight" in dong["details"]

    audit = get_audit_logs(db_path=db_path)[0]
    assert (audit["ai"], audit["lam_gi"], audit["bang_nao"], audit["ket_qua"]) == (
        "violation_engine", "SCHEMA_COLUMN_RENAMED", "raw_patient_hospital_B", "BLOCKED")

    # In ra màn hình bảng AI-ready bị ảnh hưởng
    assert AI in capsys.readouterr().out


def test_canh_bao_khong_chan(db_path):
    record_sample_lineage(db_path)
    report_violation("PLAUSIBILITY", "WARNING", "raw_vital_hospital_C",
                     "1 dòng nhịp tim < 20", column="heartrate", blocked=False, db_path=db_path)
    dong = get_violations(db_path)[0]
    assert dong["action"] == "cảnh báo"
    assert get_audit_logs(db_path=db_path)[0]["ket_qua"] == "SUCCESS"


def test_nhieu_bang_ai_noi_bang_dau_phay(db_path):
    from governance.lineage import record_lineage
    record_lineage("raw_x", "ai_ready_a", {"c": "c"}, db_path=db_path)
    record_lineage("raw_x", "ai_ready_b", {"c": "c"}, db_path=db_path)
    report_violation("COMPLETENESS", "BREAKING", "raw_x", "thiếu", column="c", db_path=db_path)
    assert get_violations(db_path)[0]["affected_ai_table"] == "ai_ready_a,ai_ready_b"


def test_muc_do_sai_bi_tu_choi(db_path):
    with pytest.raises(ValueError):
        report_violation("X", "NGHIEM_TRONG", "raw_x", "...", db_path=db_path)


def test_thong_ke(db_path):
    report_violation("COMPLETENESS", "BREAKING", "raw_patient_hospital_A", "a", db_path=db_path)
    report_violation("PLAUSIBILITY", "WARNING", "raw_vital_hospital_C", "b",
                     blocked=False, db_path=db_path)
    tk = summarize(db_path, in_ra=False)
    assert tk["tong"] == 2
    assert tk["theo_muc_do"] == {"BREAKING": 1, "WARNING": 1}
    assert tk["theo_benh_vien"] == {"Bệnh viện A": 1, "Bệnh viện C": 1}
    assert len(get_violations(db_path, severity="warning")) == 1
