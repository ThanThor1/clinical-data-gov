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


# ---------------------------------------------------------------------------
# Vòng đời vi phạm: OPEN -> RESOLVED / SUPERSEDED khi lần chạy sau kết thúc
# ---------------------------------------------------------------------------

def _lan_chay(tmp_path, db_path, ten, lo_qua_cong=(), loi=()):
    """Một lần chạy giả: các lô (bảng, lô) qua cổng và các vi phạm (loại, bảng, lô) ghi trong lần chạy đó."""
    from catalog_access import audit_context
    from governance import provenance as pv

    tep = tmp_path / f"{ten}.csv"
    tep.write_text("a\n1\n", encoding="utf-8")
    with pv.pipeline_run("pipeline_service", ten, db_path) as run_id:
        for loai, bang, lo in loi:
            with audit_context(batch=lo):
                report_violation(loai, "BREAKING", bang, "lỗi", db_path=db_path, in_ra=False)
        for bang, lo in lo_qua_cong:
            pv.record_batch(bang, "Bệnh viện B", lo, str(tep), 1, "ACCEPTED", db_path=db_path)
    return run_id


def test_vi_pham_moi_dang_mo(tmp_path, db_path):
    _lan_chay(tmp_path, db_path, "lan_1", loi=[("SCHEMA_COLUMN_RENAMED", "raw_patient_hospital_B", "hospital_B/dot_3")])
    assert [v["status"] for v in get_violations(db_path)] == ["OPEN"]


def test_lo_qua_cong_o_lan_sau_thi_resolved(tmp_path, db_path):
    lo = ("raw_patient_hospital_B", "hospital_B/dot_3")
    _lan_chay(tmp_path, db_path, "lan_1", loi=[("SCHEMA_COLUMN_RENAMED", *lo)])
    run_2 = _lan_chay(tmp_path, db_path, "lan_2", lo_qua_cong=[lo])
    v = get_violations(db_path)[0]
    assert (v["status"], v["resolved_run_id"]) == ("RESOLVED", run_2)
    assert "qua cổng" in v["resolution"] and v["resolved_at"]


def test_loi_lap_lai_thi_dong_cu_superseded(tmp_path, db_path):
    loi = ("PLAUSIBILITY", "raw_vital_hospital_B", "hospital_B/dot_1")
    _lan_chay(tmp_path, db_path, "lan_1", loi=[loi])
    _lan_chay(tmp_path, db_path, "lan_2", loi=[loi])
    moi, cu = get_violations(db_path)
    assert (moi["status"], cu["status"]) == ("OPEN", "SUPERSEDED")
    assert len(get_violations(db_path, status="open")) == 1


def test_lo_chua_kiem_tra_lai_van_mo(tmp_path, db_path):
    _lan_chay(tmp_path, db_path, "lan_1", loi=[("COMPLETENESS", "raw_patient_hospital_B", "hospital_B/dot_3")])
    _lan_chay(tmp_path, db_path, "lan_2", lo_qua_cong=[("raw_patient_hospital_B", "hospital_B/dot_1")])
    assert get_violations(db_path)[0]["status"] == "OPEN"


def test_vi_pham_muc_bang_resolved_khi_co_phien_ban_moi(tmp_path, db_path):
    from governance import provenance as pv

    _lan_chay(tmp_path, db_path, "lan_1", loi=[("PROVENANCE_UNVERIFIED", "standardized_patients", "")])
    tep = tmp_path / "std.csv"
    tep.write_text("a\n1\n", encoding="utf-8")
    with pv.pipeline_run("pipeline_service", "lan_2", db_path):
        pv.record_dataset_version("standardized_patients", "standardized", str(tep), 1, [], db_path=db_path)
    assert get_violations(db_path)[0]["status"] == "RESOLVED"
    assert summarize(db_path, in_ra=False)["theo_trang_thai"] == {"RESOLVED": 1}
