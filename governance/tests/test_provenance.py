"""Kiểm thử provenance: lần chạy, lô dữ liệu, phiên bản đầu ra, truy ngược, kiểm tra checksum."""

import hashlib

import pytest

from catalog_access import get_audit_context, get_audit_logs
from governance import provenance as pv
from governance.lineage import record_sample_lineage
from governance.violation_engine import get_violations, report_violation

AI = "ai_ready_icu_mortality"


def _file(thu_muc, ten, noi_dung):
    duong_dan = thu_muc / ten
    duong_dan.write_text(noi_dung, encoding="utf-8")
    return str(duong_dan)


@pytest.fixture
def chuoi_3_tang(tmp_path, db_path):
    """2 lô raw -> 1 phiên bản standardized -> 1 phiên bản AI-ready, tất cả trong một lần chạy."""
    raw_1 = _file(tmp_path, "raw_1.csv", "patientunitstayid,age\n1,40\n")
    raw_2 = _file(tmp_path, "raw_2.csv", "patientunitstayid,age\n2,50\n")
    std = _file(tmp_path, "std.csv", "patient_id,age\n1,40\n2,50\n")
    ai = _file(tmp_path, "ai.csv", "patient_hash,age\naa,40\nbb,50\n")
    with pv.pipeline_run("pipeline_service", "thử", db_path) as run_id:
        lo_1 = pv.record_batch("raw_patient_hospital_A", "Bệnh viện A", "hospital_A/dot_1", raw_1, 1, "ACCEPTED",
                               schema_version=1, contract="raw_patient v1.0", db_path=db_path)
        lo_2 = pv.record_batch("raw_patient_hospital_B", "Bệnh viện B", "hospital_B/dot_1", raw_2, 1, "ACCEPTED",
                               db_path=db_path)
        ma_std = pv.record_dataset_version("standardized_patients", "standardized", std, 2,
                                           [("batch", lo_1), ("batch", lo_2)], db_path=db_path)
        ma_ai = pv.record_dataset_version(AI, "ai_ready", ai, 2, [("dataset_version", ma_std)], db_path=db_path)
    return {"run_id": run_id, "lo": [lo_1, lo_2], "std": ma_std, "ai": ma_ai,
            "file": {"raw_1": raw_1, "raw_2": raw_2, "std": std, "ai": ai}}


def test_checksum_file(tmp_path):
    duong_dan = _file(tmp_path, "x.csv", "a,b\n1,2\n")
    assert pv.file_checksum(duong_dan) == hashlib.sha256(b"a,b\n1,2\n").hexdigest()


def test_vong_doi_lan_chay(db_path, chuoi_3_tang):
    run = pv.get_runs(db_path=db_path)[0]
    assert run["run_id"] == chuoi_3_tang["run_id"] and run["actor"] == "pipeline_service"
    assert run["status"] == "SUCCESS" and run["finished_at"] and run["note"] == "thử"
    assert isinstance(run["code_version"], str)
    lam_gi = [a["lam_gi"] for a in get_audit_logs(run_id=run["run_id"], db_path=db_path)]
    assert lam_gi == ["finish_run:SUCCESS", "start_run"]
    assert get_audit_context()["run_id"] == ""                 # ra khỏi khối thì ngữ cảnh được trả lại


def test_lan_chay_long_nhau_dung_chung_run_id(db_path):
    with pv.pipeline_run("pipeline_service", "ngoài", db_path) as ngoai:
        with pv.pipeline_run("pipeline_service", "trong", db_path) as trong:
            assert trong == ngoai == pv.current_run_id()
    assert len(pv.get_runs(db_path=db_path)) == 1


def test_loi_bat_ngo_thi_FAILED(db_path):
    with pytest.raises(RuntimeError):
        with pv.pipeline_run("pipeline_service", "sẽ lỗi", db_path):
            raise RuntimeError("hỏng")
    assert pv.get_runs(db_path=db_path)[0]["status"] == "FAILED"
    assert pv.current_run_id() == ""


def test_khong_co_dau_ra_thi_BLOCKED(db_path):
    with pv.pipeline_run("pipeline_service", "không ghi gì", db_path):
        pass
    assert pv.get_runs(db_path=db_path)[0]["status"] == "BLOCKED"


def test_co_vi_pham_chan_thi_PARTIAL_va_vi_pham_gan_run_lo(tmp_path, db_path):
    record_sample_lineage(db_path)
    std = _file(tmp_path, "std.csv", "patient_id\n1\n")
    with pv.pipeline_run("pipeline_service", "một lô lỗi", db_path) as run_id:
        with pv.batch_context("hospital_B/2026-10-29_20-00-00"):
            report_violation("SCHEMA_COLUMN_RENAMED", "BREAKING", "raw_patient_hospital_B", "đổi tên",
                             column="admissionweight", db_path=db_path)
        pv.record_dataset_version("standardized_patients", "standardized", std, 1, [], db_path=db_path)
    assert pv.get_runs(db_path=db_path)[0]["status"] == "PARTIAL"
    v = get_violations(db_path, run_id=run_id)[0]
    assert (v["run_id"], v["batch"]) == (run_id, "hospital_B/2026-10-29_20-00-00")


def test_ghi_lo_va_phien_ban(db_path, chuoi_3_tang):
    lo = pv.get_batches(chuoi_3_tang["run_id"], db_path)
    assert [(b["table_name"], b["batch"], b["status"], b["row_count"]) for b in lo] == [
        ("raw_patient_hospital_A", "hospital_A/dot_1", "ACCEPTED", 1),
        ("raw_patient_hospital_B", "hospital_B/dot_1", "ACCEPTED", 1),
    ]
    assert lo[0]["checksum"] == pv.file_checksum(chuoi_3_tang["file"]["raw_1"])
    assert (lo[0]["schema_version"], lo[0]["contract"]) == (1, "raw_patient v1.0")
    assert [v["table_name"] for v in pv.get_dataset_versions(db_path=db_path)] == [AI, "standardized_patients"]
    assert pv.get_latest_version(AI, db_path)["id"] == chuoi_3_tang["ai"]
    assert pv.get_latest_version("bang_khong_co", db_path) is None


def test_truy_nguoc_ve_lo_nguon(db_path, chuoi_3_tang):
    cay = pv.trace_version(chuoi_3_tang["ai"], db_path)
    assert cay["version"]["table_name"] == AI
    assert cay["inputs"][0]["version"]["table_name"] == "standardized_patients"
    assert [b["id"] for b in pv.source_batches(chuoi_3_tang["ai"], db_path)] == chuoi_3_tang["lo"]
    dong = pv.format_trace(cay)
    assert dong[0].startswith(f"{AI} #{chuoi_3_tang['ai']}") and sum("└─ lô" in d for d in dong) == 2
    assert pv.trace_version(9999, db_path) is None


def test_kiem_tra_lai_checksum(db_path, chuoi_3_tang):
    kq = pv.verify_version(chuoi_3_tang["ai"], db_path)
    assert kq == {"ok": True, "da_kiem": 4, "van_de": []}

    with open(chuoi_3_tang["file"]["raw_2"], "a", encoding="utf-8") as f:
        f.write("3,60\n")
    kq = pv.verify_version(chuoi_3_tang["ai"], db_path)
    assert kq["ok"] is False and len(kq["van_de"]) == 1 and "raw_2.csv" in kq["van_de"][0]

    import os
    os.remove(chuoi_3_tang["file"]["std"])
    assert any("không còn file" in v for v in pv.verify_version(chuoi_3_tang["ai"], db_path)["van_de"])
    assert pv.verify_version(9999, db_path)["ok"] is False


def test_cong_provenance(db_path, chuoi_3_tang):
    record_sample_lineage(db_path)
    duong_dan = chuoi_3_tang["file"]["std"]
    assert pv.verify_input("standardized_patients", duong_dan, db_path) == chuoi_3_tang["std"]

    with open(duong_dan, "a", encoding="utf-8") as f:
        f.write("3,999\n")
    assert pv.verify_input("standardized_patients", duong_dan, db_path) is None
    v = get_violations(db_path)[0]
    assert (v["error_type"], v["severity"], v["affected_ai_table"]) == ("PROVENANCE_UNVERIFIED", "BREAKING", AI)
