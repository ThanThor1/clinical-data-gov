"""Kiểm thử khởi tạo metadata.db và nâng cấp DB tạo từ phiên bản cũ."""

import os
import sqlite3
import subprocess
import sys

from catalog_access import get_audit_logs, init_database, verify_audit_chain
from catalog_access.data_access import ROOT

CAC_BANG = {"datasets", "schema_versions", "lineage_edges", "violations", "audit_log", "pipeline_runs",
            "batch_ingestions", "dataset_versions", "dataset_version_inputs"}


def _cac_bang(db):
    conn = sqlite3.connect(db)
    ten = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    conn.close()
    return ten


def _cac_cot(db, bang):
    conn = sqlite3.connect(db)
    cot = [r[1] for r in conn.execute(f"PRAGMA table_info({bang})")]
    conn.close()
    return cot


def test_tao_du_cac_bang(db_path):
    assert CAC_BANG <= _cac_bang(db_path)
    assert {"run_id", "batch"} <= set(_cac_cot(db_path, "violations"))
    assert {"description", "contract", "file_path", "row_count", "updated_at"} <= set(_cac_cot(db_path, "datasets"))


def test_goi_lai_khong_mat_du_lieu(db_path):
    from catalog_access import log_event
    log_event("u", "read", "t", "ALLOWED", db_path=db_path)
    init_database(db_path, seed_sample_data=False)
    init_database(db_path, seed_sample_data=False)
    assert len(get_audit_logs(db_path=db_path)) == 1 and verify_audit_chain(db_path)["ok"]


def test_du_lieu_mau_co_chuoi_bam_hop_le(tmp_path):
    db = str(tmp_path / "mau.db")
    init_database(db, seed_sample_data=True)
    kq = verify_audit_chain(db)
    assert kq["ok"] and kq["so_dong"] == 5


def test_nang_cap_db_phien_ban_cu(tmp_path):
    """audit_log cũ (cặp cột trùng, chưa có chuỗi băm), violations / datasets thiếu cột mới."""
    db = str(tmp_path / "cu.db")
    conn = sqlite3.connect(db)
    conn.executescript("""
    CREATE TABLE datasets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
        hospital TEXT NOT NULL, tier TEXT NOT NULL, owner TEXT NOT NULL, sensitive_columns TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE violations (id INTEGER PRIMARY KEY AUTOINCREMENT, error_type TEXT NOT NULL,
        severity TEXT NOT NULL, hospital TEXT NOT NULL, affected_table TEXT NOT NULL,
        affected_ai_table TEXT DEFAULT '', action TEXT NOT NULL DEFAULT 'đã chặn', details TEXT DEFAULT '',
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, thoi_gian TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ai TEXT NOT NULL, lam_gi TEXT NOT NULL, bang_nao TEXT DEFAULT '', ket_qua TEXT NOT NULL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, actor TEXT, action TEXT, target_table TEXT DEFAULT '',
        result TEXT);
    INSERT INTO datasets (name, hospital, tier, owner) VALUES ('raw_patient_hospital_A', 'Bệnh viện A', 'raw', 'IT');
    INSERT INTO violations (error_type, severity, hospital, affected_table) VALUES ('X', 'BREAKING', 'BV', 'raw_x');
    INSERT INTO audit_log (thoi_gian, ai, lam_gi, bang_nao, ket_qua, timestamp, actor, action, target_table, result)
        VALUES ('2026-10-16 08:00:00', 'engineer_alice', 'read', 'raw_patient_hospital_A', 'ALLOWED',
                '2026-10-16 08:00:00', 'engineer_alice', 'read', 'raw_patient_hospital_A', 'ALLOWED');
    INSERT INTO audit_log (thoi_gian, ai, lam_gi, bang_nao, ket_qua, timestamp, actor, action, target_table, result)
        VALUES ('', '', '', '', 'DENIED', '2026-10-16 09:00:00', 'researcher_bob', 'read', 'raw_x', 'DENIED');
    """)
    conn.commit()
    conn.close()

    init_database(db, seed_sample_data=False)

    assert _cac_cot(db, "audit_log") == ["id", "thoi_gian", "ai", "lam_gi", "bang_nao", "ket_qua", "run_id",
                                         "prev_hash", "row_hash"]
    dong = list(reversed(get_audit_logs(db_path=db)))
    assert [(d["id"], d["thoi_gian"], d["ai"], d["bang_nao"], d["ket_qua"]) for d in dong] == [
        (1, "2026-10-16 08:00:00", "engineer_alice", "raw_patient_hospital_A", "ALLOWED"),
        (2, "2026-10-16 09:00:00", "researcher_bob", "raw_x", "DENIED"),
    ]
    assert verify_audit_chain(db)["ok"]
    assert {"run_id", "batch"} <= set(_cac_cot(db, "violations"))
    assert "file_path" in _cac_cot(db, "datasets")
    assert CAC_BANG <= _cac_bang(db)


def test_bien_moi_truong_doi_duong_dan_db(tmp_path):
    db = str(tmp_path / "theo_bien_moi_truong.db")
    ket_qua = subprocess.run(
        [sys.executable, "-c", "import catalog_access.init_db as m; print(m.DEFAULT_DB_PATH)"],
        cwd=ROOT, env={**os.environ, "CLINICAL_GOV_DB": db}, capture_output=True, text=True, check=True)
    assert ket_qua.stdout.strip() == db
