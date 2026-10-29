"""Fixture dùng chung cho mọi test (governance, catalog_access, pipeline, dashboard)."""

import pytest

from catalog_access.init_db import init_database


@pytest.fixture(autouse=True)
def khoa_kiem_thu(monkeypatch, tmp_path):
    """Mỗi test dùng khóa riêng (giả danh + sổ audit), không đọc / không tạo file .secrets/ thật của repo."""
    monkeypatch.setenv("PATIENT_HASH_SECRET", "khoa-gia-danh-chi-dung-cho-kiem-thu")
    monkeypatch.setenv("PATIENT_HASH_KEY_FILE", str(tmp_path / "khoa_gia_danh.key"))
    monkeypatch.setenv("AUDIT_LOG_SECRET", "khoa-audit-chi-dung-cho-kiem-thu")
    monkeypatch.setenv("AUDIT_LOG_KEY_FILE", str(tmp_path / "khoa_audit.key"))


@pytest.fixture
def db_path(tmp_path):
    """DB tạm, KHÔNG nạp dữ liệu mẫu (seed_sample_data=False), không đụng metadata.db thật."""
    duong_dan = str(tmp_path / "test_metadata.db")
    init_database(duong_dan, seed_sample_data=False)
    return duong_dan


@pytest.fixture
def mt(tmp_path, db_path):
    """
    Môi trường pipeline tạm trên dữ liệu GIẢ (governance/sample_data.py): staging của 3 viện,
    raw chia 3 đợt, 9 bảng raw đã đăng ký catalog. Trả về dict đường dẫn: db, raw, std, ai, backup.
    """
    from governance.sample_data import make_hospital
    from pipeline import ingest_raw

    staging = tmp_path / "staging"
    for vien in "ABC":
        thu_muc = staging / f"hospital_{vien}"
        thu_muc.mkdir(parents=True)
        bang = make_hospital(vien)
        bang["patient"].to_csv(thu_muc / "patient.csv", index=False)
        bang["lab"].to_csv(thu_muc / "lab.csv", index=False)
        bang["vital"].to_csv(thu_muc / "vitalPeriodic.csv", index=False)
    duong_dan = {"db": db_path, "raw": str(tmp_path / "raw"), "std": str(tmp_path / "std"),
                 "ai": str(tmp_path / "ai"), "backup": str(tmp_path / "backup")}
    ingest_raw.main(db_path, str(staging), duong_dan["raw"])
    return duong_dan
