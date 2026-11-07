"""Fixture dùng chung cho mọi test (governance, catalog_access, pipeline, dashboard)."""

import os

import pytest

from catalog_access.init_db import init_database


def pytest_collection_modifyitems(items):
    """Test nào dùng fixture spark (trực tiếp hoặc gián tiếp) thì gắn nhãn @pytest.mark.spark."""
    for item in items:
        if "spark" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.spark)


@pytest.fixture(scope="session")
def spark():
    """
    Một SparkSession (local[2], kèm Delta Lake) dùng chung cho cả phiên test.
    Máy chưa cài pyspark / delta-spark hoặc chưa có Java -> các test cần Spark tự bỏ qua.
    """
    from pipeline.spark_session import get_spark, spark_available

    if not spark_available():
        pytest.skip("Chưa cài pyspark + delta-spark hoặc chưa có Java 17+")
    for bien, gia_tri in (("SPARK_MASTER", "local[2]"), ("SPARK_SHUFFLE_PARTITIONS", "2"), ("SPARK_UI", "0"),
                          ("SPARK_DRIVER_MEMORY", "1g")):
        os.environ[bien] = gia_tri
    phien = get_spark("kiem-thu")
    yield phien
    phien.stop()


@pytest.fixture(scope="session", autouse=True)
def khoa_kiem_thu(tmp_path_factory):
    """Cả phiên test dùng khóa riêng (giả danh + sổ audit), không đọc / không tạo file .secrets/ thật của repo."""
    thu_muc = tmp_path_factory.mktemp("khoa")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("PATIENT_HASH_SECRET", "khoa-gia-danh-chi-dung-cho-kiem-thu")
        mp.setenv("PATIENT_HASH_KEY_FILE", str(thu_muc / "khoa_gia_danh.key"))
        mp.setenv("AUDIT_LOG_SECRET", "khoa-audit-chi-dung-cho-kiem-thu")
        mp.setenv("AUDIT_LOG_KEY_FILE", str(thu_muc / "khoa_audit.key"))
        yield


@pytest.fixture
def db_path(tmp_path):
    """DB tạm, KHÔNG nạp dữ liệu mẫu (seed_sample_data=False), không đụng metadata.db thật."""
    duong_dan = str(tmp_path / "test_metadata.db")
    init_database(duong_dan, seed_sample_data=False)
    return duong_dan


def tao_moi_truong_mau(goc, db_path):
    """
    Môi trường pipeline tạm trên dữ liệu GIẢ (governance/sample_data.py) trong thư mục `goc`: staging của
    3 viện A, B, C, raw chia 3 đợt, 9 bảng raw đã đăng ký catalog. Trả về dict đường dẫn: db, raw, std, ai, backup.
    """
    from governance.sample_data import make_hospital
    from pipeline import ingest_raw

    staging = goc / "staging"
    for vien in "ABC":
        thu_muc = staging / f"hospital_{vien}"
        thu_muc.mkdir(parents=True)
        bang = make_hospital(vien)
        bang["patient"].to_csv(thu_muc / "patient.csv", index=False)
        bang["lab"].to_csv(thu_muc / "lab.csv", index=False)
        bang["vital"].to_csv(thu_muc / "vitalPeriodic.csv", index=False)
    duong_dan = {"db": db_path, "raw": str(goc / "raw"), "std": str(goc / "std"),
                 "ai": str(goc / "ai"), "backup": str(goc / "backup")}
    ingest_raw.main(db_path, str(staging), duong_dan["raw"])
    return duong_dan


@pytest.fixture
def mt(tmp_path, db_path):
    """Môi trường pipeline tạm trên dữ liệu giả, mỗi test một môi trường (xem tao_moi_truong_mau)."""
    return tao_moi_truong_mau(tmp_path, db_path)


@pytest.fixture(scope="module")
def mt_dung_chung(tmp_path_factory):
    """Như mt nhưng dùng chung cho cả một file test (khi các test chỉ đọc, không sửa dữ liệu)."""
    goc = tmp_path_factory.mktemp("mt")
    db = str(goc / "test_metadata.db")
    init_database(db, seed_sample_data=False)
    return tao_moi_truong_mau(goc, db)
