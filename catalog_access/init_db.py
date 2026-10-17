"""
Module khởi tạo cơ sở dữ liệu siêu dữ liệu dùng chung (metadata.db)
Phụ trách bởi: Người 3 (Quản lý, Catalog, Phân quyền, Audit & Báo cáo)

Nhiệm vụ:
- Tạo các bảng: datasets, schema_versions, lineage_edges, violations, audit_log.
- Cung cấp hàm init_database() cho toàn bộ dự án.
"""

import os
import sqlite3
from typing import Optional

DEFAULT_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "metadata.db"))


def get_db_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Tạo kết nối tới SQLite metadata.db."""
    path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_database(db_path: Optional[str] = None, seed_sample_data: bool = True) -> str:
    """
    Khởi tạo cấu trúc các bảng dùng chung cho hệ thống Quản trị Dữ liệu Lâm sàng:
    1. datasets: Danh bạ metadata các bảng dữ liệu (Người 3)
    2. schema_versions: Lịch sử schema và phát hiện thay đổi (Người 2 ghi, Người 3 hiển thị)
    3. lineage_edges: Quan hệ nguồn gốc dữ liệu giữa các cột/bảng (Người 2 ghi, Người 3 hiển thị)
    4. violations: Danh sách vi phạm chất lượng / schema (Người 2 ghi, Người 3 hiển thị)
    5. audit_log: Sổ ghi chép toàn bộ hoạt động truy cập và kiểm định (Người 3 phụ trách)
    """
    path = db_path or DEFAULT_DB_PATH
    conn = get_db_connection(path)
    cursor = conn.cursor()

    # 1. Bảng datasets: Đăng ký mỗi bảng dữ liệu
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS datasets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        hospital TEXT NOT NULL,
        tier TEXT NOT NULL CHECK(tier IN ('raw', 'standardized', 'ai_ready')),
        owner TEXT NOT NULL,
        sensitive_columns TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # 2. Bảng schema_versions: Lưu và so sánh schema các phiên bản
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS schema_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT NOT NULL,
        version INTEGER NOT NULL,
        columns_json TEXT NOT NULL,
        change_type TEXT NOT NULL,
        is_breaking BOOLEAN DEFAULT 0,
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(table_name, version)
    );
    """)

    # 3. Bảng lineage_edges: Ghi nhận đường đi dữ liệu (cột A của bảng X -> cột B của bảng Y)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS lineage_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_table TEXT NOT NULL,
        source_column TEXT NOT NULL,
        target_table TEXT NOT NULL,
        target_column TEXT NOT NULL,
        transformation TEXT DEFAULT 'direct',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # 4. Bảng violations: Bộ phát hiện vi phạm ghi lỗi chặn
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS violations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        error_type TEXT NOT NULL,
        severity TEXT NOT NULL CHECK(severity IN ('CRITICAL', 'BREAKING', 'WARNING', 'INFO')),
        hospital TEXT NOT NULL,
        affected_table TEXT NOT NULL,
        affected_ai_table TEXT DEFAULT '',
        action TEXT NOT NULL DEFAULT 'đã chặn',
        details TEXT DEFAULT '',
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # 5. Bảng audit_log: Sổ nhật ký ghi lại mọi hành động (thời gian, ai, làm gì, bảng nào, kết quả)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        thoi_gian TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ai TEXT NOT NULL,
        lam_gi TEXT NOT NULL,
        bang_nao TEXT DEFAULT '',
        ket_qua TEXT NOT NULL CHECK(ket_qua IN ('ALLOWED', 'DENIED', 'BLOCKED', 'SUCCESS', 'FAILED')),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        actor TEXT,
        action TEXT,
        target_table TEXT DEFAULT '',
        result TEXT
    );
    """)

    # Đảm bảo các cột tiếng Việt tồn tại nếu bảng đã được tạo trước đó
    cursor.execute("PRAGMA table_info(audit_log);")
    existing_cols = [row["name"] for row in cursor.fetchall()]
    for col, col_type in [
        ("thoi_gian", "TEXT DEFAULT ''"),
        ("ai", "TEXT DEFAULT ''"),
        ("lam_gi", "TEXT DEFAULT ''"),
        ("bang_nao", "TEXT DEFAULT ''"),
        ("ket_qua", "TEXT DEFAULT ''"),
    ]:
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE audit_log ADD COLUMN {col} {col_type};")

    # Xóa các cột chi_tiet và details nếu còn tồn tại
    for drop_col in ["chi_tiet", "details"]:
        if drop_col in existing_cols:
            try:
                cursor.execute(f"ALTER TABLE audit_log DROP COLUMN {drop_col};")
            except Exception:
                pass

    # Cập nhật dữ liệu cũ nếu thoi_gian hoặc ai còn trống
    cursor.execute("""
    UPDATE audit_log
    SET thoi_gian = COALESCE(NULLIF(thoi_gian, ''), timestamp),
        ai = COALESCE(NULLIF(ai, ''), actor),
        lam_gi = COALESCE(NULLIF(lam_gi, ''), action),
        bang_nao = COALESCE(NULLIF(bang_nao, ''), target_table),
        ket_qua = COALESCE(NULLIF(ket_qua, ''), result)
    WHERE thoi_gian IS NULL OR thoi_gian = '';
    """)

    conn.commit()

    if seed_sample_data:
        _seed_default_data_if_empty(cursor, conn)

    conn.close()
    return path


def _seed_default_data_if_empty(cursor: sqlite3.Cursor, conn: sqlite3.Connection):
    """Nạp dữ liệu mẫu ban đầu để hệ thống và dashboard có thể hoạt động và kiểm thử ngay."""
    # Kiểm tra nếu chưa có datasets
    cursor.execute("SELECT COUNT(*) FROM datasets;")
    if cursor.fetchone()[0] == 0:
        sample_datasets = [
            ("raw_patient_hospital_A", "Bệnh viện A", "raw", "Hospital A IT Team", "patientunitstayid,hospitalid"),
            ("raw_patient_hospital_B", "Bệnh viện B", "raw", "Hospital B IT Team", "patientunitstayid,admissionweight"),
            ("raw_patient_hospital_C", "Bệnh viện C", "raw", "Hospital C IT Team", "patientunitstayid,gender"),
            ("raw_lab_hospital_A", "Bệnh viện A", "raw", "Hospital A Lab Dept", "patientunitstayid,labresult"),
            ("raw_lab_hospital_B", "Bệnh viện B", "raw", "Hospital B Lab Dept", "patientunitstayid,labresult"),
            ("raw_lab_hospital_C", "Bệnh viện C", "raw", "Hospital C Lab Dept", "patientunitstayid,labresult"),
            ("standardized_patients", "Toàn hệ thống", "standardized", "Data Engineering Team", ""),
            ("standardized_labs", "Toàn hệ thống", "standardized", "Data Engineering Team", ""),
            ("ai_ready_icu_mortality", "Hội đồng AI & Lâm sàng", "ai_ready", "AI Research Lab", ""),
        ]
        cursor.executemany("""
        INSERT INTO datasets (name, hospital, tier, owner, sensitive_columns)
        VALUES (?, ?, ?, ?, ?);
        """, sample_datasets)

    # Dữ liệu mẫu schema versions
    cursor.execute("SELECT COUNT(*) FROM schema_versions;")
    if cursor.fetchone()[0] == 0:
        sample_schemas = [
            ("raw_patient_hospital_A", 1, '{"patientunitstayid":"INTEGER", "age":"TEXT", "gender":"TEXT", "admissionweight":"REAL"}', "Khởi tạo schema ban đầu", 0),
            ("raw_patient_hospital_B", 1, '{"patientunitstayid":"INTEGER", "age":"TEXT", "gender":"TEXT", "admissionweight":"REAL"}', "Khởi tạo schema ban đầu", 0),
            ("raw_patient_hospital_B", 2, '{"patientunitstayid":"INTEGER", "age":"TEXT", "gender":"TEXT", "weight":"REAL"}', "Đổi tên admissionweight thành weight (lỗi phá vỡ hợp đồng)", 1),
            ("standardized_patients", 1, '{"patient_id":"INTEGER", "age":"INTEGER", "gender":"TEXT", "weight_kg":"REAL", "source_hospital":"TEXT"}', "Schema chuẩn hóa", 0),
            ("ai_ready_icu_mortality", 1, '{"patient_hash":"TEXT", "age":"INTEGER", "gender":"TEXT", "weight_kg":"REAL", "unitdischargestatus":"INTEGER"}', "Schema sẵn sàng cho AI", 0),
        ]
        cursor.executemany("""
        INSERT INTO schema_versions (table_name, version, columns_json, change_type, is_breaking)
        VALUES (?, ?, ?, ?, ?);
        """, sample_schemas)

    # Dữ liệu mẫu Lineage (Truy vết nguồn gốc)
    cursor.execute("SELECT COUNT(*) FROM lineage_edges;")
    if cursor.fetchone()[0] == 0:
        sample_lineages = [
            ("raw_patient_hospital_A", "patientunitstayid", "standardized_patients", "patient_id", "mapping trực tiếp"),
            ("raw_patient_hospital_B", "patientunitstayid", "standardized_patients", "patient_id", "mapping trực tiếp"),
            ("raw_patient_hospital_C", "patientunitstayid", "standardized_patients", "patient_id", "mapping trực tiếp"),
            ("raw_patient_hospital_B", "admissionweight", "standardized_patients", "weight_kg", "chuẩn hóa đơn vị kg"),
            ("standardized_patients", "patient_id", "ai_ready_icu_mortality", "patient_hash", "giả danh hóa SHA-256"),
            ("standardized_patients", "weight_kg", "ai_ready_icu_mortality", "weight_kg", "chọn lọc biến đầu vào AI"),
            ("standardized_patients", "age", "ai_ready_icu_mortality", "age", "chuẩn hóa số nguyên"),
            ("standardized_patients", "gender", "ai_ready_icu_mortality", "gender", "mã hóa M/F"),
        ]
        cursor.executemany("""
        INSERT INTO lineage_edges (source_table, source_column, target_table, target_column, transformation)
        VALUES (?, ?, ?, ?, ?);
        """, sample_lineages)

    # Dữ liệu mẫu vi phạm (Violations - lỗi gây hỏng tô đỏ)
    cursor.execute("SELECT COUNT(*) FROM violations;")
    if cursor.fetchone()[0] == 0:
        sample_violations = [
            ("SCHEMA_MUTATION", "BREAKING", "Bệnh viện B", "raw_patient_hospital_B", "ai_ready_icu_mortality", "đã chặn", "Đổi tên cột bắt buộc 'admissionweight' thành 'weight'. Không khớp hợp đồng dữ liệu."),
            ("VALUE_OUT_OF_RANGE", "WARNING", "Bệnh viện A", "raw_patient_hospital_A", "ai_ready_icu_mortality", "cảnh báo", "Phát hiện chỉ số nhịp tim = 500 bpm vượt ngưỡng hợp lý [20-300 bpm] theo chuẩn Kahn."),
        ]
        cursor.executemany("""
        INSERT INTO violations (error_type, severity, hospital, affected_table, affected_ai_table, action, details)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """, sample_violations)

    # Dữ liệu mẫu Audit Log
    cursor.execute("SELECT COUNT(*) FROM audit_log;")
    if cursor.fetchone()[0] == 0:
        sample_audit = [
            ("2026-10-16 08:00:00", "admin_system", "init_system", "metadata.db", "SUCCESS",
             "2026-10-16 08:00:00", "admin_system", "init_system", "metadata.db", "SUCCESS"),
            ("2026-10-16 08:30:00", "engineer_alice", "read", "raw_patient_hospital_A", "ALLOWED",
             "2026-10-16 08:30:00", "engineer_alice", "read", "raw_patient_hospital_A", "ALLOWED"),
            ("2026-10-16 09:15:00", "pipeline_worker", "check_gate", "raw_patient_hospital_B", "BLOCKED",
             "2026-10-16 09:15:00", "pipeline_worker", "check_gate", "raw_patient_hospital_B", "BLOCKED"),
            ("2026-10-16 10:00:00", "researcher_bob", "read", "ai_ready_icu_mortality", "ALLOWED",
             "2026-10-16 10:00:00", "researcher_bob", "read", "ai_ready_icu_mortality", "ALLOWED"),
            ("2026-10-16 10:05:00", "researcher_bob", "read", "raw_patient_hospital_A", "DENIED",
             "2026-10-16 10:05:00", "researcher_bob", "read", "raw_patient_hospital_A", "DENIED"),
        ]
        cursor.executemany("""
        INSERT INTO audit_log (thoi_gian, ai, lam_gi, bang_nao, ket_qua,
                               timestamp, actor, action, target_table, result)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, sample_audit)

    conn.commit()


if __name__ == "__main__":
    db_file = init_database()
    print(f"[OK] Đã khởi tạo thành công metadata database tại: {db_file}")
