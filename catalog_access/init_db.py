"""
Module khởi tạo cơ sở dữ liệu siêu dữ liệu dùng chung (metadata.db)
Phụ trách bởi: Người 3 (Quản lý, Catalog, Phân quyền, Audit & Báo cáo)

Nhiệm vụ:
- Tạo các bảng: datasets, schema_versions, lineage_edges, violations, audit_log,
  pipeline_runs, batch_ingestions, dataset_versions, dataset_version_inputs.
- Nâng cấp DB tạo từ phiên bản cũ (thêm cột mới, chuyển audit_log sang dạng chuỗi băm)
  mà không mất dữ liệu.
- Cung cấp hàm init_database() cho toàn bộ dự án.

Đường dẫn DB mặc định là metadata.db ở gốc repo, đổi được bằng biến môi trường CLINICAL_GOV_DB.
"""

import os
import sqlite3
from datetime import datetime
from typing import Optional

DEFAULT_DB_PATH = os.path.abspath(
    os.environ.get("CLINICAL_GOV_DB")
    or os.path.join(os.path.dirname(__file__), "..", "metadata.db")
)

# Cấu trúc bảng audit_log (dùng cả khi tạo mới lẫn khi nâng cấp DB cũ).
# prev_hash / row_hash tạo thành chuỗi băm: sửa hay xóa một dòng là verify_audit_chain() phát hiện.
CAU_LENH_AUDIT_LOG = """
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thoi_gian TEXT NOT NULL,
    ai TEXT NOT NULL,
    lam_gi TEXT NOT NULL,
    bang_nao TEXT DEFAULT '',
    ket_qua TEXT NOT NULL CHECK(ket_qua IN ('ALLOWED', 'DENIED', 'BLOCKED', 'SUCCESS', 'FAILED')),
    run_id TEXT DEFAULT '',
    prev_hash TEXT NOT NULL,
    row_hash TEXT NOT NULL
);
"""


def now_str() -> str:
    """Thời gian hiện tại (giờ máy) dạng 'YYYY-MM-DD HH:MM:SS', dùng chung cho mọi bảng."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_db_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Tạo kết nối tới SQLite metadata.db."""
    path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    # timeout: dashboard và pipeline có thể cùng ghi một lúc -> chờ khóa thay vì báo lỗi ngay
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _cac_cot(cursor: sqlite3.Cursor, bang: str) -> set:
    cursor.execute(f"PRAGMA table_info({bang});")
    return {row["name"] for row in cursor.fetchall()}


def _them_cot_neu_thieu(cursor: sqlite3.Cursor, bang: str, cac_cot_moi: list[tuple[str, str]]) -> None:
    """Thêm cột vào bảng đã có (DB tạo từ phiên bản cũ của dự án)."""
    hien_co = _cac_cot(cursor, bang)
    for cot, kieu in cac_cot_moi:
        if cot not in hien_co:
            cursor.execute(f"ALTER TABLE {bang} ADD COLUMN {cot} {kieu};")


def _nang_cap_audit_log(conn: sqlite3.Connection) -> None:
    """
    audit_log của phiên bản cũ lưu trùng 2 bộ cột (ai/actor, lam_gi/action, ...) và chưa có chuỗi băm.
    Chuyển sang cấu trúc mới, giữ nguyên id và nội dung, tính chuỗi băm cho toàn bộ các dòng cũ.
    """
    from catalog_access.audit import _chen_dong_audit, _chuan_hoa_ket_qua

    cursor = conn.cursor()
    cot_cu = _cac_cot(cursor, "audit_log")
    if not cot_cu or "row_hash" in cot_cu:
        return

    def _lay(viet: str, anh: str) -> str:
        co = [c for c in (viet, anh) if c in cot_cu]
        if len(co) == 2:
            return f"COALESCE(NULLIF({viet}, ''), {anh}, '')"
        return f"COALESCE({co[0]}, '')" if co else "''"

    cursor.execute("ALTER TABLE audit_log RENAME TO audit_log_cu;")
    cursor.execute(CAU_LENH_AUDIT_LOG)
    dong_cu = cursor.execute(
        f"SELECT id, {_lay('thoi_gian', 'timestamp')} AS thoi_gian, {_lay('ai', 'actor')} AS ai, "
        f"{_lay('lam_gi', 'action')} AS lam_gi, {_lay('bang_nao', 'target_table')} AS bang_nao, "
        f"{_lay('ket_qua', 'result')} AS ket_qua FROM audit_log_cu ORDER BY id"
    ).fetchall()
    for d in dong_cu:
        _chen_dong_audit(conn, str(d["thoi_gian"] or now_str()), d["ai"] or "unknown_user",
                         d["lam_gi"] or "unknown_action", d["bang_nao"], _chuan_hoa_ket_qua(d["ket_qua"]),
                         ma=d["id"])
    cursor.execute("DROP TABLE audit_log_cu;")
    conn.commit()


def init_database(db_path: Optional[str] = None, seed_sample_data: bool = True) -> str:
    """
    Khởi tạo cấu trúc các bảng dùng chung cho hệ thống Quản trị Dữ liệu Lâm sàng:
    1. datasets: Danh bạ metadata các bảng dữ liệu (Người 3)
    2. schema_versions: Lịch sử schema và phát hiện thay đổi (Người 2 ghi, Người 3 hiển thị)
    3. lineage_edges: Quan hệ nguồn gốc dữ liệu giữa các cột/bảng (Người 2 ghi, Người 3 hiển thị)
    4. violations: Danh sách vi phạm chất lượng / schema (Người 2 ghi, Người 3 hiển thị)
    5. audit_log: Sổ ghi chép toàn bộ hoạt động truy cập và kiểm định, có chuỗi băm chống sửa (Người 3)
    6. pipeline_runs, batch_ingestions, dataset_versions, dataset_version_inputs:
       provenance theo từng lần chạy và từng lô dữ liệu (governance/provenance.py)

    Gọi nhiều lần không sao: bảng đã có thì giữ nguyên, DB cũ thì được nâng cấp.
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
    # Thông tin bổ sung của catalog: mô tả, hợp đồng áp dụng, vị trí file, số dòng, lần cập nhật cuối
    _them_cot_neu_thieu(cursor, "datasets", [
        ("description", "TEXT DEFAULT ''"),
        ("contract", "TEXT DEFAULT ''"),
        ("file_path", "TEXT DEFAULT ''"),
        ("row_count", "INTEGER"),
        ("updated_at", "TEXT DEFAULT ''"),
    ])

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
    # Vi phạm thuộc lần chạy nào, lô dữ liệu nào (để truy ngược về đúng file gây lỗi)
    _them_cot_neu_thieu(cursor, "violations", [
        ("run_id", "TEXT DEFAULT ''"),
        ("batch", "TEXT DEFAULT ''"),
    ])

    # 5. Bảng audit_log: Sổ nhật ký ghi lại mọi hành động (thời gian, ai, làm gì, bảng nào, kết quả)
    _nang_cap_audit_log(conn)
    cursor.execute(CAU_LENH_AUDIT_LOG)

    # 6. Provenance: mỗi lần chạy pipeline, mỗi lô dữ liệu nguồn, mỗi phiên bản bảng đầu ra
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id TEXT PRIMARY KEY,
        actor TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'RUNNING'
            CHECK(status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'BLOCKED', 'FAILED')),
        code_version TEXT DEFAULT '',
        note TEXT DEFAULT ''
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS batch_ingestions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        table_name TEXT NOT NULL,
        hospital TEXT NOT NULL,
        batch TEXT NOT NULL,
        source_file TEXT NOT NULL,
        checksum TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        schema_version INTEGER,
        contract TEXT DEFAULT '',
        status TEXT NOT NULL CHECK(status IN ('ACCEPTED', 'BLOCKED', 'SKIPPED')),
        recorded_at TEXT NOT NULL
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dataset_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        table_name TEXT NOT NULL,
        tier TEXT NOT NULL,
        file_path TEXT NOT NULL,
        checksum TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        created_at TEXT NOT NULL
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dataset_version_inputs (
        version_id INTEGER NOT NULL,
        input_type TEXT NOT NULL CHECK(input_type IN ('batch', 'dataset_version')),
        input_id INTEGER NOT NULL,
        PRIMARY KEY (version_id, input_type, input_id)
    );
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_batch_run ON batch_ingestions(run_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_version_table ON dataset_versions(table_name);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_violation_run ON violations(run_id);")

    conn.commit()

    if seed_sample_data:
        _seed_default_data_if_empty(cursor, conn)

    conn.close()
    return path


def _seed_default_data_if_empty(cursor: sqlite3.Cursor, conn: sqlite3.Connection):
    """Nạp dữ liệu mẫu ban đầu để hệ thống và dashboard có thể hoạt động và kiểm thử ngay."""
    from catalog_access.audit import _chen_dong_audit

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
            ("standardized_patients", "patient_id", "ai_ready_icu_mortality", "patient_hash", "giả danh hóa HMAC-SHA256"),
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

    # Dữ liệu mẫu Audit Log (ghi qua chuỗi băm như mọi dòng audit khác)
    cursor.execute("SELECT COUNT(*) FROM audit_log;")
    if cursor.fetchone()[0] == 0:
        sample_audit = [
            ("2026-10-16 08:00:00", "admin_system", "init_system", "metadata.db", "SUCCESS"),
            ("2026-10-16 08:30:00", "engineer_alice", "read", "raw_patient_hospital_A", "ALLOWED"),
            ("2026-10-16 09:15:00", "pipeline_worker", "check_gate", "raw_patient_hospital_B", "BLOCKED"),
            ("2026-10-16 10:00:00", "researcher_bob", "read", "ai_ready_icu_mortality", "ALLOWED"),
            ("2026-10-16 10:05:00", "researcher_bob", "read", "raw_patient_hospital_A", "DENIED"),
        ]
        for dong in sample_audit:
            _chen_dong_audit(conn, *dong)

    conn.commit()


if __name__ == "__main__":
    db_file = init_database()
    print(f"[OK] Đã khởi tạo thành công metadata database tại: {db_file}")
