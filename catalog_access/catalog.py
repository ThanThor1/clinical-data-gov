"""
Module Metadata Catalog (Danh bạ dữ liệu)
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit & Báo cáo)

Nhiệm vụ:
- Đăng ký mỗi bảng dữ liệu vào danh mục với thông tin:
  + Tên bảng (name)
  + Thuộc bệnh viện nào (hospital)
  + Tầng dữ liệu nào (tier: 'raw', 'standardized', 'ai_ready')
  + Người/bộ phận phụ trách (owner)
  + Các cột nhạy cảm (sensitive_columns)
  + Mô tả, hợp đồng áp dụng, vị trí file, số dòng, lần cập nhật cuối
- Truy vấn và liệt kê các bảng dữ liệu theo tầng hoặc bệnh viện.
"""

import os
import sys
from typing import Any, Dict, List, Optional
import pandas as pd

# Thêm thư mục gốc dự án vào sys.path nếu cần
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from catalog_access.init_db import get_db_connection, now_str
from catalog_access.audit import log_event

COT_CATALOG = ("id, name, hospital, tier, owner, sensitive_columns, description, contract, "
               "file_path, row_count, created_at, updated_at")


def register_dataset(
    name: str,
    hospital: str,
    tier: str,
    owner: str,
    sensitive_columns: str = "",
    registered_by: str = "data_steward",
    db_path: Optional[str] = None,
    description: str = "",
    contract: str = "",
    file_path: str = "",
    row_count: Optional[int] = None
) -> int:
    """
    Đăng ký một bảng dữ liệu vào danh bạ Metadata Catalog theo đúng chuẩn đề tài.

    Bao gồm 5 thông tin bắt buộc:
        name (str): Tên định danh của bảng (duy nhất).
        hospital (str): Tên bệnh viện sở hữu nguồn dữ liệu.
        tier (str): Tầng dữ liệu ('raw', 'standardized', 'ai_ready').
        owner (str): Người hoặc nhóm chịu trách nhiệm quản trị bảng.
        sensitive_columns (str): Danh sách các cột nhạy cảm (phân tách bởi dấu phẩy).
    Thông tin bổ sung (không truyền thì giữ giá trị đã đăng ký trước đó):
        description (str): Mô tả bảng (thường lấy từ hợp đồng dữ liệu).
        contract (str): Hợp đồng áp dụng, ví dụ 'raw_patient v1.0'.
        file_path (str): Vị trí file, tương đối với gốc repo; bảng raw nhiều đợt dùng mẫu '*'.
        row_count (int): Số dòng hiện tại.
    Khác:
        registered_by (str): Người thực hiện đăng ký (để ghi audit log).
        db_path (str, optional): Đường dẫn tới metadata.db.

    Trả về:
        int: ID của dataset được thêm mới hoặc cập nhật.
    """
    tier_clean = tier.lower().strip()
    if tier_clean not in ('raw', 'standardized', 'ai_ready'):
        raise ValueError(f"Tầng dữ liệu không hợp lệ: '{tier}'. Chỉ chấp nhận 'raw', 'standardized', hoặc 'ai_ready'.")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO datasets (name, hospital, tier, owner, sensitive_columns,
                          description, contract, file_path, row_count, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(name) DO UPDATE SET
        hospital = excluded.hospital,
        tier = excluded.tier,
        owner = excluded.owner,
        sensitive_columns = excluded.sensitive_columns,
        description = COALESCE(NULLIF(excluded.description, ''), datasets.description),
        contract = COALESCE(NULLIF(excluded.contract, ''), datasets.contract),
        file_path = COALESCE(NULLIF(excluded.file_path, ''), datasets.file_path),
        row_count = COALESCE(excluded.row_count, datasets.row_count),
        updated_at = excluded.updated_at;
    """, (name.strip(), hospital.strip(), tier_clean, owner.strip(), sensitive_columns.strip(),
          description.strip(), contract.strip(), file_path.strip(), row_count, now_str()))

    # lastrowid không đáng tin khi câu lệnh rơi vào nhánh UPDATE -> tra lại id theo tên
    dataset_id = cursor.execute("SELECT id FROM datasets WHERE name = ?;", (name.strip(),)).fetchone()["id"]
    conn.commit()
    conn.close()

    # Ghi nhận vào sổ audit
    log_event(
        ai=registered_by,
        hanh_dong="register_dataset",
        bang=name,
        ket_qua="SUCCESS",
        db_path=db_path
    )

    return dataset_id


def list_datasets(
    tier: Optional[str] = None,
    hospital: Optional[str] = None,
    db_path: Optional[str] = None,
    as_df: bool = False
) -> Any:
    """
    Lấy danh sách tất cả các bảng dữ liệu trong catalog.
    Có thể lọc theo tầng dữ liệu hoặc theo bệnh viện.
    """
    conn = get_db_connection(db_path)
    query = f"SELECT {COT_CATALOG} FROM datasets WHERE 1=1"
    params: List[Any] = []

    if tier:
        query += " AND tier = ?"
        params.append(tier.lower().strip())
    if hospital:
        query += " AND hospital LIKE ?"
        params.append(f"%{hospital.strip()}%")

    query += " ORDER BY tier DESC, name ASC"

    if as_df:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    results = [dict(row) for row in rows]
    conn.close()
    return results


def get_dataset(name: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Tra cứu chi tiết một bảng dữ liệu trong catalog theo tên.
    """
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT {COT_CATALOG} FROM datasets WHERE name = ?;", (name.strip(),))
    row = cursor.fetchone()
    conn.close()
    if row:
        return dict(row)
    return None


def delete_dataset(name: str, deleted_by: str = "admin", db_path: Optional[str] = None) -> bool:
    """
    Xóa một bảng khỏi catalog và ghi nhận vào audit log.
    """
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM datasets WHERE name = ?;", (name.strip(),))
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()

    if deleted:
        log_event(
            ai=deleted_by,
            hanh_dong="delete_dataset",
            bang=name,
            ket_qua="SUCCESS",
            db_path=db_path
        )
    return deleted


if __name__ == "__main__":
    # Test catalog
    all_ds = list_datasets()
    print(f"[OK] Tổng số datasets hiện có: {len(all_ds)}")
    for ds in all_ds[:3]:
        print(f" - [{ds['tier'].upper()}] {ds['name']} ({ds['hospital']}) | Owner: {ds['owner']}")
