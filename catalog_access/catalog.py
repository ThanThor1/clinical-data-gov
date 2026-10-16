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
- Truy vấn và liệt kê các bảng dữ liệu theo tầng hoặc bệnh viện.
"""

import os
import sys
import sqlite3
from typing import Any, Dict, List, Optional
import pandas as pd

# Thêm thư mục gốc dự án vào sys.path nếu cần
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from catalog_access.init_db import get_db_connection, DEFAULT_DB_PATH
from catalog_access.audit import log_event


def register_dataset(
    name: str,
    hospital: str,
    tier: str,
    owner: str,
    sensitive_columns: str = "",
    registered_by: str = "data_steward",
    db_path: Optional[str] = None
) -> int:
    """
    Đăng ký một bảng dữ liệu vào danh bạ Metadata Catalog theo đúng chuẩn đề tài.
    
    Bao gồm 5 thông tin:
        name (str): Tên định danh của bảng (duy nhất).
        hospital (str): Tên bệnh viện sở hữu nguồn dữ liệu.
        tier (str): Tầng dữ liệu ('raw', 'standardized', 'ai_ready').
        owner (str): Người hoặc nhóm chịu trách nhiệm quản trị bảng.
        sensitive_columns (str): Danh sách các cột nhạy cảm (phân tách bởi dấu phẩy).
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
    INSERT INTO datasets (name, hospital, tier, owner, sensitive_columns)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(name) DO UPDATE SET
        hospital = excluded.hospital,
        tier = excluded.tier,
        owner = excluded.owner,
        sensitive_columns = excluded.sensitive_columns;
    """, (name.strip(), hospital.strip(), tier_clean, owner.strip(), sensitive_columns.strip()))

    dataset_id = cursor.lastrowid
    conn.commit()
    conn.close()

    # Ghi nhận vào sổ audit
    log_event(
        ai=registered_by,
        hanh_dong="register_dataset",
        bang=name,
        ket_qua="SUCCESS",
        chi_tiet=f"Đăng ký bảng vào catalog thuộc tầng {tier_clean}, bệnh viện {hospital}",
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
    query = "SELECT id, name, hospital, tier, owner, sensitive_columns, created_at FROM datasets WHERE 1=1"
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
    cursor.execute("""
    SELECT id, name, hospital, tier, owner, sensitive_columns, created_at
    FROM datasets WHERE name = ?;
    """, (name.strip(),))
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
            chi_tiet="Xóa bảng khỏi catalog",
            db_path=db_path
        )
    return deleted


if __name__ == "__main__":
    # Test catalog
    all_ds = list_datasets()
    print(f"[OK] Tổng số datasets hiện có: {len(all_ds)}")
    for ds in all_ds[:3]:
        print(f" - [{ds['tier'].upper()}] {ds['name']} ({ds['hospital']}) | Owner: {ds['owner']}")
