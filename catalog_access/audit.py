"""
Module Sổ Audit (audit_log)
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit & Báo cáo)

Nhiệm vụ theo phân công trong PDF (Mục 7 & 4):
- Cung cấp hàm log_event(...) ghi một dòng vào sổ audit gồm 5 thông tin:
  1. thời gian (thoi_gian / timestamp)
  2. ai (ai / actor)
  3. làm gì (lam_gi / hanh_dong / action)
  4. bảng nào (bang_nao / bang / target_table)
  5. kết quả (ket_qua / result)
- Lưu trữ mọi thao tác đọc/ghi, sự kiện kiểm tra cổng dữ liệu, và vi phạm quyền truy cập.
- Cung cấp các hàm truy vấn nhật ký phục vụ Dashboard và báo cáo.
"""

import os
import sys
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional
import pandas as pd

# Thêm thư mục gốc dự án vào sys.path nếu cần
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from catalog_access.init_db import get_db_connection, DEFAULT_DB_PATH


def log_event(
    *args,
    ai: Optional[str] = None,
    hanh_dong: Optional[str] = None,
    lam_gi: Optional[str] = None,
    bang: Optional[str] = None,
    bang_nao: Optional[str] = None,
    ket_qua: Optional[str] = None,
    thoi_gian: Optional[str] = None,
    chi_tiet: str = "",
    actor: Optional[str] = None,
    action: Optional[str] = None,
    target_table: Optional[str] = None,
    result: Optional[str] = None,
    details: Optional[str] = None,
    db_path: Optional[str] = None,
    **kwargs
) -> int:
    """
    Ghi một dòng vào sổ audit gồm 5 trường cốt lõi theo đúng yêu cầu đề tài:
    - thời gian (thoi_gian / timestamp): Thời điểm phát sinh sự kiện (mặc định lấy thời gian thực hiện tại).
    - ai (ai / actor): Tác nhân thực hiện (Người dùng / Service / Pipeline / Role).
    - làm gì (lam_gi / hanh_dong / action): Hành động thực hiện (e.g., 'read', 'write', 'check_gate', 'schema_check').
    - bảng nào (bang_nao / bang / target_table): Bảng dữ liệu mục tiêu chịu tác động.
    - kết quả (ket_qua / result): Trạng thái kết quả ('ALLOWED', 'DENIED', 'BLOCKED', 'SUCCESS', 'FAILED').
    - chi_tiet (chi_tiet / details, optional): Ghi chú bổ sung hoặc lý do lỗi/chặn.
    
    Hỗ trợ linh hoạt:
      + Cách 1 (theo bảng Mục 4, Trang 5): log_event(ai, hanh_dong, bang, ket_qua)
      + Cách 2 (theo mô tả Mục 7.4, Trang 8): log_event(thoi_gian, ai, lam_gi, bang_nao, ket_qua)
      + Cách 3: Truyền tham số có đặt tên (keyword arguments: thoi_gian=..., ai=..., lam_gi=..., bang_nao=..., ket_qua=...)
    """
    # Xử lý các đối số truyền theo vị trí (*args)
    v_thoi_gian = thoi_gian
    v_ai = ai or actor
    v_lam_gi = lam_gi or hanh_dong or action
    v_bang_nao = bang_nao or bang or target_table
    v_ket_qua = ket_qua or result
    v_chi_tiet = chi_tiet or details or ""

    if len(args) == 4:
        # log_event(ai, hanh_dong, bang, ket_qua)
        v_ai = args[0]
        v_lam_gi = args[1]
        v_bang_nao = args[2]
        v_ket_qua = args[3]
    elif len(args) == 5:
        # Kiểm tra nếu arg[0] là chuỗi thời gian hoặc datetime
        arg0_str = str(args[0])
        if any(char in arg0_str for char in ['-', ':', '/']) and len(arg0_str) >= 8:
            # log_event(thoi_gian, ai, lam_gi, bang_nao, ket_qua)
            v_thoi_gian = args[0]
            v_ai = args[1]
            v_lam_gi = args[2]
            v_bang_nao = args[3]
            v_ket_qua = args[4]
        else:
            # log_event(ai, hanh_dong, bang, ket_qua, chi_tiet)
            v_ai = args[0]
            v_lam_gi = args[1]
            v_bang_nao = args[2]
            v_ket_qua = args[3]
            v_chi_tiet = args[4]
    elif len(args) >= 6:
        v_thoi_gian = args[0]
        v_ai = args[1]
        v_lam_gi = args[2]
        v_bang_nao = args[3]
        v_ket_qua = args[4]
        v_chi_tiet = args[5]

    # Nếu thời gian chưa có, tự động lấy thời gian hiện tại
    if not v_thoi_gian:
        v_thoi_gian = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Giá trị mặc định an toàn cho các trường
    v_ai = str(v_ai or "unknown_user")
    v_lam_gi = str(v_lam_gi or "unknown_action")
    v_bang_nao = str(v_bang_nao or "")
    v_chi_tiet = str(v_chi_tiet or "")

    # Chuẩn hóa trạng thái kết quả
    ket_qua_str = str(v_ket_qua or "DENIED").strip().upper()
    valid_results = {'ALLOWED', 'DENIED', 'BLOCKED', 'SUCCESS', 'FAILED'}
    if ket_qua_str not in valid_results:
        if ket_qua_str in {'TRUE', 'DAT', 'PASS', 'OK', 'THANH_CONG'}:
            ket_qua_str = 'ALLOWED'
        else:
            ket_qua_str = 'DENIED'

    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    # Lưu đồng thời cả các cột tiếng Việt và tiếng Anh để tương thích tuyệt đối
    cursor.execute("""
    INSERT INTO audit_log (
        thoi_gian, ai, lam_gi, bang_nao, ket_qua, chi_tiet,
        timestamp, actor, action, target_table, result, details
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        v_thoi_gian,
        v_ai,
        v_lam_gi,
        v_bang_nao,
        ket_qua_str,
        v_chi_tiet,
        v_thoi_gian,
        v_ai,
        v_lam_gi,
        v_bang_nao,
        ket_qua_str,
        v_chi_tiet
    ))

    log_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return log_id


def get_audit_logs(
    limit: int = 200,
    ai: Optional[str] = None,
    actor: Optional[str] = None,
    ket_qua: Optional[str] = None,
    result: Optional[str] = None,
    db_path: Optional[str] = None,
    as_df: bool = False
) -> Any:
    """
    Truy vấn danh sách sổ nhật ký kiểm toán từ bảng audit_log.
    Trả về đầy đủ 5 trường thông tin: thời gian, ai, làm gì, bảng nào, kết quả.
    
    Tham số:
        limit (int): Số bản ghi tối đa lấy về (mới nhất trước).
        ai / actor (str, optional): Lọc theo tên người dùng / tác nhân.
        ket_qua / result (str, optional): Lọc theo kết quả ('ALLOWED', 'DENIED', 'BLOCKED',...).
        db_path (str, optional): Đường dẫn tới metadata.db.
        as_df (bool): Trả về pandas DataFrame nếu True, ngược lại trả về list of dicts.
    """
    conn = get_db_connection(db_path)
    user_filter = ai or actor
    res_filter = ket_qua or result

    query = """
    SELECT 
        id,
        COALESCE(thoi_gian, timestamp) AS thoi_gian,
        COALESCE(ai, actor) AS ai,
        COALESCE(lam_gi, action) AS lam_gi,
        COALESCE(bang_nao, target_table) AS bang_nao,
        COALESCE(ket_qua, result) AS ket_qua,
        COALESCE(chi_tiet, details) AS chi_tiet,
        COALESCE(timestamp, thoi_gian) AS timestamp,
        COALESCE(actor, ai) AS actor,
        COALESCE(action, lam_gi) AS action,
        COALESCE(target_table, bang_nao) AS target_table,
        COALESCE(result, ket_qua) AS result,
        COALESCE(details, chi_tiet) AS details
    FROM audit_log 
    WHERE 1=1
    """
    params: List[Any] = []

    if user_filter:
        query += " AND (ai LIKE ? OR actor LIKE ?)"
        params.extend([f"%{user_filter}%", f"%{user_filter}%"])
    if res_filter:
        query += " AND (ket_qua = ? OR result = ?)"
        params.extend([res_filter.upper(), res_filter.upper()])

    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

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


if __name__ == "__main__":
    # Test cả 3 cách gọi hàm log_event với đầy đủ: thời gian, ai, làm gì, bảng nào, kết quả
    print("--- KIỂM TRA HÀM log_event() TRONG catalog_access/audit.py ---")

    # Cách 1: 4 tham số (ai, hanh_dong, bang, ket_qua) - thời gian tự động sinh
    id1 = log_event(
        ai="engineer_alice",
        hanh_dong="read",
        bang="raw_patient_hospital_A",
        ket_qua="ALLOWED",
        chi_tiet="Kỹ sư dữ liệu đọc tầng raw"
    )
    print(f"[OK] Cách 1 (4 tham số) thành công, Log ID = {id1}")

    # Cách 2: 5 tham số có truyền thời gian rõ ràng (thời gian, ai, làm gì, bảng nào, kết quả)
    id2 = log_event(
        "2026-10-16 22:10:00",
        "nhà nghiên cứu",
        "read",
        "raw_patient_hospital_B",
        "DENIED"
    )
    print(f"[OK] Cách 2 (5 tham số: thời gian, ai, làm gì, bảng nào, kết quả) thành công, Log ID = {id2}")

    # Cách 3: Keyword arguments bằng tiếng Việt
    id3 = log_event(
        thoi_gian="2026-10-16 22:15:00",
        ai="pipeline_worker",
        lam_gi="check_gate",
        bang_nao="raw_patient_hospital_B",
        ket_qua="BLOCKED",
        chi_tiet="Chặn dữ liệu do đổi tên cột phá vỡ hợp đồng"
    )
    print(f"[OK] Cách 3 (Keyword arguments tiếng Việt) thành công, Log ID = {id3}")

    # Hiển thị các bản ghi trong sổ audit
    logs = get_audit_logs(limit=3)
    print("\n--- 3 BẢN GHI AUDIT LOG GẦN NHẤT ---")
    for log in logs:
        print(f"[{log['thoi_gian']}] Ai: {log['ai']} | Làm gì: {log['lam_gi']} | Bảng nào: {log['bang_nao']} | Kết quả: {log['ket_qua']}")
