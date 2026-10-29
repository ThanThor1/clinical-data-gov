"""
Bộ phát hiện vi phạm (Violation Engine) - phần cốt lõi của governance.

Khi một cổng kiểm tra (gates.py) tìm ra lỗi, nó gọi report_violation(...). Hàm này:
  1. Suy ra bệnh viện từ tên bảng (raw_patient_hospital_B -> "Bệnh viện B").
  2. Dùng lineage tìm MỌI bảng AI-ready bị ảnh hưởng (theo cột nếu có, không thì theo bảng).
  3. Ghi một dòng vào bảng `violations` (dashboard tô đỏ nếu BREAKING, vàng nếu WARNING),
     kèm run_id và lô dữ liệu đang xử lý (lấy từ ngữ cảnh của governance/provenance.py).
  4. Ghi audit: log_event("violation_engine", error_type, table, "BLOCKED" | "SUCCESS").
  5. In ra màn hình các bảng AI-ready bị ảnh hưởng.

Chạy thử: python -m governance.violation_engine   (in thống kê vi phạm trong metadata.db)
"""

import re
from typing import Optional

from catalog_access import get_audit_context, log_event
from catalog_access.init_db import get_db_connection, now_str
from governance.lineage import get_affected_ai_tables, get_impact_paths

# Các loại lỗi governance và ý nghĩa
LOAI_LOI = {
    "SCHEMA_COLUMN_RENAMED": "Cột bị đổi tên so với phiên bản schema chuẩn",
    "SCHEMA_COLUMN_REMOVED": "Cột bị xóa so với phiên bản schema chuẩn",
    "SCHEMA_TYPE_CHANGED": "Kiểu dữ liệu của cột không còn đúng như hợp đồng",
    "MISSING_REQUIRED_COLUMN": "Thiếu cột bắt buộc theo hợp đồng",
    "NO_CONTRACT": "Bảng chưa có hợp đồng dữ liệu",
    "COMPLETENESS": "Kahn - Đầy đủ: quá nhiều ô trống",
    "CONFORMANCE": "Kahn - Tuân thủ: sai định dạng hoặc giá trị không cho phép",
    "PLAUSIBILITY": "Kahn - Hợp lý: giá trị nằm ngoài khoảng hợp lý",
    "PRIVACY_SENSITIVE_COLUMN": "Tầng AI-ready chứa cột định danh bệnh nhân",
    "PRIVACY_UNDECLARED_COLUMN": "Tầng AI-ready chứa cột không khai báo trong hợp đồng",
    "PROVENANCE_UNVERIFIED": "File đầu vào không khớp checksum với phiên bản nào do pipeline tạo ra",
}

MUC_DO_HOP_LE = {"CRITICAL", "BREAKING", "WARNING", "INFO"}


def infer_hospital(table: str) -> str:
    """'raw_patient_hospital_B' -> 'Bệnh viện B'; bảng không thuộc viện nào -> 'Toàn hệ thống'."""
    khop = re.search(r"_hospital_([A-Za-z0-9]+)$", table)
    if khop:
        return f"Bệnh viện {khop.group(1).upper()}"
    return "Toàn hệ thống"


def report_violation(error_type: str, severity: str, table: str, details: str,
                     column: Optional[str] = None, blocked: bool = True,
                     db_path: Optional[str] = None) -> dict:
    """
    Ghi một vi phạm governance và tìm các bảng AI-ready bị ảnh hưởng.

    error_type : một trong LOAI_LOI (ví dụ 'SCHEMA_COLUMN_RENAMED')
    severity   : 'BREAKING' | 'WARNING' (chấp nhận thêm 'CRITICAL', 'INFO')
    table      : bảng phát hiện lỗi, ví dụ 'raw_patient_hospital_B'
    details    : mô tả lỗi bằng tiếng Việt
    column     : cột bị lỗi (nếu có) -> tra lineage theo cột cho chính xác
    blocked    : True -> action 'đã chặn', False -> 'cảnh báo'

    Trả về dict thông tin vi phạm vừa ghi (có 'id' và 'affected_ai_tables').
    """
    severity = severity.upper()
    if severity not in MUC_DO_HOP_LE:
        raise ValueError(f"Mức độ '{severity}' không hợp lệ, chỉ nhận {sorted(MUC_DO_HOP_LE)}")

    benh_vien = infer_hospital(table)
    bang_ai = get_affected_ai_tables(table, column, db_path)
    duong_di = get_impact_paths(table, column, db_path)
    hanh_dong = "đã chặn" if blocked else "cảnh báo"
    chi_tiet = f"[Cột: {column}] {details}" if column else details
    ngu_canh = get_audit_context()

    conn = get_db_connection(db_path)
    con_tro = conn.execute(
        "INSERT INTO violations (error_type, severity, hospital, affected_table, affected_ai_table, "
        "action, details, timestamp, run_id, batch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (error_type, severity, benh_vien, table, ",".join(bang_ai), hanh_dong, chi_tiet,
         now_str(), ngu_canh["run_id"], ngu_canh["batch"]),
    )
    ma_vi_pham = con_tro.lastrowid
    conn.commit()
    conn.close()

    log_event("violation_engine", error_type, table, "BLOCKED" if blocked else "SUCCESS",
              db_path=db_path)

    # In ra màn hình cho người vận hành
    bieu_tuong = "⛔" if blocked else "⚠️ "
    lo = f" | lô {ngu_canh['batch']}" if ngu_canh["batch"] else ""
    print(f"{bieu_tuong} [{severity}] {error_type} tại {table} ({benh_vien}){lo} - {hanh_dong}")
    print(f"     {chi_tiet}")
    if bang_ai:
        print(f"     Bảng AI-ready bị ảnh hưởng: {', '.join(bang_ai)}")
        for p in duong_di[:3]:
            print(f"       đường đi: {p}")
    else:
        print("     Không có bảng AI-ready nào bị ảnh hưởng (theo lineage).")

    return {
        "id": ma_vi_pham,
        "error_type": error_type,
        "severity": severity,
        "hospital": benh_vien,
        "affected_table": table,
        "column": column,
        "affected_ai_tables": bang_ai,
        "affected_ai_table": ",".join(bang_ai),
        "impact_paths": duong_di,
        "action": hanh_dong,
        "details": chi_tiet,
        "run_id": ngu_canh["run_id"],
        "batch": ngu_canh["batch"],
    }


def get_violations(db_path: Optional[str] = None, severity: Optional[str] = None,
                   table: Optional[str] = None, since_id: int = 0,
                   run_id: Optional[str] = None) -> list[dict]:
    """Đọc danh sách vi phạm (mới nhất trước), lọc theo mức độ, bảng, lần chạy, hoặc id > since_id."""
    cau_lenh = "SELECT * FROM violations WHERE id > ?"
    tham_so: list = [since_id]
    if severity:
        cau_lenh += " AND severity = ?"
        tham_so.append(severity.upper())
    if table:
        cau_lenh += " AND affected_table = ?"
        tham_so.append(table)
    if run_id:
        cau_lenh += " AND run_id = ?"
        tham_so.append(run_id)
    conn = get_db_connection(db_path)
    dong = conn.execute(cau_lenh + " ORDER BY id DESC", tham_so).fetchall()
    conn.close()
    return [dict(d) for d in dong]


def summarize(db_path: Optional[str] = None, since_id: int = 0, in_ra: bool = True) -> dict:
    """Thống kê vi phạm theo mức độ, loại lỗi và bệnh viện; in ra màn hình nếu in_ra=True."""
    danh_sach = get_violations(db_path, since_id=since_id)
    thong_ke = {"tong": len(danh_sach), "theo_muc_do": {}, "theo_loai": {}, "theo_benh_vien": {}}
    for v in danh_sach:
        for khoa, gia_tri in (("theo_muc_do", v["severity"]), ("theo_loai", v["error_type"]),
                              ("theo_benh_vien", v["hospital"])):
            thong_ke[khoa][gia_tri] = thong_ke[khoa].get(gia_tri, 0) + 1

    if in_ra:
        print(f"Tổng số vi phạm: {thong_ke['tong']}")
        for tieu_de, khoa in (("Theo mức độ", "theo_muc_do"), ("Theo loại lỗi", "theo_loai"),
                              ("Theo bệnh viện", "theo_benh_vien")):
            print(f"  {tieu_de}:")
            for ten, so in sorted(thong_ke[khoa].items()):
                print(f"    - {ten}: {so}")
    return thong_ke


if __name__ == "__main__":
    summarize()
