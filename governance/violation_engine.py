"""
Bộ phát hiện vi phạm (Violation Engine) - phần cốt lõi của governance.

Khi một cổng kiểm tra (gates.py) tìm ra lỗi, nó gọi report_violation(...). Hàm này:
  1. Suy ra bệnh viện từ tên bảng (raw_patient_hospital_B -> "Bệnh viện B").
  2. Dùng lineage tìm MỌI bảng AI-ready bị ảnh hưởng (theo cột nếu có, không thì theo bảng).
  3. Ghi một dòng vào bảng `violations` (dashboard tô đỏ nếu BREAKING, vàng nếu WARNING),
     kèm run_id và lô dữ liệu đang xử lý (lấy từ ngữ cảnh của governance/provenance.py).
  4. Ghi audit: log_event("violation_engine", error_type, table, "BLOCKED" | "SUCCESS").
  5. In ra màn hình các bảng AI-ready bị ảnh hưởng (chỉ với lỗi bị chặn; cảnh báo chỉ ghi vào DB,
     vì dữ liệu thật của 186 viện có hàng trăm cảnh báo nhỏ mỗi lần chạy).

Vòng đời của vi phạm (cột status), cập nhật khi mỗi lần chạy pipeline kết thúc (update_statuses_after_run):
    OPEN       mới ghi, chưa được kiểm tra lại
    RESOLVED   lần chạy sau kiểm tra lại đúng lô / bảng đó và cho qua (bệnh viện gửi lại, hoặc kỹ sư
               duyệt thay đổi schema, hoặc bảng có phiên bản mới qua cổng)
    SUPERSEDED lần chạy sau ghi lại đúng lỗi đó (cùng bảng, lô, loại lỗi) -> dòng mới thay thế dòng cũ

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
    "PIPELINE_OUTPUT_DRIFT": "Code pipeline đổi làm đầu ra lệch so với phiên bản trước (governance/pipeline_drift.py)",
    "SEMANTIC_MISMATCH": "Nguồn định nghĩa một cột khác với hợp đồng (sổ đăng ký nguồn sources/*.yaml)",
}

MUC_DO_HOP_LE = {"CRITICAL", "BREAKING", "WARNING", "INFO"}
TRANG_THAI = ("OPEN", "RESOLVED", "SUPERSEDED")


def infer_hospital(table: str) -> str:
    """'raw_patient_hospital_B' -> 'Bệnh viện B'; bảng không thuộc viện nào -> 'Toàn hệ thống'."""
    khop = re.search(r"_hospital_([A-Za-z0-9]+)$", table)
    if khop:
        return f"Bệnh viện {khop.group(1).upper()}"
    return "Toàn hệ thống"


def report_violation(error_type: str, severity: str, table: str, details: str,
                     column: Optional[str] = None, blocked: bool = True,
                     db_path: Optional[str] = None, in_ra: Optional[bool] = None) -> dict:
    """
    Ghi một vi phạm governance và tìm các bảng AI-ready bị ảnh hưởng.

    error_type : một trong LOAI_LOI (ví dụ 'SCHEMA_COLUMN_RENAMED')
    severity   : 'BREAKING' | 'WARNING' (chấp nhận thêm 'CRITICAL', 'INFO')
    table      : bảng phát hiện lỗi, ví dụ 'raw_patient_hospital_B'
    details    : mô tả lỗi bằng tiếng Việt
    column     : cột bị lỗi (nếu có) -> tra lineage theo cột cho chính xác
    blocked    : True -> action 'đã chặn', False -> 'cảnh báo'
    in_ra      : in ra màn hình hay không (mặc định: chỉ in khi bị chặn)

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
    if in_ra is None:
        in_ra = blocked
    bieu_tuong = "⛔" if blocked else "⚠️ "
    lo = f" | lô {ngu_canh['batch']}" if ngu_canh["batch"] else ""
    if in_ra:
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
                   run_id: Optional[str] = None, status: Optional[str] = None) -> list[dict]:
    """Đọc danh sách vi phạm (mới nhất trước), lọc theo mức độ, bảng, lần chạy, trạng thái, hoặc id > since_id."""
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
    if status:
        cau_lenh += " AND status = ?"
        tham_so.append(status.upper())
    conn = get_db_connection(db_path)
    dong = conn.execute(cau_lenh + " ORDER BY id DESC", tham_so).fetchall()
    conn.close()
    return [dict(d) for d in dong]


def update_statuses_after_run(run_id: str, db_path: Optional[str] = None) -> dict:
    """
    Cập nhật vi phạm ĐANG MỞ của các lần chạy TRƯỚC sau khi lần chạy `run_id` kết thúc:
      - SUPERSEDED: lần chạy này ghi lại đúng lỗi đó (cùng bảng, lô, loại lỗi)
      - RESOLVED  : lần chạy này kiểm tra lại đúng lô đó và cho qua (lô ACCEPTED), hoặc với vi phạm mức
                    bảng (không gắn lô) - bảng có phiên bản mới qua cổng
    Lô / bảng chưa được kiểm tra lại thì vi phạm vẫn OPEN. Trả về {"RESOLVED": n, "SUPERSEDED": m}.
    """
    conn = get_db_connection(db_path)
    lap_lai = {(d["affected_table"], d["batch"], d["error_type"]) for d in conn.execute(
        "SELECT affected_table, batch, error_type FROM violations WHERE run_id = ?", (run_id,))}
    lo_qua_cong = {(d["table_name"], d["batch"]) for d in conn.execute(
        "SELECT table_name, batch FROM batch_ingestions WHERE run_id = ? AND status = 'ACCEPTED'", (run_id,))}
    bang_da_ghi = {d["table_name"] for d in conn.execute(
        "SELECT table_name FROM dataset_versions WHERE run_id = ?", (run_id,))}
    dang_mo = conn.execute(
        "SELECT id, affected_table, batch, error_type FROM violations "
        "WHERE status = 'OPEN' AND run_id NOT IN (?, '')", (run_id,)).fetchall()

    cap_nhat = []
    for v in dang_mo:
        if (v["affected_table"], v["batch"], v["error_type"]) in lap_lai:
            cap_nhat.append(("SUPERSEDED", f"ghi nhận lại ở lần chạy {run_id}", v["id"]))
        elif v["batch"] and (v["affected_table"], v["batch"]) in lo_qua_cong:
            cap_nhat.append(("RESOLVED", f"lô {v['batch']} qua cổng ở lần chạy {run_id}", v["id"]))
        elif not v["batch"] and v["affected_table"] in bang_da_ghi:
            cap_nhat.append(("RESOLVED", f"bảng có phiên bản mới qua cổng ở lần chạy {run_id}", v["id"]))
    thoi_gian = now_str()
    conn.executemany(
        "UPDATE violations SET status = ?, resolution = ?, resolved_run_id = ?, resolved_at = ? WHERE id = ?",
        [(trang_thai, ly_do, run_id, thoi_gian, ma) for trang_thai, ly_do, ma in cap_nhat])
    conn.commit()
    conn.close()
    return {tt: sum(1 for c in cap_nhat if c[0] == tt) for tt in ("RESOLVED", "SUPERSEDED")}


def summarize(db_path: Optional[str] = None, since_id: int = 0, in_ra: bool = True) -> dict:
    """Thống kê vi phạm theo mức độ, loại lỗi và bệnh viện; in ra màn hình nếu in_ra=True."""
    danh_sach = get_violations(db_path, since_id=since_id)
    thong_ke = {"tong": len(danh_sach), "theo_muc_do": {}, "theo_loai": {}, "theo_benh_vien": {},
                "theo_trang_thai": {}}
    for v in danh_sach:
        for khoa, gia_tri in (("theo_muc_do", v["severity"]), ("theo_loai", v["error_type"]),
                              ("theo_benh_vien", v["hospital"]), ("theo_trang_thai", v["status"])):
            thong_ke[khoa][gia_tri] = thong_ke[khoa].get(gia_tri, 0) + 1

    if in_ra:
        print(f"Tổng số vi phạm: {thong_ke['tong']}")
        for tieu_de, khoa in (("Theo mức độ", "theo_muc_do"), ("Theo loại lỗi", "theo_loai"),
                              ("Theo bệnh viện", "theo_benh_vien"), ("Theo trạng thái", "theo_trang_thai")):
            print(f"  {tieu_de}:")
            for ten, so in sorted(thong_ke[khoa].items()):
                print(f"    - {ten}: {so}")
    return thong_ke


if __name__ == "__main__":
    summarize()
