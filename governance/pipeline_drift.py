"""
Phát hiện vi phạm governance do PIPELINE thay đổi (code biến đổi sai), không phải do dữ liệu nguồn.

Hợp đồng dữ liệu chỉ bắt được lỗi làm giá trị ra ngoài luật (sai kiểu, ngoài khoảng, ...). Một lỗi code tinh vi
vẫn có thể cho ra dữ liệu "đúng luật" nhưng sai nghĩa, ví dụ đổi cân nặng sang pound (70 kg -> 154, vẫn trong
[20, 400]) hay đảo nhãn tử vong (vẫn là 0/1). Cách bắt: mỗi phiên bản bảng đầu ra lưu kèm phiên bản code,
dấu vân tay đầu vào và hồ sơ đầu ra (governance/provenance.record_dataset_version). Trước khi ghi phiên bản mới:

    đã có phiên bản do CHÍNH code này tạo từ CÙNG đầu vào -> bỏ qua (chạy lại, kết quả đã được chấp nhận)
    code đã được duyệt là cố ý đổi đầu ra (approve_code_version) -> bỏ qua
    có phiên bản do code KHÁC tạo từ CÙNG đầu vào (tìm cả lịch sử, không chỉ phiên bản mới nhất)
        -> so với phiên bản đó; lệch -> BREAKING PIPELINE_OUTPUT_DRIFT, chặn không ghi (chắc chắn do code)
    không có, nhưng phiên bản mới nhất do code khác tạo (code và dữ liệu cùng đổi)
        -> so với phiên bản mới nhất; lệch -> WARNING (không tách được nguyên nhân)
    phiên bản mới nhất do chính code này tạo -> bỏ qua (thay đổi do dữ liệu, đã có cổng kiểm tra)
  So: cột bị thêm / bớt / đổi kiểu; trung bình cột số lệch > 20%; % ô trống tăng > 10 điểm; số dòng khác (chỉ khi
  cùng đầu vào: cùng đầu vào thì code đúng phải ra cùng số dòng).

Thay đổi đầu ra là CỐ Ý (sửa lỗi, thêm nguồn ...) thì kỹ sư dữ liệu duyệt phiên bản code:
    python -m governance.pipeline_drift --duyet <phiên bản code> --nguoi engineer_alice --ly-do "..."
"""

import argparse
import json
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catalog_access import log_event
from catalog_access.init_db import get_db_connection, now_str
from governance.provenance import get_code_version, input_fingerprint, tom_tat_ho_so
from governance.violation_engine import report_violation

NGUONG_LECH_TRUNG_BINH = 0.20
NGUONG_TANG_O_TRONG = 10.0

# Tắt bộ phát hiện (chỉ dùng trong run_eval.py để đo khi KHÔNG có bộ phát hiện thì lỗi lọt vào bao nhiêu dòng)
BAT_KIEM_TRA = True


def compare_outputs(cu: dict, moi: dict, cung_dau_vao: bool) -> list[tuple[Optional[str], str]]:
    """So hồ sơ đầu ra cũ và mới (dạng tom_tat_ho_so). Trả về [(cột hoặc None, mô tả lệch)]."""
    lech = []
    if cung_dau_vao and cu["so_dong"] != moi["so_dong"]:
        lech.append((None, f"số dòng {cu['so_dong']} -> {moi['so_dong']} dù cùng đầu vào"))
    for cot in cu["cot"]:
        if cot not in moi["cot"]:
            lech.append((cot, f"cột '{cot}' không còn trong đầu ra"))
    for cot, tt in moi["cot"].items():
        tt_cu = cu["cot"].get(cot)
        if tt_cu is None:
            lech.append((cot, f"cột mới '{cot}' trong đầu ra"))
            continue
        if "unknown" not in (tt_cu["type"], tt["type"]) and tt_cu["type"] != tt["type"]:
            lech.append((cot, f"cột '{cot}' đổi kiểu {tt_cu['type']} -> {tt['type']}"))
        if tt["null_pct"] - tt_cu["null_pct"] > NGUONG_TANG_O_TRONG:
            lech.append((cot, f"cột '{cot}' trống {tt_cu['null_pct']}% -> {tt['null_pct']}%"))
        a, b = tt_cu.get("mean"), tt.get("mean")
        if a is not None and b is not None and abs(b - a) > NGUONG_LECH_TRUNG_BINH * max(abs(a), abs(b), 1e-9):
            lech.append((cot, f"cột '{cot}' trung bình {a:g} -> {b:g} ({(b - a) / abs(a) * 100 if a else float('inf'):+.0f}%)"))
    return lech


def approve_code_version(nguoi_duyet: str, code_version: str, ly_do: str = "", db_path: Optional[str] = None) -> None:
    """Kỹ sư dữ liệu duyệt: phiên bản code này CỐ Ý đổi đầu ra -> bộ phát hiện không chặn nó nữa. Ghi audit."""
    from catalog_access.access import get_role_info

    if not get_role_info(nguoi_duyet).get("can_write"):
        log_event(ai=nguoi_duyet, hanh_dong="approve_code_version", bang="", ket_qua="DENIED", db_path=db_path)
        raise PermissionError(f"'{nguoi_duyet}' không có quyền ghi dữ liệu nên không được duyệt phiên bản code")
    conn = get_db_connection(db_path)
    conn.execute("INSERT OR REPLACE INTO code_approvals (code_version, approved_by, reason, approved_at) "
                 "VALUES (?, ?, ?, ?)", (code_version, nguoi_duyet, ly_do, now_str()))
    conn.commit()
    conn.close()
    log_event(ai=nguoi_duyet, hanh_dong=f"approve_code_version:{code_version}", bang="", ket_qua="SUCCESS",
              db_path=db_path)


def check_pipeline_drift(table_name: str, ho_so: dict, inputs: list[tuple[str, int]],
                         db_path: Optional[str] = None) -> tuple[bool, list[str]]:
    """
    Gọi TRƯỚC khi ghi phiên bản mới của `table_name` (hồ sơ đầu ra `ho_so`, đầu vào `inputs`).
    Trả về (được ghi?, danh sách mô tả lệch). Lệch thì ghi vi phạm PIPELINE_OUTPUT_DRIFT (xem đầu file).
    """
    if not BAT_KIEM_TRA:
        return True, []
    code_moi = get_code_version()
    dau_vao = input_fingerprint(inputs, db_path)
    cau_lenh = ("SELECT code_version, input_fingerprint, profile_json FROM dataset_versions "
                "WHERE table_name = ? AND profile_json != ''")
    conn = get_db_connection(db_path)
    da_tai_lap = conn.execute(cau_lenh + " AND code_version = ? AND input_fingerprint = ? LIMIT 1",
                              (table_name, code_moi, dau_vao)).fetchone() is not None
    da_duyet = conn.execute("SELECT 1 FROM code_approvals WHERE code_version = ?", (code_moi,)).fetchone() is not None
    truoc = (conn.execute(cau_lenh + " AND input_fingerprint = ? ORDER BY id DESC LIMIT 1",
                          (table_name, dau_vao)).fetchone()
             or conn.execute(cau_lenh + " ORDER BY id DESC LIMIT 1", (table_name,)).fetchone())
    conn.close()
    if truoc is None or truoc["code_version"] == code_moi or da_tai_lap or da_duyet:
        return True, []

    cung_dau_vao = truoc["input_fingerprint"] == dau_vao
    lech = compare_outputs(json.loads(truoc["profile_json"]), tom_tat_ho_so(ho_so), cung_dau_vao)
    if not lech:
        return True, []
    muc_do = "BREAKING" if cung_dau_vao else "WARNING"
    boi_canh = (f"code pipeline đổi {truoc['code_version']} -> {code_moi}, "
                + ("CÙNG đầu vào nên lệch do code" if cung_dau_vao else "đầu vào cũng đổi nên chưa chắc do code"))
    for cot, mo_ta in lech:
        report_violation("PIPELINE_OUTPUT_DRIFT", muc_do, table_name, f"{mo_ta} ({boi_canh})", column=cot,
                         blocked=cung_dau_vao, db_path=db_path)
    return not cung_dau_vao, [mo_ta for _, mo_ta in lech]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Duyệt phiên bản code pipeline cố ý đổi đầu ra")
    parser.add_argument("--duyet", required=True, help="phiên bản code (xem cột code_version trên dashboard)")
    parser.add_argument("--nguoi", required=True, help="tài khoản người duyệt (vai trò được ghi dữ liệu)")
    parser.add_argument("--ly-do", default="", help="lý do đổi đầu ra")
    tham_so = parser.parse_args()
    approve_code_version(tham_so.nguoi, tham_so.duyet, tham_so.ly_do)
    print(f"Đã duyệt phiên bản code {tham_so.duyet}")
