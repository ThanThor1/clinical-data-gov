"""
Module Sổ Audit (audit_log)
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit & Báo cáo)

Nhiệm vụ theo phân công trong PDF (Mục 7 & 4):
- Cung cấp hàm log_event(...) ghi một dòng vào sổ audit gồm 5 thông tin:
  1. thời gian (thoi_gian)
  2. ai (ai)
  3. làm gì (lam_gi)
  4. bảng nào (bang_nao)
  5. kết quả (ket_qua)
  và thêm run_id: dòng audit thuộc lần chạy pipeline nào (để trống nếu không thuộc lần chạy nào).
- Lưu trữ mọi thao tác đọc/ghi, sự kiện kiểm tra cổng dữ liệu, và vi phạm quyền truy cập.
- Cung cấp các hàm truy vấn nhật ký phục vụ Dashboard và báo cáo.

Chống sửa sổ (tamper-evident): mỗi dòng lưu prev_hash (băm của dòng trước) và
row_hash = HMAC-SHA256(khóa audit, prev_hash + nội dung dòng).
- Sửa nội dung hay xóa một dòng ở giữa -> đứt chuỗi băm.
- Chèn dòng giả -> người không có khóa audit không tính được row_hash hợp lệ.
- Xóa các dòng cuối sổ -> sqlite_sequence vẫn nhớ id lớn nhất đã cấp -> phát hiện được.
verify_audit_chain() chỉ ra đúng dòng bị can thiệp. Khóa audit: AUDIT_LOG_SECRET hoặc file
.secrets/audit_log.key (xem catalog_access/keys.py).
"""

import contextvars
import hashlib
import hmac
import json
import os
import sys
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
import pandas as pd

# Thêm thư mục gốc dự án vào sys.path nếu cần
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from catalog_access.init_db import get_db_connection, now_str
from catalog_access.keys import get_key

KET_QUA_HOP_LE = ('ALLOWED', 'DENIED', 'BLOCKED', 'SUCCESS', 'FAILED')

# Dòng đầu tiên của sổ nối vào "mắt xích gốc" này
HASH_GOC = "0" * 64

# Ngữ cảnh ghi sổ: lần chạy (run_id) và lô dữ liệu (batch) đang xử lý.
# governance/provenance.py đặt ngữ cảnh khi bắt đầu một lần chạy, mọi log_event / violation
# trong lúc đó tự gắn run_id mà không phải truyền tay qua từng hàm.
_NGU_CANH: contextvars.ContextVar = contextvars.ContextVar("ngu_canh_audit", default=None)


def get_audit_context() -> Dict[str, str]:
    """Ngữ cảnh hiện tại: {'run_id': ..., 'batch': ...} (chuỗi rỗng nếu chưa đặt)."""
    return {"run_id": "", "batch": "", **(_NGU_CANH.get() or {})}


@contextmanager
def audit_context(run_id: Optional[str] = None, batch: Optional[str] = None):
    """
    Đặt ngữ cảnh ghi sổ trong một khối lệnh, ví dụ:
        with audit_context(run_id="run_...", batch="hospital_B/2026-10-29_20-00-00"):
            check_gate(...)   # violations và audit_log tự gắn run_id, batch
    Tham số None = giữ nguyên giá trị của ngữ cảnh bên ngoài.
    """
    moi = get_audit_context()
    if run_id is not None:
        moi["run_id"] = run_id
    if batch is not None:
        moi["batch"] = batch
    token = _NGU_CANH.set(moi)
    try:
        yield moi
    finally:
        _NGU_CANH.reset(token)


def get_audit_key() -> bytes:
    """Khóa HMAC của chuỗi băm sổ audit."""
    return get_key("AUDIT_LOG_SECRET", "AUDIT_LOG_KEY_FILE", "audit_log.key")


def compute_row_hash(prev_hash: str, ma: int, thoi_gian: str, ai: str, lam_gi: str,
                     bang_nao: str, ket_qua: str, run_id: str, khoa: Optional[bytes] = None) -> str:
    """HMAC-SHA256 của một dòng audit, nối với băm của dòng trước (prev_hash)."""
    noi_dung = json.dumps([prev_hash, int(ma), str(thoi_gian), str(ai), str(lam_gi),
                           str(bang_nao or ""), str(ket_qua), str(run_id or "")], ensure_ascii=False)
    return hmac.new(khoa if khoa is not None else get_audit_key(), noi_dung.encode("utf-8"),
                    hashlib.sha256).hexdigest()


def _chuan_hoa_ket_qua(ket_qua: Any) -> str:
    """Đưa kết quả về một trong KET_QUA_HOP_LE (giá trị lạ -> DENIED cho an toàn)."""
    ket_qua_str = str(ket_qua or "DENIED").strip().upper()
    if ket_qua_str in KET_QUA_HOP_LE:
        return ket_qua_str
    if ket_qua_str in {'TRUE', 'DAT', 'PASS', 'OK', 'THANH_CONG'}:
        return 'ALLOWED'
    return 'DENIED'


def _chen_dong_audit(conn: sqlite3.Connection, thoi_gian: str, ai: str, lam_gi: str, bang_nao: str,
                     ket_qua: str, run_id: str = "", ma: Optional[int] = None) -> int:
    """
    Chèn một dòng vào cuối chuỗi băm. Hàm không tự commit: người gọi phải giữ khóa ghi
    (BEGIN IMMEDIATE) để hai tiến trình không cùng nối vào một mắt xích.
    """
    cuoi = conn.execute("SELECT id, row_hash FROM audit_log ORDER BY id DESC LIMIT 1").fetchone()
    prev_hash = cuoi["row_hash"] if cuoi else HASH_GOC
    if ma is None:
        # Lấy id lớn nhất từng cấp (sqlite_sequence), không lấy id dòng cuối: nếu dòng cuối từng bị xóa
        # thì id mới để lại khoảng trống, verify_audit_chain vẫn thấy dấu vết thay vì bị lấp mất.
        try:
            so_thu_tu = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'audit_log'").fetchone()
        except sqlite3.OperationalError:
            so_thu_tu = None
        ma = max(so_thu_tu["seq"] if so_thu_tu else 0, cuoi["id"] if cuoi else 0) + 1
    row_hash = compute_row_hash(prev_hash, ma, thoi_gian, ai, lam_gi, bang_nao, ket_qua, run_id)
    conn.execute("""
    INSERT INTO audit_log (id, thoi_gian, ai, lam_gi, bang_nao, ket_qua, run_id, prev_hash, row_hash)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (ma, thoi_gian, ai, lam_gi, bang_nao or "", ket_qua, run_id or "", prev_hash, row_hash))
    return ma


def log_event(
    *args,
    ai: Optional[str] = None,
    hanh_dong: Optional[str] = None,
    lam_gi: Optional[str] = None,
    bang: Optional[str] = None,
    bang_nao: Optional[str] = None,
    ket_qua: Optional[str] = None,
    thoi_gian: Optional[str] = None,
    actor: Optional[str] = None,
    action: Optional[str] = None,
    target_table: Optional[str] = None,
    result: Optional[str] = None,
    run_id: Optional[str] = None,
    db_path: Optional[str] = None,
    **kwargs
) -> int:
    """
    Ghi một dòng vào sổ audit gồm 5 trường cốt lõi theo đúng yêu cầu đề tài:
    - thời gian (thoi_gian): Thời điểm phát sinh sự kiện (mặc định lấy thời gian hiện tại).
    - ai (ai / actor): Tác nhân thực hiện (Người dùng / Service / Pipeline / Role).
    - làm gì (lam_gi / hanh_dong / action): Hành động thực hiện (e.g., 'read', 'write', 'check_gate', 'schema_check').
    - bảng nào (bang_nao / bang / target_table): Bảng dữ liệu mục tiêu chịu tác động.
    - kết quả (ket_qua / result): Trạng thái kết quả ('ALLOWED', 'DENIED', 'BLOCKED', 'SUCCESS', 'FAILED').
    run_id mặc định lấy từ ngữ cảnh audit_context (lần chạy pipeline đang diễn ra).

    Hỗ trợ linh hoạt:
      + Cách 1 (theo bảng Mục 4, Trang 5): log_event(ai, hanh_dong, bang, ket_qua)
      + Cách 2 (theo mô tả Mục 7.4, Trang 8): log_event(thoi_gian, ai, lam_gi, bang_nao, ket_qua)
      + Cách 3: Truyền tham số có đặt tên (keyword arguments: thoi_gian=..., ai=..., lam_gi=..., bang_nao=..., ket_qua=...)

    Trả về id của dòng vừa ghi.
    """
    # Xử lý các đối số truyền theo vị trí (*args)
    v_thoi_gian = thoi_gian
    v_ai = ai or actor
    v_lam_gi = lam_gi or hanh_dong or action
    v_bang_nao = bang_nao or bang or target_table
    v_ket_qua = ket_qua or result

    if len(args) == 4:
        # log_event(ai, hanh_dong, bang, ket_qua)
        v_ai, v_lam_gi, v_bang_nao, v_ket_qua = args
    elif len(args) >= 5:
        # log_event(thoi_gian, ai, lam_gi, bang_nao, ket_qua)
        v_thoi_gian, v_ai, v_lam_gi, v_bang_nao, v_ket_qua = args[:5]

    if run_id is None:
        run_id = get_audit_context()["run_id"]

    conn = get_db_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        log_id = _chen_dong_audit(
            conn,
            str(v_thoi_gian or now_str()),
            str(v_ai or "unknown_user"),
            str(v_lam_gi or "unknown_action"),
            str(v_bang_nao or ""),
            _chuan_hoa_ket_qua(v_ket_qua),
            run_id,
        )
        conn.commit()
    finally:
        conn.close()
    return log_id


def verify_audit_chain(db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Kiểm tra toàn vẹn sổ audit bằng cách tính lại chuỗi băm từ dòng đầu tiên.

    Trả về dict:
        ok          : True nếu không dòng nào bị sửa / xóa / chèn
        so_dong     : số dòng đã kiểm tra
        hash_cuoi   : băm của dòng cuối (có thể ghi ra ngoài làm mốc đối chiếu)
        dong_loi    : id dòng đầu tiên bị can thiệp (None nếu ok)
        ly_do       : mô tả lỗi
    """
    conn = get_db_connection(db_path)
    dong = conn.execute(
        "SELECT id, thoi_gian, ai, lam_gi, bang_nao, ket_qua, run_id, prev_hash, row_hash "
        "FROM audit_log ORDER BY id"
    ).fetchall()
    try:
        so_thu_tu = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'audit_log'").fetchone()
    except sqlite3.OperationalError:
        so_thu_tu = None    # bảng sqlite_sequence chỉ có sau lần chèn đầu tiên
    conn.close()

    def _loi(ma, ly_do):
        return {"ok": False, "so_dong": len(dong), "hash_cuoi": None, "dong_loi": ma, "ly_do": ly_do}

    khoa = get_audit_key()
    truoc, id_truoc = HASH_GOC, None
    for d in dong:
        if d["prev_hash"] != truoc:
            return _loi(d["id"], f"Dòng #{d['id']}: không nối đúng vào dòng trước (có dòng bị xóa hoặc chèn thêm)")
        if id_truoc is not None and d["id"] != id_truoc + 1:
            return _loi(id_truoc + 1, f"Mất dòng #{id_truoc + 1}..#{d['id'] - 1} (id không liên tục)")
        id_truoc = d["id"]
        tinh_lai = compute_row_hash(d["prev_hash"], d["id"], d["thoi_gian"], d["ai"], d["lam_gi"],
                                    d["bang_nao"], d["ket_qua"], d["run_id"], khoa)
        if not hmac.compare_digest(tinh_lai, d["row_hash"]):
            return _loi(d["id"], f"Dòng #{d['id']}: nội dung bị sửa hoặc dòng không do hệ thống ghi")
        truoc = d["row_hash"]

    id_cuoi = dong[-1]["id"] if dong else 0
    if so_thu_tu and so_thu_tu["seq"] > id_cuoi:
        return _loi(id_cuoi + 1, f"Mất {so_thu_tu['seq'] - id_cuoi} dòng cuối sổ "
                                 f"(id {id_cuoi + 1}..{so_thu_tu['seq']} đã từng được ghi)")
    return {"ok": True, "so_dong": len(dong), "hash_cuoi": truoc, "dong_loi": None,
            "ly_do": "Chuỗi băm toàn vẹn"}


def get_audit_logs(
    limit: int = 200,
    ai: Optional[str] = None,
    actor: Optional[str] = None,
    ket_qua: Optional[str] = None,
    result: Optional[str] = None,
    run_id: Optional[str] = None,
    db_path: Optional[str] = None,
    as_df: bool = False
) -> Any:
    """
    Truy vấn danh sách sổ nhật ký kiểm toán từ bảng audit_log (mới nhất trước).
    Trả về đầy đủ 5 trường thông tin: thời gian, ai, làm gì, bảng nào, kết quả (+ run_id).
    Mỗi dòng có thêm tên tiếng Anh (timestamp, actor, action, target_table, result) cho code cũ.

    Tham số:
        limit (int): Số bản ghi tối đa lấy về (mới nhất trước).
        ai / actor (str, optional): Lọc theo tên người dùng / tác nhân.
        ket_qua / result (str, optional): Lọc theo kết quả ('ALLOWED', 'DENIED', 'BLOCKED',...).
        run_id (str, optional): Lọc theo lần chạy pipeline.
        db_path (str, optional): Đường dẫn tới metadata.db.
        as_df (bool): Trả về pandas DataFrame nếu True, ngược lại trả về list of dicts.
    """
    conn = get_db_connection(db_path)
    user_filter = ai or actor
    res_filter = ket_qua or result

    query = """
    SELECT
        id, thoi_gian, ai, lam_gi, bang_nao, ket_qua, run_id,
        thoi_gian AS timestamp, ai AS actor, lam_gi AS action,
        bang_nao AS target_table, ket_qua AS result
    FROM audit_log
    WHERE 1=1
    """
    params: List[Any] = []

    if user_filter:
        query += " AND ai LIKE ?"
        params.append(f"%{user_filter}%")
    if res_filter:
        query += " AND ket_qua = ?"
        params.append(res_filter.upper())
    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)

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
        ket_qua="ALLOWED"
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
        ket_qua="BLOCKED"
    )
    print(f"[OK] Cách 3 (Keyword arguments tiếng Việt) thành công, Log ID = {id3}")

    # Hiển thị các bản ghi trong sổ audit
    logs = get_audit_logs(limit=3)
    print("\n--- 3 BẢN GHI AUDIT LOG GẦN NHẤT ---")
    for log in logs:
        print(f"[{log['thoi_gian']}] Ai: {log['ai']} | Làm gì: {log['lam_gi']} | Bảng nào: {log['bang_nao']} | Kết quả: {log['ket_qua']}")

    kiem_tra = verify_audit_chain()
    print(f"\nToàn vẹn sổ audit: {'OK' if kiem_tra['ok'] else 'BỊ CAN THIỆP'} - {kiem_tra['ly_do']} "
          f"({kiem_tra['so_dong']} dòng)")
