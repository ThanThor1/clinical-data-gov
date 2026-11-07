"""
Provenance (nguồn gốc theo từng lần chạy): dữ liệu này được tạo ra TỪ ĐÂU, KHI NÀO, BỞI AI, bằng CODE nào.

Lineage (governance/lineage.py) trả lời "cột nào chảy vào cột nào" ở mức THIẾT KẾ.
Provenance trả lời "phiên bản bảng AI-ready này được tạo từ những LÔ dữ liệu nào, trong LẦN CHẠY nào"
ở mức THỰC THI. Cách ghi theo mô hình W3C PROV:

    Activity : một lần chạy pipeline       -> pipeline_runs     (run_id, ai chạy, lúc nào, phiên bản code)
    Entity   : một lô dữ liệu nguồn         -> batch_ingestions  (file, checksum SHA-256, số dòng, kết quả cổng)
               một phiên bản bảng đầu ra    -> dataset_versions  (file, checksum, số dòng)
    Quan hệ  : phiên bản được tạo từ lô / phiên bản nào -> dataset_version_inputs

Checksum giúp phát hiện file bị sửa ngoài pipeline: verify_input() chặn việc dùng một bảng
standardized không khớp với bất kỳ phiên bản nào pipeline đã ghi (vi phạm PROVENANCE_UNVERIFIED).
Với bảng Delta Lake: checksum tính trên các file Parquet của đúng một phiên bản trong Delta log và
lưu kèm số phiên bản (table_version) -> kiểm tra lại được cả phiên bản cũ nhờ time travel.

Cách dùng trong pipeline:
    with pipeline_run("pipeline_service", "chuẩn hóa") as run_id:
        with batch_context("hospital_B/2026-10-29_20-00-00"):
            ... check_gate(...)            # violations / audit tự gắn run_id và batch
            record_batch(...)
        record_dataset_version(..., inputs=[("batch", 12), ("batch", 13)])

Chạy thử: python -m governance.provenance   (in provenance của bảng AI-ready mới nhất)
"""

import glob
import hashlib
import json
import os
import secrets
import subprocess
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache
from typing import Optional
from urllib.parse import unquote, urlparse

from catalog_access import audit_context, get_audit_context, log_event
from catalog_access.data_access import from_catalog_path, is_delta_table, to_catalog_path
from catalog_access.init_db import get_db_connection, now_str

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Mã nguồn quyết định đầu ra của pipeline (đổi file nào là phiên bản code đổi)
MA_NGUON_PIPELINE = ("pipeline/*.py", "governance/*.py", "sources/*.yaml")


# ---------------------------------------------------------------------------
# 1. Tiện ích
# ---------------------------------------------------------------------------

def file_checksum(duong_dan: str) -> str:
    """SHA-256 nội dung file (đọc từng khối 1 MB, file lớn cũng không tốn RAM)."""
    bam = hashlib.sha256()
    with open(duong_dan, "rb") as f:
        for khoi in iter(lambda: f.read(1 << 20), b""):
            bam.update(khoi)
    return bam.hexdigest()


def _bam_danh_sach(thu_muc: str, cac_file) -> str:
    """Checksum của một tập file: SHA-256 của các dòng 'đường_dẫn_tương_đối:sha256' đã sắp xếp."""
    bam = hashlib.sha256()
    for ten in sorted(cac_file):
        bam.update(f"{ten}:{file_checksum(os.path.join(thu_muc, ten))}\n".encode("utf-8"))
    return bam.hexdigest()


def delta_version(duong_dan: str) -> Optional[int]:
    """Phiên bản hiện tại của bảng Delta (None nếu không phải bảng Delta)."""
    if not is_delta_table(duong_dan):
        return None
    from deltalake import DeltaTable
    return DeltaTable(duong_dan).version()


def path_checksum(duong_dan: str, version: Optional[int] = None) -> str:
    """
    Checksum của một file, hoặc của một bảng:
      - bảng Delta : các file Parquet thuộc phiên bản `version` (mặc định phiên bản hiện tại)
      - thư mục khác (Parquet): mọi file dữ liệu trong thư mục (bỏ file ẩn / file bắt đầu bằng '_')
    """
    if os.path.isfile(duong_dan):
        return file_checksum(duong_dan)
    if is_delta_table(duong_dan):
        from deltalake import DeltaTable
        cac_uri = DeltaTable(duong_dan, version=version).file_uris()
        return _bam_danh_sach(duong_dan, [os.path.relpath(unquote(urlparse(u).path), duong_dan) for u in cac_uri])
    cac_file = []
    for goc, thu_muc_con, ten_file in os.walk(duong_dan):
        thu_muc_con[:] = [d for d in thu_muc_con if not d.startswith(("_", "."))]
        cac_file += [os.path.relpath(os.path.join(goc, f), duong_dan) for f in ten_file
                     if not f.startswith(("_", "."))]
    return _bam_danh_sach(duong_dan, cac_file)


@lru_cache(maxsize=1)
def _git_describe() -> str:
    try:
        ket_qua = subprocess.run(["git", "describe", "--always", "--dirty"], cwd=ROOT,
                                 capture_output=True, text=True, timeout=5)
        if ket_qua.returncode == 0 and ket_qua.stdout.strip():
            return ket_qua.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return os.environ.get("CODE_VERSION", "")


@lru_cache(maxsize=1)
def source_hash() -> str:
    """SHA-256 (12 ký tự đầu) nội dung mã nguồn quyết định đầu ra: pipeline/*.py, governance/*.py, sources/*.yaml."""
    bam = hashlib.sha256()
    for mau in MA_NGUON_PIPELINE:
        for duong_dan in sorted(glob.glob(os.path.join(ROOT, mau))):
            bam.update(os.path.relpath(duong_dan, ROOT).encode())
            with open(duong_dan, "rb") as f:
                bam.update(f.read())
    return bam.hexdigest()[:12]


def get_code_version() -> str:
    """
    Phiên bản code đang chạy: '<git describe>+src.<băm mã nguồn>', ví dụ '5b38172-dirty+src.3fa1c09b2e7d'.
    git describe chỉ thêm '-dirty' (không đổi khi sửa tiếp code chưa commit) nên kèm băm NỘI DUNG mã nguồn:
    sửa một dòng code pipeline là phiên bản đổi. Không có git (ví dụ trong container) -> biến môi trường CODE_VERSION.
    Đang bật lỗi pipeline giả lập (PIPELINE_LOI_GIA_LAP, xem pipeline/inject_errors.py) -> thêm '+loi.<tên lỗi>',
    vì code chạy thật lúc đó khác code gốc.
    """
    phien_ban = f"{_git_describe() or 'khong-git'}+src.{source_hash()}"
    loi = os.environ.get("PIPELINE_LOI_GIA_LAP", "")
    return phien_ban + (f"+loi.{loi}" if loi else "")


def current_run_id() -> str:
    """run_id của lần chạy đang diễn ra ('' nếu không có)."""
    return get_audit_context()["run_id"]


# ---------------------------------------------------------------------------
# 2. Lần chạy (Activity)
# ---------------------------------------------------------------------------

def start_run(actor: str, note: str = "", db_path: Optional[str] = None) -> str:
    """Mở một lần chạy mới (trạng thái RUNNING), trả về run_id."""
    run_id = f"run_{datetime.now():%Y%m%d_%H%M%S}_{secrets.token_hex(3)}"
    conn = get_db_connection(db_path)
    conn.execute(
        "INSERT INTO pipeline_runs (run_id, actor, started_at, status, code_version, note) "
        "VALUES (?, ?, ?, 'RUNNING', ?, ?)",
        (run_id, actor, now_str(), get_code_version(), note),
    )
    conn.commit()
    conn.close()
    log_event(actor, "start_run", "", "SUCCESS", run_id=run_id, db_path=db_path)
    return run_id


def _tinh_trang_thai(run_id: str, db_path: Optional[str]) -> str:
    """
    SUCCESS : có đầu ra, không có vi phạm BREAKING nào
    PARTIAL : có đầu ra nhưng một số lô / bảng bị chặn
    BLOCKED : không tạo được phiên bản đầu ra nào (bị cổng chặn hết)
    """
    conn = get_db_connection(db_path)
    so_phien_ban = conn.execute("SELECT COUNT(*) FROM dataset_versions WHERE run_id = ?",
                                (run_id,)).fetchone()[0]
    so_loi_chan = conn.execute(
        "SELECT COUNT(*) FROM violations WHERE run_id = ? AND severity IN ('BREAKING', 'CRITICAL')",
        (run_id,)).fetchone()[0]
    conn.close()
    if so_phien_ban == 0:
        return "BLOCKED"
    return "PARTIAL" if so_loi_chan else "SUCCESS"


def finish_run(run_id: str, status: Optional[str] = None, db_path: Optional[str] = None) -> str:
    """
    Đóng lần chạy. status=None -> tự tính từ đầu ra và vi phạm của lần chạy. Trả về trạng thái.
    Lần chạy không lỗi bất ngờ thì cập nhật trạng thái các vi phạm cũ (RESOLVED / SUPERSEDED).
    """
    from governance.violation_engine import update_statuses_after_run

    trang_thai = status or _tinh_trang_thai(run_id, db_path)
    if trang_thai != "FAILED":
        update_statuses_after_run(run_id, db_path)
    conn = get_db_connection(db_path)
    dong = conn.execute("SELECT actor FROM pipeline_runs WHERE run_id = ?", (run_id,)).fetchone()
    conn.execute("UPDATE pipeline_runs SET finished_at = ?, status = ? WHERE run_id = ?",
                 (now_str(), trang_thai, run_id))
    conn.commit()
    conn.close()
    log_event(dong["actor"] if dong else "unknown_user", f"finish_run:{trang_thai}", "",
              "FAILED" if trang_thai == "FAILED" else "SUCCESS", run_id=run_id, db_path=db_path)
    return trang_thai


@contextmanager
def pipeline_run(actor: str, note: str = "", db_path: Optional[str] = None):
    """
    Bọc một lần chạy pipeline. Nếu đã nằm trong một lần chạy khác (ví dụ run_demo gọi
    standardize rồi build_ai_ready) thì dùng lại run_id bên ngoài, không mở lần chạy mới.
    Lỗi bất ngờ (exception) -> lần chạy được đóng với trạng thái FAILED.
    """
    dang_chay = current_run_id()
    if dang_chay:
        yield dang_chay
        return

    run_id = start_run(actor, note, db_path)
    try:
        with audit_context(run_id=run_id, batch=""):
            yield run_id
    except BaseException:
        finish_run(run_id, "FAILED", db_path)
        raise
    finish_run(run_id, db_path=db_path)


def batch_context(batch: str):
    """Đánh dấu lô dữ liệu đang xử lý (vi phạm phát sinh trong khối này ghi kèm tên lô)."""
    return audit_context(batch=batch)


# ---------------------------------------------------------------------------
# 3. Lô dữ liệu nguồn và phiên bản đầu ra (Entity)
# ---------------------------------------------------------------------------

def record_batch(table_name: str, hospital: str, batch: str, source_file: str, row_count: int,
                 status: str, schema_version: Optional[int] = None, contract: str = "",
                 run_id: Optional[str] = None, db_path: Optional[str] = None,
                 checksum: Optional[str] = None, mapping: str = "") -> int:
    """
    Ghi provenance của một lô: file nguồn, checksum, số dòng, phiên bản schema, hợp đồng áp dụng,
    kết quả (ACCEPTED: qua cả 2 cổng | BLOCKED: bị chặn | SKIPPED: bỏ qua vì bảng patient cùng đợt bị chặn).
    checksum: SHA-256 của file nếu bên gọi đã tính sẵn (không truyền thì tự tính).
    mapping : đổi tên cột đã duyệt được áp khi đọc lô (ví dụ 'weight -> admissionweight (#1)').
    Trả về id của lô.
    """
    conn = get_db_connection(db_path)
    con_tro = conn.execute(
        "INSERT INTO batch_ingestions (run_id, table_name, hospital, batch, source_file, checksum, "
        "row_count, schema_version, contract, status, recorded_at, mapping) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id or current_run_id(), table_name, hospital, batch, to_catalog_path(source_file),
         checksum or file_checksum(source_file), int(row_count), schema_version, contract, status, now_str(),
         mapping),
    )
    ma = con_tro.lastrowid
    conn.commit()
    conn.close()
    return ma


def input_fingerprint(inputs: list[tuple[str, int]], db_path: Optional[str] = None) -> str:
    """
    Dấu vân tay của tập đầu vào theo các LÔ RAW gốc (đi ngược qua các phiên bản trung gian) và NỘI DUNG của
    chúng (checksum), không theo id. Không lấy checksum của phiên bản trung gian vì mỗi lần ghi lại, Delta tạo
    file Parquet mới (checksum đổi) dù dữ liệu y hệt. Dùng để biết hai phiên bản có cùng đầu vào không.
    """
    lo = []
    for loai, ma in inputs:
        if loai == "dataset_version":
            lo += source_batches(int(ma), db_path)
        else:
            lo += _truy_van("SELECT * FROM batch_ingestions WHERE id = ?", (int(ma),), db_path)
    phan = sorted({f"{b['table_name']}:{b['checksum']}" for b in lo})
    return hashlib.sha256("\n".join(phan).encode()).hexdigest()


def tom_tat_ho_so(ho_so: dict) -> dict:
    """Phần hồ sơ đầu ra lưu kèm phiên bản: số dòng; kiểu, % trống, trung bình của từng cột."""
    return {"so_dong": ho_so["so_dong"],
            "cot": {ten: {k: tt[k] for k in ("type", "null_pct", "mean") if k in tt} for ten, tt in ho_so["cot"].items()}}


def record_dataset_version(table_name: str, tier: str, file_path: str, row_count: int,
                           inputs: list[tuple[str, int]], run_id: Optional[str] = None,
                           db_path: Optional[str] = None, ho_so: Optional[dict] = None) -> int:
    """
    Ghi một phiên bản bảng đầu ra vừa được tạo (file / bảng Delta đã ghi xong) và các đầu vào của nó:
    inputs = [("batch", id_lô), ("dataset_version", id_phiên_bản), ...]. Trả về id phiên bản.
    Lưu kèm phiên bản code, dấu vân tay đầu vào và (nếu có) hồ sơ đầu ra -> governance/pipeline_drift.py
    so được lần chạy sau với lần này.
    """
    conn = get_db_connection(db_path)
    con_tro = conn.execute(
        "INSERT INTO dataset_versions (run_id, table_name, tier, file_path, checksum, row_count, created_at, "
        "table_version, code_version, input_fingerprint, profile_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id or current_run_id(), table_name, tier, to_catalog_path(file_path),
         path_checksum(file_path), int(row_count), now_str(), delta_version(file_path), get_code_version(),
         input_fingerprint(inputs, db_path),
         json.dumps(tom_tat_ho_so(ho_so), ensure_ascii=False) if ho_so else ""),
    )
    ma = con_tro.lastrowid
    conn.executemany(
        "INSERT OR IGNORE INTO dataset_version_inputs (version_id, input_type, input_id) VALUES (?, ?, ?)",
        [(ma, loai, int(ma_vao)) for loai, ma_vao in inputs],
    )
    conn.commit()
    conn.close()
    return ma


def find_version_by_checksum(table_name: str, checksum: str,
                             db_path: Optional[str] = None) -> Optional[dict]:
    """Phiên bản mới nhất của bảng có đúng checksum này (None nếu không có)."""
    conn = get_db_connection(db_path)
    dong = conn.execute(
        "SELECT * FROM dataset_versions WHERE table_name = ? AND checksum = ? ORDER BY id DESC LIMIT 1",
        (table_name, checksum)).fetchone()
    conn.close()
    return dict(dong) if dong else None


def verify_input(table_name: str, duong_dan: str, db_path: Optional[str] = None) -> Optional[int]:
    """
    "Cổng provenance": chỉ cho dùng file đầu vào do chính pipeline tạo ra.
    File khớp checksum với một phiên bản đã ghi -> trả về id phiên bản đó.
    Không khớp (file bị sửa tay, chép từ nơi khác...) -> ghi vi phạm BREAKING PROVENANCE_UNVERIFIED,
    trả về None để bên gọi dừng lại.
    """
    from governance.violation_engine import report_violation

    checksum = path_checksum(duong_dan)
    phien_ban = find_version_by_checksum(table_name, checksum, db_path)
    if phien_ban:
        return phien_ban["id"]
    report_violation(
        "PROVENANCE_UNVERIFIED", "BREAKING", table_name,
        f"{to_catalog_path(duong_dan)} (sha256 {checksum[:12]}…) không khớp với phiên bản nào do "
        f"pipeline tạo ra - có thể đã bị sửa ngoài pipeline, không dùng để tạo dữ liệu tầng sau",
        db_path=db_path,
    )
    return None


# ---------------------------------------------------------------------------
# 4. Tra cứu
# ---------------------------------------------------------------------------

def _truy_van(cau_lenh: str, tham_so: tuple, db_path: Optional[str]) -> list[dict]:
    conn = get_db_connection(db_path)
    dong = conn.execute(cau_lenh, tham_so).fetchall()
    conn.close()
    return [dict(d) for d in dong]


def get_runs(limit: int = 50, db_path: Optional[str] = None) -> list[dict]:
    """Các lần chạy, mới nhất trước."""
    return _truy_van("SELECT * FROM pipeline_runs ORDER BY started_at DESC, rowid DESC LIMIT ?",
                     (limit,), db_path)


def get_batches(run_id: Optional[str] = None, db_path: Optional[str] = None) -> list[dict]:
    """Các lô đã ghi (của một lần chạy, hoặc tất cả)."""
    if run_id:
        return _truy_van("SELECT * FROM batch_ingestions WHERE run_id = ? ORDER BY id", (run_id,), db_path)
    return _truy_van("SELECT * FROM batch_ingestions ORDER BY id", (), db_path)


def get_dataset_versions(table_name: Optional[str] = None, db_path: Optional[str] = None) -> list[dict]:
    """Các phiên bản bảng đầu ra, mới nhất trước."""
    if table_name:
        return _truy_van("SELECT * FROM dataset_versions WHERE table_name = ? ORDER BY id DESC",
                         (table_name,), db_path)
    return _truy_van("SELECT * FROM dataset_versions ORDER BY id DESC", (), db_path)


def get_latest_version(table_name: str, db_path: Optional[str] = None) -> Optional[dict]:
    phien_ban = get_dataset_versions(table_name, db_path)
    return phien_ban[0] if phien_ban else None


def trace_version(version_id: int, db_path: Optional[str] = None) -> Optional[dict]:
    """
    Cây provenance của một phiên bản: {"version": {...}, "inputs": [nút con...]}.
    Nút con là phiên bản khác ({"version", "inputs"}) hoặc lô dữ liệu ({"batch": {...}}).
    """
    dong = _truy_van("SELECT * FROM dataset_versions WHERE id = ?", (version_id,), db_path)
    if not dong:
        return None
    dau_vao = _truy_van("SELECT input_type, input_id FROM dataset_version_inputs WHERE version_id = ? "
                        "ORDER BY input_type DESC, input_id", (version_id,), db_path)
    con = []
    for d in dau_vao:
        if d["input_type"] == "dataset_version":
            nut = trace_version(d["input_id"], db_path)
            if nut:
                con.append(nut)
        else:
            lo = _truy_van("SELECT * FROM batch_ingestions WHERE id = ?", (d["input_id"],), db_path)
            if lo:
                con.append({"batch": lo[0]})
    return {"version": dong[0], "inputs": con}


def source_batches(version_id: int, db_path: Optional[str] = None) -> list[dict]:
    """Mọi lô dữ liệu nguồn (ở tầng raw) đã góp vào một phiên bản, không trùng."""
    ket_qua, da_co = [], set()

    def _duyet(nut):
        for con in nut["inputs"]:
            if "batch" in con:
                if con["batch"]["id"] not in da_co:
                    da_co.add(con["batch"]["id"])
                    ket_qua.append(con["batch"])
            else:
                _duyet(con)

    goc = trace_version(version_id, db_path)
    if goc:
        _duyet(goc)
    return sorted(ket_qua, key=lambda b: b["id"])


def verify_version(version_id: int, db_path: Optional[str] = None) -> dict:
    """
    Kiểm tra lại một phiên bản: file đầu ra và mọi file nguồn còn đúng checksum đã ghi không.
    Trả về {"ok": bool, "da_kiem": số file đã kiểm, "van_de": [mô tả...]}.
    """
    van_de, da_kiem = [], 0

    def _kiem(nhan: str, duong_dan_catalog: str, checksum: str, phien_ban_delta: Optional[int] = None):
        nonlocal da_kiem
        da_kiem += 1
        duong_dan = from_catalog_path(duong_dan_catalog)
        if not os.path.exists(duong_dan):
            van_de.append(f"{nhan}: không còn file {duong_dan_catalog}")
            return
        try:
            khop = path_checksum(duong_dan, phien_ban_delta) == checksum
        except Exception as loi:    # ví dụ phiên bản Delta đã bị dọn (VACUUM) hoặc file Parquet bị xóa
            van_de.append(f"{nhan}: không đọc được {duong_dan_catalog} ({type(loi).__name__})")
            return
        if not khop:
            van_de.append(f"{nhan}: {duong_dan_catalog} đã thay đổi sau khi ghi provenance")

    def _duyet(nut):
        v = nut["version"]
        _kiem(f"{v['table_name']} (phiên bản #{v['id']})", v["file_path"], v["checksum"], v.get("table_version"))
        for con in nut["inputs"]:
            if "batch" in con:
                b = con["batch"]
                _kiem(f"lô #{b['id']} {b['table_name']} / {b['batch']}", b["source_file"], b["checksum"])
            else:
                _duyet(con)

    goc = trace_version(version_id, db_path)
    if goc is None:
        return {"ok": False, "da_kiem": 0, "van_de": [f"Không có phiên bản #{version_id}"]}
    _duyet(goc)
    return {"ok": not van_de, "da_kiem": da_kiem, "van_de": van_de}


def format_trace(nut: dict, thut_le: str = "") -> list[str]:
    """Cây provenance dạng các dòng chữ để in ra màn hình."""
    v = nut["version"]
    delta = f" | Delta v{v['table_version']}" if v.get("table_version") is not None else ""
    dong = [f"{thut_le}{v['table_name']} #{v['id']}{delta} | {v['row_count']} dòng | sha256 {v['checksum'][:12]}… "
            f"| run {v['run_id']} | {v['created_at']}"]
    for con in nut["inputs"]:
        if "batch" in con:
            b = con["batch"]
            dong.append(f"{thut_le}  └─ lô #{b['id']} {b['table_name']} / {b['batch']} | {b['row_count']} dòng "
                        f"| {b['status']} | sha256 {b['checksum'][:12]}… | {b['source_file']}")
        else:
            dong.extend(format_trace(con, thut_le + "  "))
    return dong


if __name__ == "__main__":
    moi_nhat = get_latest_version("ai_ready_icu_mortality")
    if not moi_nhat:
        print("Chưa có phiên bản AI-ready nào. Chạy: python run_demo.py --reset")
    else:
        print("\n".join(format_trace(trace_version(moi_nhat["id"]))))
        kiem_tra = verify_version(moi_nhat["id"])
        print(f"\nKiểm tra checksum: {'OK' if kiem_tra['ok'] else 'CÓ VẤN ĐỀ'} ({kiem_tra['da_kiem']} file)")
        for v in kiem_tra["van_de"]:
            print("  -", v)
