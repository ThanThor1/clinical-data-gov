"""
Chạy toàn bộ kịch bản demo bằng một lệnh (từ gốc repo):

    python run_demo.py            # dùng metadata.db hiện có (chỉ xét các dòng mới sinh ra)
    python run_demo.py --reset    # xóa metadata.db cũ, chạy lại từ đầu

Kịch bản:
  1. Dữ liệu sạch của 3 bệnh viện đi qua 3 tầng -> không có vi phạm BREAKING.
  2. Bệnh viện B (đợt 3) đổi tên cột admissionweight -> weight -> bị chặn,
     in vi phạm và bảng AI-ready bị ảnh hưởng, audit_log có dòng BLOCKED,
     provenance ghi lô đó là BLOCKED.
  3. Bệnh viện B sửa lại dữ liệu -> chạy lại -> qua cổng.
  4. Phân quyền được thực thi khi đọc dữ liệu: nhà nghiên cứu đọc raw bị từ chối (DENIED),
     chuyên viên phân tích đọc standardized nhưng cột định danh bị ẩn.
  5. Provenance: truy ngược bảng AI-ready về từng lô nguồn (checksum); sửa tay file
     standardized -> tạo AI-ready bị chặn (PROVENANCE_UNVERIFIED).
  6. Sổ audit chống sửa: chuỗi băm toàn vẹn; sửa một dòng (trên bản sao DB) -> bị phát hiện.
"""

import argparse
import os
import shutil
import sqlite3
import sys
import tempfile

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import pandas as pd  # noqa: E402

from catalog_access import AccessDenied, read_table, verify_audit_chain  # noqa: E402
from catalog_access.init_db import DEFAULT_DB_PATH, get_db_connection, init_database  # noqa: E402
from governance import provenance  # noqa: E402
from governance.violation_engine import get_violations  # noqa: E402
from pipeline import build_ai_ready, ingest_raw, inject_errors, split_hospitals, standardize  # noqa: E402

AI = "ai_ready_icu_mortality"
ACTOR = "pipeline_service"


def tieu_de(chu):
    print("\n" + "=" * 78 + f"\n{chu}\n" + "=" * 78, flush=True)


def id_lon_nhat(bang):
    """id lớn nhất hiện có trong bảng (violations / audit_log) để chỉ xét các dòng mới."""
    conn = get_db_connection()
    gia_tri = conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {bang}").fetchone()[0]
    conn.close()
    return gia_tri


def audit_moi(tu_id, ket_qua=None):
    """Các dòng audit_log sinh ra sau id `tu_id` (lọc theo kết quả nếu có)."""
    conn = get_db_connection()
    cau_lenh = "SELECT id, ai, lam_gi, bang_nao, ket_qua FROM audit_log WHERE id > ?"
    tham_so = [tu_id]
    if ket_qua:
        cau_lenh += " AND ket_qua = ?"
        tham_so.append(ket_qua)
    dong = conn.execute(cau_lenh, tham_so).fetchall()
    conn.close()
    return [dict(d) for d in dong]


def trang_thai_run(run_id):
    return next((r["status"] for r in provenance.get_runs(limit=500) if r["run_id"] == run_id), None)


def chay_pipeline(ghi_chu):
    """standardize -> build_ai_ready trong CÙNG một lần chạy. Trả về (kq standardize, AI-ready df, run_id)."""
    with provenance.pipeline_run(ACTOR, ghi_chu) as run_id:
        tieu_de(f"Chuẩn hóa: raw -> standardized (standardize) | {run_id}")
        kq_std = standardize.main()
        tieu_de(f"Tạo tầng AI-ready (build_ai_ready) | {run_id}")
        ai_df = build_ai_ready.main()
    return kq_std, ai_df, run_id


def in_tom_tat(kq_std, ai_df, run_id, vi_pham_breaking):
    print(f"  Lần chạy              : {run_id} -> {trang_thai_run(run_id)}")
    print(f"  Số đợt đi qua đủ cổng : {kq_std['so_dot_dat']}/{kq_std['so_dot']}")
    if kq_std["bi_chan"]:
        print(f"  Bị chặn               : {', '.join(kq_std['bi_chan'])}")
    print(f"  {AI}: {len(ai_df) if ai_df is not None else 'KHÔNG tạo được'} dòng")
    print(f"  Vi phạm BREAKING mới  : {len(vi_pham_breaking)}")


def kich_ban_phan_quyen():
    """Kịch bản 4. Trả về True nếu mọi kỳ vọng đều đúng."""
    audit_dau = id_lon_nhat("audit_log")
    cac_lan_thu = [
        ("researcher_bob", "raw_patient_hospital_B", False),
        ("researcher_bob", AI, True),
        ("analyst_dave", "standardized_patients", True),
        ("guest_charlie", "standardized_patients", False),
    ]
    dung_ky_vong = True
    cot_analyst = []
    for nguoi_dung, bang, ky_vong in cac_lan_thu:
        try:
            df = read_table(nguoi_dung, bang)
            print(f"  ✅ {nguoi_dung:<15} đọc {bang:<24} ĐƯỢC PHÉP: {len(df)} dòng, cột {list(df.columns)}")
            dung_ky_vong &= ky_vong
            if nguoi_dung == "analyst_dave":
                cot_analyst = list(df.columns)
        except AccessDenied as loi:
            print(f"  ⛔ {nguoi_dung:<15} đọc {bang:<24} TỪ CHỐI: {loi.reason}")
            dung_ky_vong &= not ky_vong

    audit = audit_moi(audit_dau)
    print("\n  Dòng audit_log vừa ghi:")
    for a in audit:
        print(f"     ai={a['ai']:<15} | làm gì={a['lam_gi']:<20} | bảng={a['bang_nao']:<24} | {a['ket_qua']}")
    return (dung_ky_vong
            and "patient_id" not in cot_analyst and "weight_kg" in cot_analyst
            and any(a["ai"] == "researcher_bob" and a["bang_nao"] == "raw_patient_hospital_B"
                    and a["ket_qua"] == "DENIED" for a in audit)
            and any(a["ai"] == "analyst_dave" and a["lam_gi"].startswith("mask:patient_id") for a in audit))


def kich_ban_provenance():
    """Kịch bản 5. Trả về True nếu mọi kỳ vọng đều đúng."""
    phien_ban = provenance.get_latest_version(AI)
    if phien_ban is None:
        print("  ❌ Chưa có phiên bản AI-ready nào")
        return False
    cay = provenance.trace_version(phien_ban["id"])
    for dong in provenance.format_trace(cay):
        if "└─ lô" not in dong:
            print("  " + dong)
    lo_nguon = provenance.source_batches(phien_ban["id"])
    theo_vien = pd.Series([b["hospital"] for b in lo_nguon]).value_counts().sort_index().to_dict()
    print(f"  -> {len(lo_nguon)} lô nguồn ở tầng raw: {theo_vien}")
    for b in lo_nguon[:3]:
        print(f"     lô #{b['id']} {b['table_name']} / {b['batch']} | {b['row_count']} dòng "
              f"| sha256 {b['checksum'][:16]}… | {b['source_file']}")
    print("     ...")
    kiem_tra = provenance.verify_version(phien_ban["id"])
    print(f"  Kiểm tra lại checksum: {'OK' if kiem_tra['ok'] else 'CÓ VẤN ĐỀ'} ({kiem_tra['da_kiem']} file)")

    print("\n  Sửa tay 1 ô cân nặng trong data/standardized/standardized_patients.csv rồi tạo lại AI-ready:")
    duong_dan = os.path.join(standardize.STD_DIR, "standardized_patients.csv")
    df = pd.read_csv(duong_dan)
    df.loc[0, "weight_kg"] = 999.0
    df.to_csv(duong_dan, index=False)
    with provenance.pipeline_run(ACTOR, "thử tạo AI-ready từ file bị sửa tay") as run_id:
        ai_df = build_ai_ready.main()
    vp = get_violations(run_id=run_id)
    bi_chan = ai_df is None and any(v["error_type"] == "PROVENANCE_UNVERIFIED" for v in vp)
    print(f"  -> {'⛔ Bị chặn (PROVENANCE_UNVERIFIED)' if bi_chan else '❌ KHÔNG bị chặn'} | lần chạy {run_id}: "
          f"{trang_thai_run(run_id)}")

    print("\n  Chạy lại pipeline để khôi phục tầng standardized và AI-ready từ dữ liệu raw:")
    _, ai_df, run_id = chay_pipeline("khôi phục sau khi file standardized bị sửa tay")
    khoi_phuc = ai_df is not None and trang_thai_run(run_id) == "SUCCESS"
    print(f"  -> {'✅ Đã khôi phục' if khoi_phuc else '❌ Chưa khôi phục'} ({run_id})")
    return kiem_tra["ok"] and len(lo_nguon) > 0 and bi_chan and khoi_phuc


def kich_ban_audit():
    """Kịch bản 6. Trả về True nếu mọi kỳ vọng đều đúng."""
    that = verify_audit_chain()
    print(f"  metadata.db: {'✅' if that['ok'] else '❌'} {that['ly_do']} ({that['so_dong']} dòng, "
          f"băm cuối {str(that['hash_cuoi'])[:16]}…)")

    thu_muc = tempfile.mkdtemp()
    ban_sao = os.path.join(thu_muc, "ban_sao.db")
    shutil.copy(DEFAULT_DB_PATH, ban_sao)
    conn = sqlite3.connect(ban_sao)
    dong = conn.execute("SELECT id FROM audit_log WHERE ket_qua = 'DENIED' ORDER BY id LIMIT 1").fetchone()
    conn.execute("UPDATE audit_log SET ket_qua = 'ALLOWED' WHERE id = ?", (dong[0],))
    conn.commit()
    conn.close()
    bi_sua = verify_audit_chain(ban_sao)
    shutil.rmtree(thu_muc)
    print(f"  Bản sao bị sửa dòng #{dong[0]} (DENIED -> ALLOWED): "
          f"{'⛔ phát hiện - ' + bi_sua['ly_do'] if not bi_sua['ok'] else '❌ KHÔNG phát hiện'}")
    return that["ok"] and not bi_sua["ok"] and bi_sua["dong_loi"] == dong[0]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Chạy toàn bộ kịch bản demo governance")
    parser.add_argument("--reset", action="store_true", help="xóa metadata.db cũ trước khi chạy")
    args = parser.parse_args(argv)
    ket_qua = []

    # -----------------------------------------------------------------------
    tieu_de("BƯỚC 0: Khởi tạo metadata.db (không nạp dữ liệu giả)")
    if args.reset and os.path.exists(DEFAULT_DB_PATH):
        os.remove(DEFAULT_DB_PATH)
        print(f"Đã xóa DB cũ: {DEFAULT_DB_PATH}")
    init_database(seed_sample_data=False)
    print(f"DB: {DEFAULT_DB_PATH}")

    tieu_de("BƯỚC 1: Tách 3 bệnh viện đông bệnh nhân nhất (split_hospitals)")
    split_hospitals.main()

    tieu_de("BƯỚC 2: Nạp tầng RAW, chia 3 đợt và đăng ký catalog (ingest_raw)")
    ingest_raw.main()

    # -----------------------------------------------------------------------
    kq_std, ai_df, run_id = chay_pipeline("kịch bản 1: dữ liệu sạch")
    breaking = get_violations(severity="BREAKING", run_id=run_id)
    dat_1 = (not breaking and kq_std["so_dot_dat"] == kq_std["so_dot"] and ai_df is not None
             and trang_thai_run(run_id) == "SUCCESS")
    tieu_de("KỊCH BẢN 1: dữ liệu sạch – không có vi phạm BREAKING"
            + ("" if dat_1 else "  (❌ CHƯA ĐẠT)"))
    in_tom_tat(kq_std, ai_df, run_id, breaking)
    for v in get_violations(severity="WARNING", run_id=run_id):
        print(f"  (cảnh báo, không chặn) {v['error_type']} tại {v['affected_table']} | lô {v['batch']}: "
              f"{v['details']}")
    ket_qua.append(("Kịch bản 1: dữ liệu sạch không có BREAKING", dat_1))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 2: Bệnh viện B đổi tên cột admissionweight -> weight (inject_errors --loi doi_ten)")
    inject_errors.gay_loi("doi_ten")
    audit_dau = id_lon_nhat("audit_log")
    kq_std, ai_df, run_id = chay_pipeline("kịch bản 2: viện B đổi tên cột")
    breaking = get_violations(severity="BREAKING", run_id=run_id)
    chan = audit_moi(audit_dau, "BLOCKED")
    lo_b = [b for b in provenance.get_batches(run_id) if b["batch"].startswith("hospital_B/")
            and b["batch"].endswith("20-00-00")]

    tieu_de("KẾT QUẢ KỊCH BẢN 2: vi phạm và bảng AI-ready bị ảnh hưởng")
    in_tom_tat(kq_std, ai_df, run_id, breaking)
    for v in breaking:
        print(f"\n  ⛔ [{v['severity']}] {v['error_type']} | {v['hospital']} | bảng {v['affected_table']}"
              f" | lô {v['batch']}")
        print(f"     {v['details']}")
        print(f"     Bảng AI-ready bị ảnh hưởng: {v['affected_ai_table'] or '(không có)'} | hành động: {v['action']}")
    print("\n  Dòng audit_log BLOCKED:")
    for a in chan:
        print(f"     ai={a['ai']} | làm gì={a['lam_gi']} | bảng={a['bang_nao']} | {a['ket_qua']}")
    print("\n  Provenance của đợt 3 viện B trong lần chạy này:")
    for b in lo_b:
        print(f"     lô #{b['id']} {b['table_name']:<24} {b['status']:<8} {b['row_count']:>5} dòng "
              f"| sha256 {b['checksum'][:16]}…")
    dat_2 = (any(v["error_type"] == "SCHEMA_COLUMN_RENAMED" and AI in v["affected_ai_table"].split(",")
                 and v["batch"].startswith("hospital_B/") for v in breaking)
             and any(a["bang_nao"] == "raw_patient_hospital_B" for a in chan)
             and {(b["table_name"], b["status"]) for b in lo_b} == {
                 ("raw_patient_hospital_B", "BLOCKED"), ("raw_vital_hospital_B", "SKIPPED"),
                 ("raw_lab_hospital_B", "SKIPPED")}
             and trang_thai_run(run_id) == "PARTIAL")
    ket_qua.append(("Kịch bản 2: đổi tên cột bị chặn, chỉ ra ai_ready_icu_mortality, có audit BLOCKED", dat_2))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 3: Bệnh viện B sửa lại dữ liệu (inject_errors --sua-lai --vien B)")
    inject_errors.sua_lai("B")
    kq_std, ai_df, run_id = chay_pipeline("kịch bản 3: viện B sửa lại dữ liệu")
    breaking = get_violations(severity="BREAKING", run_id=run_id)
    dat_3 = not breaking and kq_std["so_dot_dat"] == kq_std["so_dot"] and ai_df is not None
    tieu_de("KẾT QUẢ KỊCH BẢN 3: dữ liệu viện B qua cổng" + ("" if dat_3 else "  (❌ CHƯA ĐẠT)"))
    in_tom_tat(kq_std, ai_df, run_id, breaking)
    ket_qua.append(("Kịch bản 3: sau khi sửa lại, không còn BREAKING mới", dat_3))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 4: Phân quyền được THỰC THI khi đọc dữ liệu (catalog_access.read_table)")
    ket_qua.append(("Kịch bản 4: nhà nghiên cứu đọc raw bị từ chối, cột định danh bị ẩn với chuyên viên phân tích",
                    kich_ban_phan_quyen()))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 5: Provenance – bảng AI-ready được tạo từ những lô nào, file bị sửa tay thì sao")
    ket_qua.append(("Kịch bản 5: truy ngược AI-ready về lô nguồn, file standardized sửa tay bị chặn",
                    kich_ban_provenance()))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 6: Sổ audit chống sửa (chuỗi băm SHA-256)")
    ket_qua.append(("Kịch bản 6: sổ audit toàn vẹn, sửa một dòng là bị phát hiện", kich_ban_audit()))

    # -----------------------------------------------------------------------
    tieu_de("TỔNG KẾT")
    for mo_ta, dat in ket_qua:
        print(f"  {'✅' if dat else '❌'} {mo_ta}")
    thanh_cong = all(dat for _, dat in ket_qua)
    print("\n" + ("🎉 DEMO THÀNH CÔNG – mở dashboard: streamlit run dashboard/app.py" if thanh_cong
                  else "❌ DEMO CHƯA ĐẠT, xem các dòng ❌ ở trên"))
    return 0 if thanh_cong else 1


if __name__ == "__main__":
    sys.exit(main())
