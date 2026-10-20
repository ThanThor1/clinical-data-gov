"""
Chạy toàn bộ kịch bản demo bằng một lệnh (từ gốc repo):

    python run_demo.py            # dùng metadata.db hiện có (chỉ xét các dòng mới sinh ra)
    python run_demo.py --reset    # xóa metadata.db cũ, chạy lại từ đầu

Kịch bản:
  1. Dữ liệu sạch của 3 bệnh viện đi qua 3 tầng -> không có vi phạm BREAKING.
  2. Bệnh viện B (đợt 3) đổi tên cột admissionweight -> weight -> bị chặn,
     in vi phạm và bảng AI-ready bị ảnh hưởng, audit_log có dòng BLOCKED.
  3. Bệnh viện B sửa lại dữ liệu -> chạy lại -> qua cổng.
"""

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from catalog_access.init_db import DEFAULT_DB_PATH, get_db_connection, init_database  # noqa: E402
from governance.violation_engine import get_violations  # noqa: E402
from pipeline import build_ai_ready, ingest_raw, inject_errors, split_hospitals, standardize  # noqa: E402

AI = "ai_ready_icu_mortality"


def tieu_de(chu):
    print("\n" + "=" * 78 + f"\n{chu}\n" + "=" * 78, flush=True)


def id_lon_nhat(bang):
    """id lớn nhất hiện có trong bảng (violations / audit_log) để chỉ xét các dòng mới."""
    conn = get_db_connection()
    gia_tri = conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {bang}").fetchone()[0]
    conn.close()
    return gia_tri


def audit_bi_chan(tu_id):
    """Các dòng audit_log có kết quả BLOCKED sinh ra sau id `tu_id`."""
    conn = get_db_connection()
    dong = conn.execute(
        "SELECT ai, lam_gi, bang_nao, ket_qua FROM audit_log WHERE id > ? AND ket_qua = 'BLOCKED'",
        (tu_id,),
    ).fetchall()
    conn.close()
    return [dict(d) for d in dong]


def chay_pipeline():
    """standardize -> build_ai_ready. Trả về (kết quả standardize, DataFrame AI-ready hoặc None)."""
    tieu_de("Chuẩn hóa: raw -> standardized (standardize)")
    kq_std = standardize.main()
    tieu_de("Tạo tầng AI-ready (build_ai_ready)")
    ai_df = build_ai_ready.main()
    return kq_std, ai_df


def in_tom_tat(kq_std, ai_df, vi_pham_breaking):
    print(f"  Số đợt đi qua đủ cổng : {kq_std['so_dot_dat']}/{kq_std['so_dot']}")
    if kq_std["bi_chan"]:
        print(f"  Bị chặn               : {', '.join(kq_std['bi_chan'])}")
    print(f"  {AI}: {len(ai_df) if ai_df is not None else 'KHÔNG tạo được'} dòng")
    print(f"  Vi phạm BREAKING mới  : {len(vi_pham_breaking)}")


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
    vp_dau, audit_dau = id_lon_nhat("violations"), id_lon_nhat("audit_log")
    kq_std, ai_df = chay_pipeline()
    breaking = get_violations(severity="BREAKING", since_id=vp_dau)
    dat_1 = not breaking and kq_std["so_dot_dat"] == kq_std["so_dot"] and ai_df is not None
    tieu_de("KỊCH BẢN 1: dữ liệu sạch – không có vi phạm BREAKING"
            + ("" if dat_1 else "  (❌ CHƯA ĐẠT)"))
    in_tom_tat(kq_std, ai_df, breaking)
    canh_bao = get_violations(severity="WARNING", since_id=vp_dau)
    for v in canh_bao:
        print(f"  (cảnh báo, không chặn) {v['error_type']} tại {v['affected_table']}: {v['details']}")
    ket_qua.append(("Kịch bản 1: dữ liệu sạch không có BREAKING", dat_1))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 2: Bệnh viện B đổi tên cột admissionweight -> weight (inject_errors --loi doi_ten)")
    inject_errors.gay_loi("doi_ten")
    vp_dau, audit_dau = id_lon_nhat("violations"), id_lon_nhat("audit_log")
    kq_std, ai_df = chay_pipeline()
    breaking = get_violations(severity="BREAKING", since_id=vp_dau)
    chan = audit_bi_chan(audit_dau)

    tieu_de("KẾT QUẢ KỊCH BẢN 2: vi phạm và bảng AI-ready bị ảnh hưởng")
    in_tom_tat(kq_std, ai_df, breaking)
    for v in breaking:
        print(f"\n  ⛔ [{v['severity']}] {v['error_type']} | {v['hospital']} | bảng {v['affected_table']}")
        print(f"     {v['details']}")
        print(f"     Bảng AI-ready bị ảnh hưởng: {v['affected_ai_table'] or '(không có)'} | hành động: {v['action']}")
    print("\n  Dòng audit_log BLOCKED:")
    for a in chan:
        print(f"     ai={a['ai']} | làm gì={a['lam_gi']} | bảng={a['bang_nao']} | {a['ket_qua']}")
    dat_2 = (any(v["error_type"] == "SCHEMA_COLUMN_RENAMED" and AI in v["affected_ai_table"].split(",")
                 for v in breaking)
             and any(a["bang_nao"] == "raw_patient_hospital_B" for a in chan))
    ket_qua.append(("Kịch bản 2: đổi tên cột bị chặn, chỉ ra ai_ready_icu_mortality, có audit BLOCKED", dat_2))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 3: Bệnh viện B sửa lại dữ liệu (inject_errors --sua-lai --vien B)")
    inject_errors.sua_lai("B")
    vp_dau = id_lon_nhat("violations")
    kq_std, ai_df = chay_pipeline()
    breaking = get_violations(severity="BREAKING", since_id=vp_dau)
    dat_3 = not breaking and kq_std["so_dot_dat"] == kq_std["so_dot"] and ai_df is not None
    tieu_de("KẾT QUẢ KỊCH BẢN 3: dữ liệu viện B qua cổng" + ("" if dat_3 else "  (❌ CHƯA ĐẠT)"))
    in_tom_tat(kq_std, ai_df, breaking)
    ket_qua.append(("Kịch bản 3: sau khi sửa lại, không còn BREAKING mới", dat_3))

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
