"""
Demo phần Governance (chạy độc lập với pipeline của Người 1).

Kịch bản (tài liệu phân công mục 8):
  1. Dữ liệu sạch của 3 bệnh viện đi qua gate1 + gate2 ở cả 3 tầng -> không có lỗi BREAKING.
  2. Bệnh viện B đổi tên cột admissionweight -> weight.
  3. Hệ thống phát hiện, chặn, ghi violations (BREAKING), chỉ ra ai_ready_icu_mortality bị ảnh hưởng.
  4. audit_log có dòng BLOCKED.

Dữ liệu: dùng data/02_staging/hospital_A|B|C nếu có, không thì dùng dữ liệu giả (sample_data).

Cách chạy (từ gốc repo):
    python -m governance.demo_governance                 # ghi vào metadata.db (dashboard đọc được)
    python -m governance.demo_governance --reset         # xóa metadata.db cũ trước khi chạy
    python -m governance.demo_governance --db /tmp/x.db  # dùng DB khác
    python -m governance.demo_governance --fake          # bắt buộc dùng dữ liệu giả
"""

import argparse
import os
import sys

import pandas as pd

from catalog_access import register_dataset
from catalog_access.init_db import DEFAULT_DB_PATH, get_db_connection, init_database
from governance.contracts import find_contract, get_sensitive_columns
from governance.gates import check_gate
from governance.lineage import record_sample_lineage
from governance.sample_data import (
    build_ai_ready,
    make_hospital,
    standardize_labs,
    standardize_patients,
)
from governance.violation_engine import get_violations, summarize

STAGING_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "02_staging"))
AI = "ai_ready_icu_mortality"


def tieu_de(chu: str) -> None:
    print("\n" + "=" * 78 + f"\n{chu}\n" + "=" * 78)


# ---------------------------------------------------------------------------
# Chuẩn bị
# ---------------------------------------------------------------------------

def co_du_lieu_that() -> bool:
    return all(os.path.exists(os.path.join(STAGING_DIR, f"hospital_{v}", "patient.csv")) for v in "ABC")


def doc_du_lieu(dung_du_lieu_gia: bool) -> tuple[dict, str]:
    """Trả về ({'A': {'patient', 'lab', 'vital'}, ...}, mô tả nguồn dữ liệu)."""
    if not dung_du_lieu_gia and co_du_lieu_that():
        raw = {}
        for vien in "ABC":
            thu_muc = os.path.join(STAGING_DIR, f"hospital_{vien}")
            raw[vien] = {
                "patient": pd.read_csv(os.path.join(thu_muc, "patient.csv")),
                "lab": pd.read_csv(os.path.join(thu_muc, "lab.csv")),
                "vital": pd.read_csv(os.path.join(thu_muc, "vitalPeriodic.csv")),
            }
        return raw, f"dữ liệu THẬT eICU ({STAGING_DIR})"
    return {v: make_hospital(v) for v in "ABC"}, "dữ liệu GIẢ mô phỏng eICU (governance/sample_data.py)"


def dang_ky_catalog(db: str) -> None:
    """Đăng ký 12 bảng vào catalog (hàm register_dataset của Người 3), cột nhạy cảm lấy từ hợp đồng."""
    def _nhay_cam(ten):
        hop_dong = find_contract(ten)
        return ",".join(get_sensitive_columns(hop_dong)) if hop_dong else ""

    for vien in "ABC":
        for loai in ("patient", "lab", "vital"):
            ten = f"raw_{loai}_hospital_{vien}"
            register_dataset(ten, f"Bệnh viện {vien}", "raw", f"Đội IT Bệnh viện {vien}",
                             _nhay_cam(ten), registered_by="governance_demo", db_path=db)
    for ten in ("standardized_patients", "standardized_labs"):
        register_dataset(ten, "Toàn hệ thống", "standardized", "Nhóm Kỹ thuật dữ liệu",
                         _nhay_cam(ten), registered_by="governance_demo", db_path=db)
    register_dataset(AI, "Toàn hệ thống", "ai_ready", "Nhóm nghiên cứu AI",
                     _nhay_cam(AI), registered_by="governance_demo", db_path=db)


def id_lon_nhat(db: str, bang: str) -> int:
    conn = get_db_connection(db)
    gia_tri = conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {bang}").fetchone()[0]
    conn.close()
    return gia_tri


def qua_cong(ten_bang: str, df: pd.DataFrame, db: str) -> bool:
    """Chạy gate1 rồi gate2 (gate1 trượt thì không chạy gate2), in một dòng kết quả."""
    dat1, loi1 = check_gate(ten_bang, df, "gate1", db)
    dat2, loi2 = check_gate(ten_bang, df, "gate2", db) if dat1 else (False, ["(bỏ qua vì gate1 trượt)"])
    ky_hieu = lambda dat: "ĐẠT " if dat else "CHẶN"   # noqa: E731
    print(f"  {ten_bang:<26} {len(df):>6} dòng | gate1: {ky_hieu(dat1)} | gate2: {ky_hieu(dat2)}"
          + (f" | {len(loi1) + len(loi2)} cảnh báo/lỗi" if loi1 or loi2 else ""))
    return dat1 and dat2


# ---------------------------------------------------------------------------
# Kịch bản
# ---------------------------------------------------------------------------

def kich_ban_1_du_lieu_sach(raw: dict, db: str) -> bool:
    tieu_de("KỊCH BẢN 1: Dữ liệu sạch của 3 bệnh viện đi qua 3 tầng")
    tat_ca_dat = True
    print("Tầng RAW:")
    for vien, bang in raw.items():
        for loai in ("patient", "lab", "vital"):
            tat_ca_dat &= qua_cong(f"raw_{loai}_hospital_{vien}", bang[loai], db)

    print("Tầng STANDARDIZED:")
    std_patients = standardize_patients(raw)
    tat_ca_dat &= qua_cong("standardized_patients", std_patients, db)
    tat_ca_dat &= qua_cong("standardized_labs", standardize_labs(raw), db)

    print("Tầng AI-READY:")
    tat_ca_dat &= qua_cong(AI, build_ai_ready(std_patients), db)
    return tat_ca_dat


def kich_ban_2_doi_ten_cot(raw: dict, db: str) -> bool:
    tieu_de("KỊCH BẢN 2: Bệnh viện B đổi tên cột admissionweight -> weight")
    loi_df = raw["B"]["patient"].rename(columns={"admissionweight": "weight"})
    dat, loi = check_gate("raw_patient_hospital_B", loi_df, "gate1", db)
    print(f"\nKết quả check_gate('raw_patient_hospital_B', ..., 'gate1'): đạt = {dat}")
    for dong in loi:
        print("  -", dong)
    return dat


def kiem_tra_ket_qua(db: str, vp_dau: int, audit_dau: int, sach_dat: bool, loi_dat: bool) -> bool:
    tieu_de("KIỂM TRA KẾT QUẢ")
    vi_pham = get_violations(db, since_id=vp_dau)
    conn = get_db_connection(db)
    audit_chan = conn.execute(
        "SELECT ai, lam_gi, bang_nao, ket_qua FROM audit_log WHERE id > ? AND ket_qua = 'BLOCKED' "
        "AND bang_nao = 'raw_patient_hospital_B'", (audit_dau,)
    ).fetchall()
    conn.close()

    vp_doi_ten = [v for v in vi_pham if v["error_type"] == "SCHEMA_COLUMN_RENAMED"
                  and v["severity"] == "BREAKING" and AI in v["affected_ai_table"].split(",")]
    breaking_ngoai_kich_ban_2 = [v for v in vi_pham if v["severity"] == "BREAKING" and v not in vp_doi_ten]

    cac_dieu_kien = [
        ("Kịch bản 1: dữ liệu sạch đạt ở mọi cổng", sach_dat),
        ("Kịch bản 1: không có vi phạm BREAKING nào", not breaking_ngoai_kich_ban_2),
        ("Kịch bản 2: dữ liệu viện B bị chặn", not loi_dat),
        ("violations có dòng BREAKING SCHEMA_COLUMN_RENAMED, ảnh hưởng ai_ready_icu_mortality",
         bool(vp_doi_ten)),
        ("audit_log có dòng BLOCKED cho raw_patient_hospital_B", bool(audit_chan)),
    ]
    for mo_ta, dat in cac_dieu_kien:
        print(f"  {'✅' if dat else '❌'} {mo_ta}")

    if vp_doi_ten:
        v = vp_doi_ten[0]
        print(f"\n  Dòng violations: #{v['id']} | {v['severity']} | {v['hospital']} | {v['affected_table']}"
              f" -> {v['affected_ai_table']} | {v['action']}")
    for a in audit_chan:
        print(f"  Dòng audit_log : ai={a['ai']} | làm gì={a['lam_gi']} | bảng={a['bang_nao']} | {a['ket_qua']}")

    print("\nThống kê vi phạm của lần chạy này:")
    summarize(db, since_id=vp_dau)
    return all(dat for _, dat in cac_dieu_kien)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Demo governance: gate -> violation -> lineage -> audit")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="đường dẫn metadata.db (mặc định ở gốc repo)")
    parser.add_argument("--reset", action="store_true", help="xóa file DB cũ trước khi chạy")
    parser.add_argument("--fake", action="store_true", help="bắt buộc dùng dữ liệu giả")
    args = parser.parse_args(argv)

    if args.reset and os.path.exists(args.db):
        os.remove(args.db)
        print(f"Đã xóa DB cũ: {args.db}")
    init_database(args.db, seed_sample_data=False)

    raw, nguon = doc_du_lieu(args.fake)
    print(f"Nguồn dữ liệu: {nguon}")
    print(f"Metadata DB  : {args.db}")

    vp_dau, audit_dau = id_lon_nhat(args.db, "violations"), id_lon_nhat(args.db, "audit_log")
    dang_ky_catalog(args.db)
    record_sample_lineage(args.db)

    sach_dat = kich_ban_1_du_lieu_sach(raw, args.db)
    loi_dat = kich_ban_2_doi_ten_cot(raw, args.db)
    thanh_cong = kiem_tra_ket_qua(args.db, vp_dau, audit_dau, sach_dat, loi_dat)

    print("\n" + ("🎉 DEMO THÀNH CÔNG" if thanh_cong else "❌ DEMO CHƯA ĐẠT, xem các dòng ❌ ở trên"))
    return 0 if thanh_cong else 1


if __name__ == "__main__":
    sys.exit(main())
