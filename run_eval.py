"""
Đánh giá hệ thống governance trên dữ liệu thật eICU (số liệu cho phần thực nghiệm của báo cáo).

Chạy trong một thư mục tạm (bản sao tầng raw + metadata.db riêng + khóa tạm), KHÔNG đụng
data/raw, data/standardized, data/ai_ready hay metadata.db thật:

    python run_eval.py                    # in các bảng kết quả
    python run_eval.py --out ket_qua_eval  # ghi thêm các bảng ra file CSV + tong_hop.json

Các phép đo:
  E1. Dữ liệu sạch: số vi phạm BREAKING (kỳ vọng 0 = không chặn oan), số cảnh báo.
  E2. 5 loại lỗi của inject_errors, mỗi loại chạy 2 cách và so với bảng AI-ready sạch:
        - CÓ governance  : phát hiện đúng loại lỗi? đúng lô? chặn lô? chỉ ra bảng AI-ready bị ảnh hưởng?
        - KHÔNG governance (cùng các bước biến đổi, bỏ cổng kiểm tra): lỗi lọt vào AI-ready hay pipeline sập?
      "Dòng nhiễm bẩn" = dòng AI-ready có giá trị khác bản sạch (không tính ô trống).
  E3. Hiệu năng gate1 + gate2 theo số dòng (10 nghìn -> 1 triệu dòng sinh hiệu).
  E4. Phân quyền: ma trận vai trò x tầng x (đọc, ghi) so với kỳ vọng viết sẵn; ẩn cột nhạy cảm.
  E5. Sổ audit: 4 kiểu can thiệp trên bản sao DB (sửa, xóa giữa, chèn dòng giả, xóa dòng cuối).
  E6. Provenance: checksum mọi lô nguồn; sửa tay file standardized; sửa file raw sau khi nạp.
"""

import argparse
import glob
import json
import os
import secrets
import shutil
import sqlite3
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

# Khóa tạm cho riêng lần đánh giá (không đọc / tạo file .secrets/ của repo)
os.environ.setdefault("PATIENT_HASH_SECRET", secrets.token_hex(16))
os.environ.setdefault("AUDIT_LOG_SECRET", secrets.token_hex(16))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from catalog_access import (AccessDenied, evaluate_access, init_database, read_table,  # noqa: E402
                            verify_audit_chain)
from catalog_access.audit import compute_row_hash  # noqa: E402
from governance import provenance  # noqa: E402
from governance.gates import check_gate_detailed  # noqa: E402
from governance.violation_engine import get_violations  # noqa: E402
from pipeline import build_ai_ready, inject_errors, split_hospitals, standardize  # noqa: E402
from pipeline.ingest_raw import HOSPITALS, register_raw_tables  # noqa: E402
from pipeline.split_snapshots import STAGING_DIR, split_into_timestamps  # noqa: E402

AI = "ai_ready_icu_mortality"
COT_SO_SANH = ["age", "gender", "weight_kg", "heartrate_mean", "creatinine_max", "glucose_mean",
               "icu_mortality"]

# Loại lỗi -> (bảng bị lỗi, loại vi phạm phải phát hiện)
KY_VONG_LOI = {
    "doi_ten": ("raw_patient_hospital_B", "SCHEMA_COLUMN_RENAMED"),
    "doi_kieu": ("raw_patient_hospital_A", "SCHEMA_TYPE_CHANGED"),
    "xoa_cot": ("raw_patient_hospital_C", "SCHEMA_COLUMN_REMOVED"),
    "nhip_tim_500": ("raw_vital_hospital_A", "PLAUSIBILITY"),
    "thieu_can_nang": ("raw_patient_hospital_B", "COMPLETENESS"),
}

# Kỳ vọng phân quyền viết TAY (không đọc từ policies.yaml) -> kiểm tra cấu hình có đúng thiết kế không
KY_VONG_QUYEN = {
    # vai trò: {tầng: (được đọc, được ghi)}
    "data_engineer": {"raw": (True, True), "standardized": (True, True), "ai_ready": (True, True)},
    "analyst": {"raw": (False, False), "standardized": (True, False), "ai_ready": (True, False)},
    "researcher": {"raw": (False, False), "standardized": (False, False), "ai_ready": (True, False)},
    "guest": {"raw": (False, False), "standardized": (False, False), "ai_ready": (False, False)},
}
BANG_THEO_TANG = {"raw": "raw_patient_hospital_A", "standardized": "standardized_patients", "ai_ready": AI}


def tieu_de(chu):
    print("\n" + "=" * 78 + f"\n{chu}\n" + "=" * 78, flush=True)


class MoiTruong:
    """Thư mục tạm chứa tầng raw / standardized / ai_ready và metadata.db riêng."""

    def __init__(self, thu_muc):
        self.thu_muc = thu_muc
        self.raw = os.path.join(thu_muc, "raw")
        self.std = os.path.join(thu_muc, "standardized")
        self.ai = os.path.join(thu_muc, "ai_ready")
        self.backup = os.path.join(thu_muc, "backup")
        self.db = os.path.join(thu_muc, "metadata.db")
        init_database(self.db, seed_sample_data=False)
        split_into_timestamps(STAGING_DIR, self.raw)
        register_raw_tables(self.db, self.raw)

    def chay(self, ghi_chu):
        """Một lần chạy pipeline có governance. Trả về (AI-ready df hoặc None, run_id, thời gian giây)."""
        bat_dau = time.perf_counter()
        with provenance.pipeline_run("pipeline_service", ghi_chu, self.db) as run_id:
            standardize.main(self.db, self.raw, self.std)
            ai_df = build_ai_ready.main(self.db, self.std, self.ai)
        return ai_df, run_id, time.perf_counter() - bat_dau


def ai_ready_khong_governance(raw_dir):
    """Cùng các bước biến đổi của pipeline nhưng BỎ mọi cổng kiểm tra (để so sánh)."""
    benh_nhan, xet_nghiem = [], []
    for vien in HOSPITALS:
        for dot in sorted(os.listdir(os.path.join(raw_dir, f"hospital_{vien}"))):
            thu_muc = os.path.join(raw_dir, f"hospital_{vien}", dot)
            nhan = f"hospital_{vien}/{dot}"
            p = standardize.clean_patient(pd.read_csv(os.path.join(thu_muc, "patient.csv")), vien, nhan)
            hr = standardize.calc_heartrate_mean(pd.read_csv(os.path.join(thu_muc, "vitalPeriodic.csv")))
            benh_nhan.append(p.merge(hr, on="patient_id", how="left"))
            xet_nghiem.append(standardize.clean_lab(pd.read_csv(os.path.join(thu_muc, "lab.csv")), vien, nhan))
    return build_ai_ready.build_ai_ready(pd.concat(benh_nhan, ignore_index=True),
                                         pd.concat(xet_nghiem, ignore_index=True))


def so_sanh_voi_ban_sach(sach, df):
    """
    So df với bảng AI-ready sạch (ghép theo patient_hash). Trả về:
      - số dòng nhiễm bẩn : có ít nhất một ô KHÁC bản sạch (ô trống không tính)
      - số dòng mất giá trị: có ít nhất một ô bản sạch có giá trị nhưng df để trống
    """
    ghep = sach.merge(df, on="patient_hash", how="inner", suffixes=("_sach", ""))
    nhiem_ban = pd.Series(False, index=ghep.index)
    mat_gia_tri = pd.Series(False, index=ghep.index)
    for cot in COT_SO_SANH:
        a, b = ghep[f"{cot}_sach"], ghep[cot]
        if cot == "gender":
            khac = a.astype(str) != b.astype(str)
        else:
            so_a, so_b = pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce")
            # giá trị không đổi được sang số (ví dụ "70.5 kg") cũng là khác bản sạch
            khac = ~pd.Series(np.isclose(so_a, so_b, equal_nan=True), index=ghep.index) | (b.notna() & so_b.isna())
        nhiem_ban |= khac & b.notna()
        mat_gia_tri |= a.notna() & b.isna()
    return int(nhiem_ban.sum()), int(mat_gia_tri.sum())


# ---------------------------------------------------------------------------
# E1 + E2
# ---------------------------------------------------------------------------

def danh_gia_loi(mt):
    tieu_de("E1. Dữ liệu sạch")
    sach, run_id, giay = mt.chay("đánh giá: dữ liệu sạch")
    e1 = {
        "so_dong_ai_ready": len(sach),
        "breaking": len(get_violations(mt.db, severity="BREAKING", run_id=run_id)),
        "warning": len(get_violations(mt.db, severity="WARNING", run_id=run_id)),
        "thoi_gian_giay": round(giay, 2),
    }
    print(pd.Series(e1).to_string())

    tieu_de("E2. Năm loại lỗi: có governance vs không governance")
    dong = []
    for loai, (bang_loi, loi_ky_vong) in KY_VONG_LOI.items():
        vien = inject_errors.gay_loi(loai, mt.raw, mt.backup)

        ai_df, run_id, giay = mt.chay(f"đánh giá: lỗi {loai}")
        vp = [v for v in get_violations(mt.db, run_id=run_id) if v["severity"] in ("BREAKING", "CRITICAL")]
        dung = [v for v in vp if v["error_type"] == loi_ky_vong and v["affected_table"] == bang_loi]
        lo_loi = [b for b in provenance.get_batches(run_id, mt.db)
                  if b["table_name"] == bang_loi and b["batch"].endswith("20-00-00")]
        nhiem, mat = so_sanh_voi_ban_sach(sach, ai_df) if ai_df is not None else (None, None)

        try:
            khong_gov = ai_ready_khong_governance(mt.raw)
            nhiem_kg, mat_kg = so_sanh_voi_ban_sach(sach, khong_gov)
            ket_qua_kg = f"lọt {nhiem_kg} dòng nhiễm bẩn, {mat_kg} dòng mất giá trị" if nhiem_kg or mat_kg \
                else "không ảnh hưởng"
        except Exception as loi:
            nhiem_kg, mat_kg = None, None
            ket_qua_kg = f"pipeline sập ({type(loi).__name__}: {loi})"

        dong.append({
            "loi": loai,
            "vien": vien,
            "phat_hien_dung_loai": bool(dung),
            "loai_vi_pham": ",".join(sorted({v["error_type"] for v in vp})),
            "dung_lo": bool(dung) and all(v["batch"].startswith(f"hospital_{vien}/")
                                          and v["batch"].endswith("20-00-00") for v in dung),
            "lo_bi_chan": bool(lo_loi) and lo_loi[0]["status"] == "BLOCKED",
            "chi_ra_bang_ai": bool(dung) and all(AI in v["affected_ai_table"].split(",") for v in dung),
            "so_dong_ai_ready": len(ai_df) if ai_df is not None else 0,
            "nhiem_ban_co_gov": nhiem,
            "mat_gia_tri_co_gov": mat,
            "khong_governance": ket_qua_kg,
            "nhiem_ban_khong_gov": nhiem_kg,
            "thoi_gian_giay": round(giay, 2),
        })
        inject_errors.sua_lai(vien, mt.raw, mt.backup)

    bang = pd.DataFrame(dong)
    print(bang.drop(columns=["khong_governance"]).to_string(index=False))
    print("\nKhông có governance:")
    for d in dong:
        print(f"  - {d['loi']:<15}: {d['khong_governance']}")
    tom_tat = {
        "phat_hien": f"{int(bang['phat_hien_dung_loai'].sum())}/{len(bang)}",
        "chan_dung_lo": f"{int(bang['lo_bi_chan'].sum())}/{len(bang)}",
        "dong_nhiem_ban_co_governance": int(bang["nhiem_ban_co_gov"].fillna(0).sum()),
        "loi_lot_hoac_sap_khi_khong_governance": int(
            sum(1 for d in dong if d["nhiem_ban_khong_gov"] is None or d["nhiem_ban_khong_gov"] > 0
                or "mất" in d["khong_governance"])),
    }
    print("\nTóm tắt:", tom_tat)
    return e1, bang, tom_tat, sach


# ---------------------------------------------------------------------------
# E3
# ---------------------------------------------------------------------------

def danh_gia_hieu_nang(mt, cac_co=(10_000, 100_000, 1_000_000)):
    tieu_de("E3. Hiệu năng gate1 + gate2 (bảng sinh hiệu, chạy thử không ghi DB)")
    goc = pd.concat([pd.read_csv(f) for f in glob.glob(os.path.join(mt.raw, "hospital_C", "*",
                                                                    "vitalPeriodic.csv"))],
                    ignore_index=True)
    dong = []
    for n in cac_co:
        df = goc.sample(n, replace=True, random_state=0).reset_index(drop=True)
        bat_dau = time.perf_counter()
        check_gate_detailed("raw_vital_hospital_C", df, "gate1", mt.db, ghi_nhan=False)
        giua = time.perf_counter()
        check_gate_detailed("raw_vital_hospital_C", df, "gate2", mt.db, ghi_nhan=False)
        het = time.perf_counter()
        dong.append({"so_dong": n, "gate1_giay": round(giua - bat_dau, 3), "gate2_giay": round(het - giua, 3),
                     "dong_moi_giay": int(n / (het - bat_dau))})
    bang = pd.DataFrame(dong)
    print(bang.to_string(index=False))
    return bang


# ---------------------------------------------------------------------------
# E4
# ---------------------------------------------------------------------------

def danh_gia_phan_quyen(mt):
    tieu_de("E4. Phân quyền: ma trận vai trò x tầng so với kỳ vọng")
    dong, dung = [], 0
    for vai_tro, theo_tang in KY_VONG_QUYEN.items():
        for tang, (doc, ghi) in theo_tang.items():
            bang = BANG_THEO_TANG[tang]
            thuc_doc = evaluate_access(vai_tro, bang, "read", db_path=mt.db)["allowed"]
            thuc_ghi = evaluate_access(vai_tro, bang, "write", db_path=mt.db)["allowed"]
            dung += (thuc_doc == doc) + (thuc_ghi == ghi)
            dong.append({"vai_tro": vai_tro, "tang": tang, "doc": thuc_doc, "ghi": thuc_ghi,
                         "dung_ky_vong": thuc_doc == doc and thuc_ghi == ghi})
    bang = pd.DataFrame(dong)
    print(bang.to_string(index=False))

    nguoi_la = evaluate_access("nguoi_khong_co_trong_danh_sach", AI, "read", db_path=mt.db)["allowed"]
    try:
        read_table("researcher_bob", "raw_patient_hospital_A", db_path=mt.db)
        tu_choi_that = False
    except AccessDenied:
        tu_choi_that = True
    cot_analyst = list(read_table("analyst_dave", "standardized_patients", db_path=mt.db).columns)
    cot_ky_su = list(read_table("engineer_alice", "standardized_patients", db_path=mt.db).columns)
    tom_tat = {
        "dung_ky_vong": f"{dung}/{2 * len(bang)}",
        "nguoi_la_bi_tu_choi": not nguoi_la,
        "read_table_nem_AccessDenied": tu_choi_that,
        "an_patient_id_voi_analyst": "patient_id" not in cot_analyst,
        "ky_su_thay_patient_id": "patient_id" in cot_ky_su,
    }
    print("\nTóm tắt:", tom_tat)
    return bang, tom_tat


# ---------------------------------------------------------------------------
# E5
# ---------------------------------------------------------------------------

def _sua(ban_sao, lenh):
    conn = sqlite3.connect(ban_sao)
    lenh(conn)
    conn.commit()
    conn.close()


def _chen_dong_gia(conn):
    """Kẻ gian có quyền ghi DB, biết thuật toán nhưng KHÔNG có khóa audit: tự tính băm bằng khóa đoán."""
    cuoi = conn.execute("SELECT id, row_hash FROM audit_log ORDER BY id DESC LIMIT 1").fetchone()
    ma = cuoi[0] + 1
    gia = ("2026-10-29 03:00:00", "researcher_bob", "read:raw", "raw_patient_hospital_A", "ALLOWED", "")
    row_hash = compute_row_hash(cuoi[1], ma, *gia, khoa=b"khoa-doan-mo-khong-dung")
    conn.execute("INSERT INTO audit_log (id, thoi_gian, ai, lam_gi, bang_nao, ket_qua, run_id, prev_hash, "
                 "row_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (ma, *gia, cuoi[1], row_hash))


def danh_gia_audit(mt):
    tieu_de("E5. Sổ audit chống sửa: 4 kiểu can thiệp trên bản sao DB")
    goc = verify_audit_chain(mt.db)
    print(f"Sổ gốc: {goc['ly_do']} ({goc['so_dong']} dòng)")
    giua = goc["so_dong"] // 2
    cac_kieu = {
        "sửa 1 dòng DENIED thành ALLOWED":
            lambda c: c.execute("UPDATE audit_log SET ket_qua = 'ALLOWED' WHERE id = "
                                "(SELECT MIN(id) FROM audit_log WHERE ket_qua = 'DENIED')"),
        "xóa 1 dòng ở giữa":
            lambda c: c.execute("DELETE FROM audit_log WHERE id = ?", (giua,)),
        "chèn dòng giả cuối sổ (không có khóa)": _chen_dong_gia,
        "xóa 5 dòng cuối sổ":
            lambda c: c.execute("DELETE FROM audit_log WHERE id > (SELECT MAX(id) - 5 FROM audit_log)"),
    }
    dong = []
    with tempfile.TemporaryDirectory() as tam:
        for ten, lenh in cac_kieu.items():
            ban_sao = os.path.join(tam, "ban_sao.db")
            shutil.copy(mt.db, ban_sao)
            _sua(ban_sao, lenh)
            kq = verify_audit_chain(ban_sao)
            dong.append({"kieu_can_thiep": ten, "phat_hien": not kq["ok"], "chi_tiet": kq["ly_do"]})
    bang = pd.DataFrame(dong)
    print(bang.to_string(index=False))
    return bang, {"so_goc_toan_ven": goc["ok"], "phat_hien": f"{int(bang['phat_hien'].sum())}/{len(bang)}"}


# ---------------------------------------------------------------------------
# E6
# ---------------------------------------------------------------------------

def danh_gia_provenance(mt):
    tieu_de("E6. Provenance")
    # Mọi lỗi của E2 đã được sửa lại -> chạy lại một lần để có phiên bản AI-ready sạch, đủ 3 viện
    mt.chay("đánh giá: chạy lại sau khi sửa hết lỗi")
    phien_ban = provenance.get_latest_version(AI, mt.db)
    lo_nguon = provenance.source_batches(phien_ban["id"], mt.db)
    kiem_tra = provenance.verify_version(phien_ban["id"], mt.db)

    # Sửa tay file standardized rồi tạo lại AI-ready
    duong_dan = os.path.join(mt.std, "standardized_patients.csv")
    sao_luu = duong_dan + ".bak"
    shutil.copy(duong_dan, sao_luu)
    df = pd.read_csv(duong_dan)
    df.loc[0, "weight_kg"] = 999.0
    df.to_csv(duong_dan, index=False)
    with provenance.pipeline_run("pipeline_service", "đánh giá: file standardized bị sửa tay", mt.db) as run_id:
        ai_df = build_ai_ready.main(mt.db, mt.std, mt.ai)
    chan_sua_tay = ai_df is None and any(v["error_type"] == "PROVENANCE_UNVERIFIED"
                                         for v in get_violations(mt.db, run_id=run_id))
    shutil.move(sao_luu, duong_dan)

    # Sửa một file raw SAU khi đã nạp -> verify_version phải chỉ ra đúng file đó
    lo = lo_nguon[0]
    file_raw = provenance.from_catalog_path(lo["source_file"])
    with open(file_raw, "a", encoding="utf-8") as f:
        f.write("\n")
    kiem_tra_sau = provenance.verify_version(phien_ban["id"], mt.db)
    chi_ra_file = any(lo["source_file"] in v for v in kiem_tra_sau["van_de"])

    tom_tat = {
        "so_lo_nguon_cua_ai_ready": len(lo_nguon),
        "so_file_kiem_checksum": kiem_tra["da_kiem"],
        "checksum_khop_het": kiem_tra["ok"],
        "chan_file_standardized_sua_tay": chan_sua_tay,
        "phat_hien_file_raw_bi_sua_sau_khi_nap": chi_ra_file,
    }
    print(pd.Series(tom_tat).to_string())
    return tom_tat


def main(argv=None):
    parser = argparse.ArgumentParser(description="Đánh giá hệ thống governance trên dữ liệu thật")
    parser.add_argument("--out", help="thư mục ghi kết quả (CSV + tong_hop.json)")
    parser.add_argument("--max-rows", type=int, default=1_000_000,
                        help="số dòng lớn nhất khi đo hiệu năng (mặc định 1 triệu)")
    args = parser.parse_args(argv)

    if not all(os.path.exists(os.path.join(STAGING_DIR, f"hospital_{v}", "patient.csv")) for v in HOSPITALS):
        tieu_de("Chưa có data/02_staging -> tách 3 bệnh viện từ data/01_landing")
        split_hospitals.main()

    with tempfile.TemporaryDirectory(prefix="danh_gia_governance_") as tam:
        mt = MoiTruong(tam)
        e1, bang_loi, tom_tat_loi, _ = danh_gia_loi(mt)
        cac_co = tuple(n for n in (10_000, 100_000, 1_000_000) if n <= args.max_rows) or (args.max_rows,)
        bang_hieu_nang = danh_gia_hieu_nang(mt, cac_co)
        bang_quyen, tom_tat_quyen = danh_gia_phan_quyen(mt)
        bang_audit, tom_tat_audit = danh_gia_audit(mt)
        tom_tat_prov = danh_gia_provenance(mt)

    tong_hop = {"E1_du_lieu_sach": e1, "E2_loi": tom_tat_loi, "E4_phan_quyen": tom_tat_quyen,
                "E5_audit": tom_tat_audit, "E6_provenance": tom_tat_prov}
    tieu_de("TỔNG HỢP")
    print(json.dumps(tong_hop, ensure_ascii=False, indent=2))

    if args.out:
        os.makedirs(args.out, exist_ok=True)
        bang_loi.to_csv(os.path.join(args.out, "e2_loi.csv"), index=False)
        bang_hieu_nang.to_csv(os.path.join(args.out, "e3_hieu_nang.csv"), index=False)
        bang_quyen.to_csv(os.path.join(args.out, "e4_phan_quyen.csv"), index=False)
        bang_audit.to_csv(os.path.join(args.out, "e5_audit.csv"), index=False)
        with open(os.path.join(args.out, "tong_hop.json"), "w", encoding="utf-8") as f:
            json.dump(tong_hop, f, ensure_ascii=False, indent=2)
        print(f"\nĐã ghi kết quả vào {os.path.abspath(args.out)}")

    so_o_quyen = 2 * len(bang_quyen)
    dat = (e1["breaking"] == 0
           and tom_tat_loi["phat_hien"] == f"{len(KY_VONG_LOI)}/{len(KY_VONG_LOI)}"
           and tom_tat_loi["dong_nhiem_ban_co_governance"] == 0
           and tom_tat_quyen["dung_ky_vong"] == f"{so_o_quyen}/{so_o_quyen}"
           and all(v for k, v in tom_tat_quyen.items() if k != "dung_ky_vong")
           and tom_tat_audit["phat_hien"] == f"{len(bang_audit)}/{len(bang_audit)}"
           and all(tom_tat_prov[k] for k in ("checksum_khop_het", "chan_file_standardized_sua_tay",
                                            "phat_hien_file_raw_bi_sua_sau_khi_nap")))
    print("\n" + ("✅ ĐÁNH GIÁ ĐẠT mọi kỳ vọng" if dat else "❌ CÓ KỲ VỌNG CHƯA ĐẠT, xem các bảng ở trên"))
    return 0 if dat else 1


if __name__ == "__main__":
    sys.exit(main())
