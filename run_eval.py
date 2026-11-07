"""
Đánh giá hệ thống governance trên dữ liệu thật eICU demo (186 bệnh viện, khoảng 2 triệu dòng) - số liệu
cho phần thực nghiệm của báo cáo. Pipeline chạy bằng Spark (SPARK_MASTER, mặc định local[*]).

Chạy trong một thư mục tạm (bản sao tầng raw + metadata.db riêng + khóa tạm), KHÔNG đụng
data/raw, data/standardized, data/ai_ready hay metadata.db thật:

    python run_eval.py                    # đầy đủ (khoảng 10-15 phút)
    python run_eval.py --nhanh            # bỏ phép đo theo số core / số viện (khoảng 4 phút)
    python run_eval.py --out ket_qua_eval  # ghi thêm các bảng ra file CSV + tong_hop.json

Các phép đo:
  E1. Dữ liệu thật: bao nhiêu lô qua cổng / bị chặn, vì lỗi gì; số giá trị được làm sạch; thời gian.
  E2. 5 loại lỗi của inject_errors, mỗi loại chạy 2 cách:
        - CÓ governance  : phát hiện đúng loại lỗi? đúng lô? chặn lô? chỉ ra bảng AI-ready bị ảnh hưởng?
        - KHÔNG governance: cùng các bước biến đổi nhưng đọc một lần mọi file CSV bằng Spark (khớp cột theo
          vị trí), không cổng, không làm sạch -> lỗi lọt vào AI-ready hay pipeline sập?
      Mỗi cách được so với CHÍNH NÓ khi dữ liệu sạch: "dòng nhiễm bẩn" = dòng AI-ready có giá trị khác
      lần chạy sạch (không tính ô trống), "dòng mất giá trị" = ô có giá trị lúc sạch nhưng trống khi có lỗi.
  E3. Hiệu năng: (a) lập hồ sơ 1.674 lô bằng pandas (từng file) so với Spark (mỗi loại một job);
      (b) thời gian pipeline theo số core; (c) theo số bệnh viện (lượng dữ liệu); (d) chạy lại tăng dần.
  E4. Phân quyền: ma trận vai trò x tầng x (đọc, ghi) so với kỳ vọng viết sẵn; ẩn cột nhạy cảm.
  E5. Sổ audit: 4 kiểu can thiệp trên bản sao DB (sửa, xóa giữa, chèn dòng giả, xóa dòng cuối).
  E6. Provenance: checksum mọi lô nguồn; bảng standardized bị ghi ngoài pipeline; kiểm tra lại phiên bản
      cũ bằng Delta time travel; file raw bị sửa sau khi nạp.
  E7. Lỗi do PIPELINE đổi code (inject_errors.LOI_PIPELINE: dữ liệu đúng hợp đồng nhưng sai nghĩa), chạy
      có / không có bộ phát hiện lệch đầu ra: phát hiện? hợp đồng có bắt được không? bao nhiêu dòng AI-ready
      bị nhiễm bẩn? sửa code thì tầng AI-ready có trở về như lúc sạch không?
E1 có thêm số viện theo hệ thống nguồn (sources/*.yaml) và cảnh báo nguồn định nghĩa cột khác hợp đồng.
"""

import argparse
import contextlib
import io
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
from catalog_access.init_db import backup_database, get_db_connection  # noqa: E402
from governance import pipeline_drift, provenance  # noqa: E402
from governance.contracts import find_contract  # noqa: E402
from governance.profiling import profile_pandas  # noqa: E402
from governance.sources import load_sources, source_of_hospital  # noqa: E402
from governance.violation_engine import get_violations  # noqa: E402
from pipeline import build_ai_ready, inject_errors, source_adapters, split_hospitals, standardize  # noqa: E402
from pipeline.ingest_raw import FILE_THEO_LOAI, register_raw_tables  # noqa: E402
from pipeline.spark_session import get_spark, stop_spark  # noqa: E402
from pipeline.split_snapshots import STAGING_DIR, danh_sach_vien, split_into_timestamps  # noqa: E402

AI = "ai_ready_icu_mortality"
COT_SO_SANH = ["age", "gender", "weight_kg", "heartrate_mean", "creatinine_max", "glucose_mean",
               "icu_mortality"]

# Loại lỗi -> (loại bảng bị lỗi, loại vi phạm phải phát hiện)
KY_VONG_LOI = {
    "doi_ten": ("patient", "SCHEMA_COLUMN_RENAMED"),
    "doi_kieu": ("patient", "SCHEMA_TYPE_CHANGED"),
    "xoa_cot": ("patient", "SCHEMA_COLUMN_REMOVED"),
    "nhip_tim_500": ("vital", "PLAUSIBILITY"),
    "thieu_can_nang": ("patient", "COMPLETENESS"),
}

# Kỳ vọng phân quyền viết TAY (không đọc từ policies.yaml) -> kiểm tra cấu hình có đúng thiết kế không
KY_VONG_QUYEN = {
    # vai trò: {tầng: (được đọc, được ghi)}
    "data_engineer": {"raw": (True, True), "standardized": (True, True), "ai_ready": (True, True)},
    "analyst": {"raw": (False, False), "standardized": (True, False), "ai_ready": (True, False)},
    "researcher": {"raw": (False, False), "standardized": (False, False), "ai_ready": (True, False)},
    "guest": {"raw": (False, False), "standardized": (False, False), "ai_ready": (False, False)},
}
# Tài khoản đại diện cho từng vai trò (quyền được tra theo tài khoản, không theo tên vai trò)
TAI_KHOAN_THEO_VAI_TRO = {"data_engineer": "engineer_alice", "analyst": "analyst_dave",
                          "researcher": "researcher_bob", "guest": "guest_charlie"}


def tieu_de(chu):
    print("\n" + "=" * 78 + f"\n{chu}\n" + "=" * 78, flush=True)


@contextlib.contextmanager
def im_lang():
    """Ẩn bớt chữ in ra của pipeline khi đo (kết quả đã có trong DB)."""
    with contextlib.redirect_stdout(io.StringIO()):
        yield


def _dem_dong(duong_dan):
    with open(duong_dan, "rb") as f:
        return max(sum(1 for _ in f) - 1, 0)


def xep_hang_staging():
    """Mã các viện trong data/02_staging, đông bệnh nhân trước."""
    so_bn = {v: _dem_dong(os.path.join(STAGING_DIR, f"hospital_{v}", "patient.csv")) for v in danh_sach_vien(STAGING_DIR)}
    return sorted(so_bn, key=lambda v: (-so_bn[v], int(v) if v.isdigit() else 0, v))


class MoiTruong:
    """Thư mục tạm chứa tầng raw / standardized / ai_ready và metadata.db riêng."""

    def __init__(self, thu_muc, cac_vien=None):
        self.thu_muc = thu_muc
        self.raw = os.path.join(thu_muc, "raw")
        self.std = os.path.join(thu_muc, "standardized")
        self.ai = os.path.join(thu_muc, "ai_ready")
        self.backup = os.path.join(thu_muc, "backup")
        self.db = os.path.join(thu_muc, "metadata.db")
        init_database(self.db, seed_sample_data=False)
        staging = STAGING_DIR
        if cac_vien is not None:
            # Chỉ một số viện (đo theo lượng dữ liệu): chép phần staging của các viện đó
            staging = os.path.join(thu_muc, "staging")
            for vien in cac_vien:
                shutil.copytree(os.path.join(STAGING_DIR, f"hospital_{vien}"), os.path.join(staging, f"hospital_{vien}"))
        with im_lang():
            split_into_timestamps(staging, self.raw)
            register_raw_tables(self.db, self.raw)

    def chay(self, spark, ghi_chu):
        """Một lần chạy pipeline có governance. Trả về (kq standardize, AI-ready df hoặc None, run_id, giây)."""
        bat_dau = time.perf_counter()
        with im_lang(), provenance.pipeline_run("pipeline_service", ghi_chu, self.db) as run_id:
            kq = standardize.main(self.db, self.raw, self.std, spark)
            ai_df = build_ai_ready.main(self.db, self.std, self.ai, spark)
        return kq, ai_df, run_id, time.perf_counter() - bat_dau

    def xoa_ho_so_da_luu(self):
        """Bỏ hồ sơ đã lưu để lần chạy sau phải tính lại toàn bộ (đo chi phí lần chạy đầu)."""
        conn = get_db_connection(self.db)
        conn.execute("DELETE FROM batch_profiles")
        conn.commit()
        conn.close()


def ai_ready_khong_governance(spark, raw_dir):
    """
    Cùng các phép biến đổi nhưng không có governance: đọc một lần MỌI file CSV của từng loại bằng Spark
    (tiêu đề của một file, khớp cột theo vị trí), không cổng kiểm tra, không làm sạch giá trị.
    Chỉ đọc các viện eICU (cùng schema): đọc lẫn cả nguồn khác schema (Challenge 2012) thì mọi dòng lệch cột ngay
    cả khi dữ liệu sạch, không còn so được ảnh hưởng của từng lỗi giả lập.
    """
    from pyspark.sql import functions as F

    cac_vien = [v for v in danh_sach_vien(raw_dir) if source_of_hospital(v)["hospitals"] == "default"]

    def _doc(loai):
        cac_mau = [os.path.join(raw_dir, f"hospital_{v}", "*", FILE_THEO_LOAI[loai]) for v in cac_vien]
        return spark.read.option("header", True).csv(cac_mau).withColumn("_file", F.col("_metadata.file_path"))

    khong_lam_sach = {"columns": {"admissionweight": {"min": None, "max": None},
                                  "heartrate": {"min": None, "max": None}}}
    benh_nhan = standardize.chuan_hoa_benh_nhan(_doc("patient"), khong_lam_sach)
    benh_nhan = benh_nhan.join(standardize.nhip_tim_trung_binh(_doc("vital"), khong_lam_sach), "patient_id", "left")
    ai = build_ai_ready.build_ai_ready(benh_nhan, standardize.chuan_hoa_xet_nghiem(_doc("lab")))
    return pd.DataFrame([r.asDict() for r in ai.collect()], columns=build_ai_ready.COT_AI)


def so_sanh(goc, df):
    """
    So df với bảng AI-ready lúc dữ liệu sạch (ghép theo patient_hash). Trả về:
      - số dòng nhiễm bẩn : có ít nhất một ô KHÁC lúc sạch (ô trống không tính)
      - số dòng mất giá trị: có ít nhất một ô lúc sạch có giá trị nhưng giờ để trống
    """
    ghep = goc.merge(df, on="patient_hash", how="inner", suffixes=("_goc", ""))
    nhiem_ban = pd.Series(False, index=ghep.index)
    mat_gia_tri = pd.Series(False, index=ghep.index)
    for cot in COT_SO_SANH:
        a, b = ghep[f"{cot}_goc"], ghep[cot]
        if cot == "gender":
            khac = a.astype(str) != b.astype(str)
        else:
            so_a, so_b = pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce")
            khac = ~pd.Series(np.isclose(so_a, so_b, equal_nan=True), index=ghep.index) | (b.notna() & so_b.isna())
        nhiem_ban |= khac & b.notna()
        mat_gia_tri |= a.notna() & b.isna()
    return int(nhiem_ban.sum()), int(mat_gia_tri.sum())


# ---------------------------------------------------------------------------
# E1 + E2
# ---------------------------------------------------------------------------

def danh_gia_du_lieu_that(spark, mt):
    tieu_de("E1. Dữ liệu thật của mọi bệnh viện")
    kq, sach, run_id, giay = mt.chay(spark, "đánh giá: dữ liệu thật")
    breaking = get_violations(mt.db, severity="BREAKING", run_id=run_id)
    e1 = {
        "so_vien": len(danh_sach_vien(mt.raw)),
        "so_vien_theo_nguon": pd.Series([source_of_hospital(v)["name"] for v in danh_sach_vien(mt.raw)],
                                        dtype=object).value_counts().to_dict(),
        "so_dot_gui": kq["so_dot"],
        "lo_theo_ket_qua": kq["trang_thai_lo"],
        "breaking_theo_loai": pd.Series([v["error_type"] for v in breaking], dtype=object).value_counts().to_dict(),
        "lo_bi_chan": sorted(kq["bi_chan"]),
        "warning": len(get_violations(mt.db, severity="WARNING", run_id=run_id)),
        "canh_bao_ngu_nghia": [v["affected_table"] for v in get_violations(mt.db, run_id=run_id)
                               if v["error_type"] == "SEMANTIC_MISMATCH"],
        "lam_sach": kq["lam_sach"],
        "so_dong_ai_ready": len(sach),
        "thoi_gian_giay": round(giay, 1),
    }
    for khoa, gia_tri in e1.items():
        print(f"  {khoa:<18}: {gia_tri}")
    return e1, sach


def danh_gia_loi(spark, mt, sach):
    tieu_de("E2. Năm loại lỗi: có governance vs không governance")
    sach_khong_gov = ai_ready_khong_governance(spark, mt.raw)
    dong = []
    for loai_loi, (loai_bang, loi_ky_vong) in KY_VONG_LOI.items():
        with im_lang():
            vien = inject_errors.gay_loi(loai_loi, mt.raw, mt.backup)
        bang_loi = f"raw_{loai_bang}_hospital_{vien}"

        kq, ai_df, run_id, giay = mt.chay(spark, f"đánh giá: lỗi {loai_loi}")
        vp = [v for v in get_violations(mt.db, run_id=run_id) if v["severity"] in ("BREAKING", "CRITICAL")]
        dung = [v for v in vp if v["error_type"] == loi_ky_vong and v["affected_table"] == bang_loi]
        lo_loi = [b for b in provenance.get_batches(run_id, mt.db)
                  if b["table_name"] == bang_loi and b["batch"].endswith("20-00-00")]
        nhiem, mat = so_sanh(sach, ai_df) if ai_df is not None else (None, None)

        try:
            khong_gov = ai_ready_khong_governance(spark, mt.raw)
            nhiem_kg, mat_kg = so_sanh(sach_khong_gov, khong_gov)
            ket_qua_kg = (f"lọt {nhiem_kg} dòng nhiễm bẩn, {mat_kg} dòng mất giá trị" if nhiem_kg or mat_kg
                          else "không ảnh hưởng")
        except Exception as loi:
            nhiem_kg, mat_kg = None, None
            ket_qua_kg = f"pipeline sập ({type(loi).__name__}: {str(loi).splitlines()[0][:80]})"

        dong.append({
            "loi": loai_loi,
            "vien": vien,
            "phat_hien_dung_loai": bool(dung),
            "dung_lo": bool(dung) and all(v["batch"].startswith(f"hospital_{vien}/")
                                          and v["batch"].endswith("20-00-00") for v in dung),
            "lo_bi_chan": bool(lo_loi) and lo_loi[0]["status"] == "BLOCKED",
            "chi_ra_bang_ai": bool(dung) and all(AI in v["affected_ai_table"].split(",") for v in dung),
            "so_dong_ai_ready": len(ai_df) if ai_df is not None else 0,
            "nhiem_ban_co_gov": nhiem,
            "mat_gia_tri_co_gov": mat,
            "khong_governance": ket_qua_kg,
            "nhiem_ban_khong_gov": nhiem_kg,
            "mat_gia_tri_khong_gov": mat_kg,
            "thoi_gian_giay": round(giay, 1),
        })
        with im_lang():
            inject_errors.sua_lai(vien, mt.raw, mt.backup)

    bang = pd.DataFrame(dong)
    print(bang.drop(columns=["khong_governance"]).to_string(index=False))
    print("\nKhông có governance (đọc CSV theo vị trí, không cổng, không làm sạch):")
    for d in dong:
        print(f"  - {d['loi']:<15}: {d['khong_governance']}")
    tom_tat = {
        "phat_hien": f"{int(bang['phat_hien_dung_loai'].sum())}/{len(bang)}",
        "chan_dung_lo": f"{int(bang['lo_bi_chan'].sum())}/{len(bang)}",
        "dong_nhiem_ban_co_governance": int(bang["nhiem_ban_co_gov"].fillna(0).sum()),
        "loi_anh_huong_khi_khong_governance": int(sum(
            1 for d in dong if d["nhiem_ban_khong_gov"] is None or d["nhiem_ban_khong_gov"] or d["mat_gia_tri_khong_gov"])),
    }
    print("\nTóm tắt:", tom_tat)
    return bang, tom_tat


# ---------------------------------------------------------------------------
# E7
# ---------------------------------------------------------------------------

def _ai_tren_dia(mt):
    from deltalake import DeltaTable
    return DeltaTable(os.path.join(mt.ai, AI)).to_pandas()


def danh_gia_loi_pipeline(spark, mt, sach):
    tieu_de("E7. Lỗi do pipeline đổi code: có / không có bộ phát hiện lệch đầu ra")
    dong = []
    for loi, (bang, mo_ta) in inject_errors.LOI_PIPELINE.items():
        with inject_errors.gay_loi_pipeline(loi):
            kq, _, run_id, giay = mt.chay(spark, f"đánh giá: lỗi pipeline {loi}")
        breaking = [v for v in get_violations(mt.db, run_id=run_id) if v["severity"] in ("BREAKING", "CRITICAL")]
        lech = [v for v in breaking if v["error_type"] == "PIPELINE_OUTPUT_DRIFT" and v["affected_table"] == bang]
        nhiem, _ = so_sanh(sach, _ai_tren_dia(mt))

        # Cùng lỗi nhưng tắt bộ phát hiện: chỉ còn cổng hợp đồng
        pipeline_drift.BAT_KIEM_TRA = False
        try:
            with inject_errors.gay_loi_pipeline(loi):
                mt.chay(spark, f"đánh giá: lỗi pipeline {loi}, không có bộ phát hiện")
        finally:
            pipeline_drift.BAT_KIEM_TRA = True
        nhiem_kg, _ = so_sanh(sach, _ai_tren_dia(mt))
        # Sửa code (tắt lỗi) chạy lại: tầng standardized / AI-ready trở về như lúc sạch
        mt.chay(spark, f"đánh giá: sửa code sau lỗi pipeline {loi}")
        dong.append({
            "loi": loi,
            "mo_ta": mo_ta,
            "phat_hien": bool(lech),
            "hop_dong_bat_duoc": any(v["affected_table"] == bang and v not in lech for v in breaking),
            "chi_ra_bang_ai": bool(lech) and all(AI in v["affected_ai_table"].split(",") for v in lech),
            "nhiem_ban_co_bo_phat_hien": nhiem,
            "nhiem_ban_khong_co": nhiem_kg,
            "khoi_phuc_sach": so_sanh(sach, _ai_tren_dia(mt))[0] == 0,
            "thoi_gian_giay": round(giay, 1),
        })
    bang = pd.DataFrame(dong)
    print(bang.drop(columns=["mo_ta"]).to_string(index=False))
    tom_tat = {
        "phat_hien": f"{int(bang['phat_hien'].sum())}/{len(bang)}",
        "hop_dong_bat_duoc": f"{int(bang['hop_dong_bat_duoc'].sum())}/{len(bang)}",
        "dong_nhiem_ban_co_bo_phat_hien": int(bang["nhiem_ban_co_bo_phat_hien"].sum()),
        "dong_nhiem_ban_khong_co_bo_phat_hien": int(bang["nhiem_ban_khong_co"].sum()),
        "khoi_phuc_sach": bool(bang["khoi_phuc_sach"].all()),
    }
    print("\nTóm tắt:", tom_tat)
    return bang, tom_tat


# ---------------------------------------------------------------------------
# E3
# ---------------------------------------------------------------------------

def danh_gia_hieu_nang(spark, mt, nhanh):
    tieu_de("E3. Hiệu năng")
    ket_qua = {}

    # (a) Lập hồ sơ mọi lô: pandas từng file vs Spark mỗi loại một job (DB tạm, không dùng hồ sơ đã lưu)
    cac_file = standardize.tim_file_gui(mt.raw)
    bat_dau = time.perf_counter()
    for f in cac_file:
        profile_pandas(pd.read_csv(f.duong_dan), find_contract(f.bang))
    giay_pandas = time.perf_counter() - bat_dau
    db_tam = init_database(os.path.join(mt.thu_muc, "ho_so_tam.db"), seed_sample_data=False)
    bat_dau = time.perf_counter()
    for loai in FILE_THEO_LOAI:
        standardize.lap_ho_so(spark, [f for f in cac_file if f.loai == loai],
                              find_contract(f"raw_{loai}_hospital_x"), db_tam)
    giay_spark = time.perf_counter() - bat_dau
    ket_qua["lap_ho_so"] = {"so_file": len(cac_file), "pandas_tung_file_giay": round(giay_pandas, 1),
                            "spark_giay": round(giay_spark, 1)}
    print(f"  (a) Lập hồ sơ {len(cac_file)} lô: pandas từng file {giay_pandas:.1f}s | Spark {giay_spark:.1f}s")

    # (d) Chạy lại tăng dần: không file nào đổi -> dùng lại hồ sơ đã lưu
    mt.xoa_ho_so_da_luu()
    _, _, _, giay_dau = mt.chay(spark, "đánh giá: lần chạy đầu (tính hồ sơ mọi lô)")
    _, _, _, giay_lai = mt.chay(spark, "đánh giá: chạy lại, không file nào đổi")
    ket_qua["tang_dan"] = {"lan_dau_giay": round(giay_dau, 1), "chay_lai_giay": round(giay_lai, 1)}
    print(f"  (d) Pipeline lần đầu {giay_dau:.1f}s | chạy lại khi không file nào đổi {giay_lai:.1f}s")
    if nhanh:
        return ket_qua, pd.DataFrame(), pd.DataFrame()

    # (b) Theo số core: mỗi lần một SparkSession mới, xóa hồ sơ đã lưu để đo chi phí lần chạy đầu
    so_core_may = os.cpu_count() or 1
    dong_core = []
    master_cu = os.environ.get("SPARK_MASTER")
    for so_core in sorted({1, 2, 4, 8, so_core_may} & set(range(1, so_core_may + 1))):
        stop_spark()
        os.environ["SPARK_MASTER"] = f"local[{so_core}]"
        phien = get_spark("danh-gia-so-core")
        mt.xoa_ho_so_da_luu()
        _, _, _, giay = mt.chay(phien, f"đánh giá: local[{so_core}]")
        dong_core.append({"so_core": so_core, "giay": round(giay, 1)})
        print(f"  (b) local[{so_core}]: {giay:.1f}s")
    stop_spark()
    if master_cu is None:
        os.environ.pop("SPARK_MASTER", None)
    else:
        os.environ["SPARK_MASTER"] = master_cu
    spark = get_spark("danh-gia")
    bang_core = pd.DataFrame(dong_core)
    bang_core["tang_toc"] = (bang_core["giay"].iloc[0] / bang_core["giay"]).round(2)

    # (c) Theo số bệnh viện (lượng dữ liệu), mỗi lần một môi trường mới
    tat_ca = xep_hang_staging()
    dong_vien = []
    for so_vien in sorted({min(k, len(tat_ca)) for k in (25, 50, 100, len(tat_ca))}):
        with tempfile.TemporaryDirectory() as tam:
            nho = MoiTruong(tam, cac_vien=tat_ca[:so_vien])
            so_dong = sum(_dem_dong(f.duong_dan) for f in standardize.tim_file_gui(nho.raw))
            _, _, _, giay = nho.chay(spark, f"đánh giá: {so_vien} viện")
        dong_vien.append({"so_vien": so_vien, "so_dong_raw": so_dong, "giay": round(giay, 1),
                          "dong_moi_giay": int(so_dong / giay)})
        print(f"  (c) {so_vien} viện, {so_dong} dòng raw: {giay:.1f}s")
    return ket_qua, bang_core, pd.DataFrame(dong_vien)


# ---------------------------------------------------------------------------
# E4
# ---------------------------------------------------------------------------

def danh_gia_phan_quyen(mt, vien):
    tieu_de("E4. Phân quyền: ma trận vai trò x tầng so với kỳ vọng")
    bang_theo_tang = {"raw": f"raw_patient_hospital_{vien}", "standardized": "standardized_patients",
                      "ai_ready": AI}
    dong, dung = [], 0
    for vai_tro, theo_tang in KY_VONG_QUYEN.items():
        tai_khoan = TAI_KHOAN_THEO_VAI_TRO[vai_tro]
        for tang, (doc, ghi) in theo_tang.items():
            bang = bang_theo_tang[tang]
            thuc_doc = evaluate_access(tai_khoan, bang, "read", db_path=mt.db)["allowed"]
            thuc_ghi = evaluate_access(tai_khoan, bang, "write", db_path=mt.db)["allowed"]
            dung += (thuc_doc == doc) + (thuc_ghi == ghi)
            dong.append({"vai_tro": vai_tro, "tang": tang, "doc": thuc_doc, "ghi": thuc_ghi,
                         "dung_ky_vong": thuc_doc == doc and thuc_ghi == ghi})
    bang = pd.DataFrame(dong)
    print(bang.to_string(index=False))

    nguoi_la = evaluate_access("nguoi_khong_co_trong_danh_sach", AI, "read", db_path=mt.db)["allowed"]
    # Gõ tên vai trò thay cho tài khoản không được nhận quyền của vai trò đó
    ten_vai_tro = any(evaluate_access(ten, bang_theo_tang["raw"], "read", db_path=mt.db)["allowed"]
                      for ten in ("data_engineer", "Kỹ sư dữ liệu"))
    try:
        read_table("researcher_bob", f"raw_patient_hospital_{vien}", db_path=mt.db)
        tu_choi_that = False
    except AccessDenied:
        tu_choi_that = True
    cot_analyst = list(read_table("analyst_dave", "standardized_patients", db_path=mt.db).columns)
    cot_ky_su = list(read_table("engineer_alice", "standardized_patients", db_path=mt.db).columns)
    tom_tat = {
        "dung_ky_vong": f"{dung}/{2 * len(bang)}",
        "nguoi_la_bi_tu_choi": not nguoi_la,
        "ten_vai_tro_bi_tu_choi": not ten_vai_tro,
        "read_table_nem_AccessDenied": tu_choi_that,
        "an_patient_id_voi_analyst": "patient_id" not in cot_analyst,
        "ky_su_thay_patient_id": "patient_id" in cot_ky_su,
    }
    print("\nTóm tắt:", tom_tat)
    return bang, tom_tat


# ---------------------------------------------------------------------------
# E5
# ---------------------------------------------------------------------------

def _chen_dong_gia(conn):
    """Kẻ gian có quyền ghi DB, biết thuật toán nhưng KHÔNG có khóa audit: tự tính băm bằng khóa đoán."""
    cuoi = conn.execute("SELECT id, row_hash FROM audit_log ORDER BY id DESC LIMIT 1").fetchone()
    ma = cuoi[0] + 1
    gia = ("2026-10-29 03:00:00", "researcher_bob", "read:raw", "raw_patient_hospital_123", "ALLOWED", "")
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
            ban_sao = backup_database(os.path.join(tam, "ban_sao.db"), mt.db)
            conn = sqlite3.connect(ban_sao)
            lenh(conn)
            conn.commit()
            conn.close()
            kq = verify_audit_chain(ban_sao)
            dong.append({"kieu_can_thiep": ten, "phat_hien": not kq["ok"], "chi_tiet": kq["ly_do"]})
            os.remove(ban_sao)
    bang = pd.DataFrame(dong)
    print(bang.to_string(index=False))
    return bang, {"so_goc_toan_ven": goc["ok"], "so_dong": goc["so_dong"],
                  "phat_hien": f"{int(bang['phat_hien'].sum())}/{len(bang)}"}


# ---------------------------------------------------------------------------
# E6
# ---------------------------------------------------------------------------

def danh_gia_provenance(spark, mt):
    from pyspark.sql import functions as F

    tieu_de("E6. Provenance")
    mt.chay(spark, "đánh giá: chạy lại sau khi sửa hết lỗi")
    phien_ban = provenance.get_latest_version(AI, mt.db)
    lo_nguon = provenance.source_batches(phien_ban["id"], mt.db)
    kiem_tra = provenance.verify_version(phien_ban["id"], mt.db)

    # Ghi đè bảng standardized bằng một job Spark ngoài pipeline rồi tạo lại AI-ready
    duong_dan = os.path.join(mt.std, "standardized_patients")
    goc = spark.read.format("delta").load(duong_dan)
    ma_dau = goc.select(F.min("patient_id")).first()[0]
    (goc.withColumn("weight_kg", F.when(F.col("patient_id") == ma_dau, F.lit(999.0)).otherwise(F.col("weight_kg")))
     .write.format("delta").mode("overwrite").save(duong_dan))
    with im_lang(), provenance.pipeline_run("pipeline_service", "đánh giá: bảng bị ghi ngoài pipeline", mt.db) as run_id:
        ai_df = build_ai_ready.main(mt.db, mt.std, mt.ai, spark)
    chan_ghi_ngoai = ai_df is None and any(v["error_type"] == "PROVENANCE_UNVERIFIED"
                                           for v in get_violations(mt.db, run_id=run_id))

    # Chạy lại pipeline -> phiên bản mới; phiên bản cũ vẫn kiểm tra được nhờ time travel
    mt.chay(spark, "đánh giá: khôi phục")
    time_travel = provenance.verify_version(phien_ban["id"], mt.db)["ok"]

    # Sửa một file raw SAU khi đã nạp -> verify_version phải chỉ ra đúng file đó
    lo = lo_nguon[0]
    with open(provenance.from_catalog_path(lo["source_file"]), "a", encoding="utf-8") as f:
        f.write("\n")
    kiem_tra_sau = provenance.verify_version(phien_ban["id"], mt.db)
    chi_ra_file = any(lo["source_file"] in v for v in kiem_tra_sau["van_de"])

    tom_tat = {
        "so_lo_nguon_cua_ai_ready": len(lo_nguon),
        "so_vien_nguon": len({b["hospital"] for b in lo_nguon}),
        "so_file_kiem_checksum": kiem_tra["da_kiem"],
        "checksum_khop_het": kiem_tra["ok"],
        "chan_bang_ghi_ngoai_pipeline": chan_ghi_ngoai,
        "kiem_tra_phien_ban_cu_bang_time_travel": time_travel,
        "phat_hien_file_raw_bi_sua_sau_khi_nap": chi_ra_file,
    }
    print(pd.Series(tom_tat).to_string())
    return tom_tat


def main(argv=None):
    parser = argparse.ArgumentParser(description="Đánh giá hệ thống governance trên dữ liệu thật")
    parser.add_argument("--out", help="thư mục ghi kết quả (CSV + tong_hop.json)")
    parser.add_argument("--nhanh", action="store_true", help="bỏ phép đo theo số core và số bệnh viện")
    args = parser.parse_args(argv)

    if not danh_sach_vien(STAGING_DIR):
        tieu_de("Chưa có data/02_staging -> tách mọi bệnh viện từ data/01_landing")
        split_hospitals.main()
    if any(not os.path.isdir(os.path.join(STAGING_DIR, f"hospital_{ma}"))
           for nguon in load_sources() if nguon["hospitals"] != "default" for ma in nguon["hospitals"]):
        tieu_de("Chuyển các nguồn khác chuẩn về schema chuẩn (pipeline/source_adapters.py)")
        source_adapters.main()

    spark = get_spark("danh-gia")
    with tempfile.TemporaryDirectory(prefix="danh_gia_governance_") as tam:
        mt = MoiTruong(tam)
        e1, sach = danh_gia_du_lieu_that(spark, mt)
        bang_loi, tom_tat_loi = danh_gia_loi(spark, mt, sach)
        bang_loi_pipeline, tom_tat_loi_pipeline = danh_gia_loi_pipeline(spark, mt, sach)
        e3, bang_core, bang_vien = danh_gia_hieu_nang(spark, mt, args.nhanh)
        spark = get_spark("danh-gia")
        bang_quyen, tom_tat_quyen = danh_gia_phan_quyen(mt, inject_errors.xep_hang_vien(mt.raw)[1])
        bang_audit, tom_tat_audit = danh_gia_audit(mt)
        tom_tat_prov = danh_gia_provenance(spark, mt)

    tong_hop = {"E1_du_lieu_that": e1, "E2_loi": tom_tat_loi,
                "E3_hieu_nang": {**e3, "theo_so_core": bang_core.to_dict("records"),
                                 "theo_so_vien": bang_vien.to_dict("records")},
                "E4_phan_quyen": tom_tat_quyen, "E5_audit": tom_tat_audit, "E6_provenance": tom_tat_prov,
                "E7_loi_pipeline": tom_tat_loi_pipeline}
    tieu_de("TỔNG HỢP")
    print(json.dumps(tong_hop, ensure_ascii=False, indent=2))

    if args.out:
        os.makedirs(args.out, exist_ok=True)
        bang_loi.to_csv(os.path.join(args.out, "e2_loi.csv"), index=False)
        bang_loi_pipeline.to_csv(os.path.join(args.out, "e7_loi_pipeline.csv"), index=False)
        if not bang_core.empty:
            bang_core.to_csv(os.path.join(args.out, "e3_theo_so_core.csv"), index=False)
            bang_vien.to_csv(os.path.join(args.out, "e3_theo_so_vien.csv"), index=False)
        bang_quyen.to_csv(os.path.join(args.out, "e4_phan_quyen.csv"), index=False)
        bang_audit.to_csv(os.path.join(args.out, "e5_audit.csv"), index=False)
        with open(os.path.join(args.out, "tong_hop.json"), "w", encoding="utf-8") as f:
            json.dump(tong_hop, f, ensure_ascii=False, indent=2)
        print(f"\nĐã ghi kết quả vào {os.path.abspath(args.out)}")

    so_o_quyen = 2 * len(bang_quyen)
    dat = (tom_tat_loi["phat_hien"] == f"{len(KY_VONG_LOI)}/{len(KY_VONG_LOI)}"
           and tom_tat_loi["chan_dung_lo"] == f"{len(KY_VONG_LOI)}/{len(KY_VONG_LOI)}"
           and tom_tat_loi["dong_nhiem_ban_co_governance"] == 0
           and tom_tat_quyen["dung_ky_vong"] == f"{so_o_quyen}/{so_o_quyen}"
           and all(v for k, v in tom_tat_quyen.items() if k != "dung_ky_vong")
           and tom_tat_audit["phat_hien"] == f"{len(bang_audit)}/{len(bang_audit)}"
           and tom_tat_loi_pipeline["phat_hien"] == f"{len(inject_errors.LOI_PIPELINE)}/{len(inject_errors.LOI_PIPELINE)}"
           and tom_tat_loi_pipeline["dong_nhiem_ban_co_bo_phat_hien"] == 0 and tom_tat_loi_pipeline["khoi_phuc_sach"]
           and all(tom_tat_prov[k] for k in ("checksum_khop_het", "chan_bang_ghi_ngoai_pipeline",
                                            "kiem_tra_phien_ban_cu_bang_time_travel",
                                            "phat_hien_file_raw_bi_sua_sau_khi_nap")))
    print("\n" + ("✅ ĐÁNH GIÁ ĐẠT mọi kỳ vọng" if dat else "❌ CÓ KỲ VỌNG CHƯA ĐẠT, xem các bảng ở trên"))
    return 0 if dat else 1


if __name__ == "__main__":
    sys.exit(main())
