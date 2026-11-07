"""
Chạy toàn bộ kịch bản demo bằng một lệnh (từ gốc repo):

    python run_demo.py --reset              # xóa metadata.db cũ, chạy với MỌI bệnh viện của eICU demo (186 viện)
    python run_demo.py --reset --top 3      # chạy nhanh với 3 viện đông bệnh nhân nhất
    python run_demo.py                      # chạy tiếp trên metadata.db hiện có

Pipeline chạy bằng Apache Spark (SPARK_MASTER, mặc định local[*]) và ghi tầng standardized / AI-ready
thành bảng Delta Lake. Xem pipeline/spark_session.py để chạy trên cụm Spark.

Kịch bản:
  1. Dữ liệu thật của các bệnh viện đi qua 3 tầng; các lô kém chất lượng CÓ SẴN trong eICU
     (ví dụ viện 246 thiếu 64-100% nhịp tim) bị chặn đúng lô, phần còn lại lên tới AI-ready.
  1b. Nguồn KHÔNG đồng nhất: Challenge 2012 (dạng dài EAV, mã 0/1, -1 là thiếu, nhãn tử vong trong bệnh viện)
     được bộ chuyển đưa về schema chuẩn theo sources/challenge2012.yaml, lên AI-ready cùng eICU; lineage từ
     tham số gốc; cảnh báo SEMANTIC_MISMATCH vì nhãn định nghĩa khác (nếu đã tải dữ liệu, xem README).
  2. Viện hạng 2 (đông bệnh nhân thứ 2) đổi tên cột admissionweight -> weight ở đợt 3 -> bị chặn,
     in vi phạm và bảng AI-ready bị ảnh hưởng, audit_log có dòng BLOCKED, provenance ghi lô BLOCKED.
  3. Kỹ sư dữ liệu DUYỆT thay đổi đó ('weight' chính là 'admissionweight') -> chạy lại -> lô qua cổng mà
     bệnh viện không phải gửi lại, vi phạm kịch bản 2 chuyển RESOLVED, provenance của lô ghi ánh xạ đã duyệt.
  4. Phân quyền được thực thi khi đọc dữ liệu: nhà nghiên cứu đọc raw bị từ chối (DENIED),
     chuyên viên phân tích đọc standardized nhưng cột định danh bị ẩn.
  5. Provenance: truy ngược bảng AI-ready về từng lô nguồn (checksum); ghi đè bảng standardized
     ngoài pipeline -> tạo AI-ready bị chặn (PROVENANCE_UNVERIFIED); lịch sử phiên bản Delta.
  6. Sổ audit chống sửa: chuỗi băm toàn vẹn; sửa một dòng (trên bản sao DB) -> bị phát hiện.
  7. Pipeline đổi code (giả lập: chuẩn hóa cân nặng sang pound): dữ liệu vẫn đúng hợp đồng nên cổng không bắt
     được, bộ phát hiện lệch đầu ra chặn (PIPELINE_OUTPUT_DRIFT) -> tầng AI-ready không bị nhiễm bẩn;
     sửa code (tắt lỗi) chạy lại -> vi phạm RESOLVED.
"""

import argparse
import os
import sqlite3
import sys
import tempfile
from collections import Counter

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from catalog_access import AccessDenied, read_table, verify_audit_chain  # noqa: E402
from catalog_access.init_db import DEFAULT_DB_PATH, backup_database, get_db_connection, init_database  # noqa: E402
from governance import provenance  # noqa: E402
from governance.lineage import get_impact_paths  # noqa: E402
from governance.schema_registry import approve_rename  # noqa: E402
from governance.sources import load_sources, native_table, source_of_hospital  # noqa: E402
from governance.violation_engine import get_violations  # noqa: E402
from pipeline import (build_ai_ready, ingest_raw, inject_errors, source_adapters, split_hospitals,  # noqa: E402
                      standardize)
from pipeline.spark_session import get_spark  # noqa: E402

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


def chay_pipeline(spark, ghi_chu):
    """standardize -> build_ai_ready trong CÙNG một lần chạy. Trả về (kq standardize, AI-ready df, run_id)."""
    with provenance.pipeline_run(ACTOR, ghi_chu) as run_id:
        tieu_de(f"Chuẩn hóa: raw -> standardized (Spark) | {run_id}")
        kq_std = standardize.main(spark=spark)
        tieu_de(f"Tạo tầng AI-ready (Spark) | {run_id}")
        ai_df = build_ai_ready.main(spark=spark)
    return kq_std, ai_df, run_id


def in_tom_tat(kq_std, ai_df, run_id, vi_pham_breaking):
    print(f"  Lần chạy              : {run_id} -> {trang_thai_run(run_id)}")
    print(f"  Số đợt đi qua đủ cổng : {kq_std['so_dot_dat']}/{kq_std['so_dot']}")
    print(f"  Lô theo kết quả cổng  : {kq_std['trang_thai_lo']}")
    print(f"  Làm sạch (ngoài khoảng hợp lý -> trống): {kq_std['lam_sach']}")
    print(f"  {AI}: {len(ai_df) if ai_df is not None else 'KHÔNG tạo được'} dòng")
    print(f"  Vi phạm BREAKING      : {len(vi_pham_breaking)}")


def in_lo_bi_chan(vi_pham_breaking):
    for v in sorted(vi_pham_breaking, key=lambda x: x["batch"]):
        print(f"    ⛔ {v['error_type']:<22} {v['affected_table']:<26} lô {v['batch']}")
        print(f"       {v['details'][:110]}")


def kich_ban_nguon_khac(run_id):
    """Kịch bản 1b. Trả về True nếu mọi kỳ vọng đều đúng."""
    from deltalake import DeltaTable

    std = DeltaTable(os.path.join(standardize.STD_DIR, "standardized_patients")).to_pandas(columns=["source_hospital"])
    nguon_cua = {m: source_of_hospital(m)["name"] for m in std["source_hospital"].unique()}
    theo_nguon = Counter(std["source_hospital"].map(nguon_cua))
    for nguon in load_sources():
        print(f"  {nguon['name']:<14} {nguon['format']:<9} {nguon['system']}: "
              f"{theo_nguon.get(nguon['name'], 0)} bệnh nhân ở tầng standardized")
    dat = all(theo_nguon.get(n["name"], 0) > 0 for n in load_sources())
    for nguon in load_sources():
        if nguon["hospitals"] == "default":
            continue
        tham_so = nguon["patient"]["admissionweight"]["from"]
        duong_di = get_impact_paths(native_table(nguon), tham_so)
        print(f"  Lineage từ tham số gốc '{tham_so}': {duong_di[0] if duong_di else '(không có)'}")
        dat &= bool(duong_di)
    canh_bao = [v for v in get_violations(run_id=run_id) if v["error_type"] == "SEMANTIC_MISMATCH"]
    for v in canh_bao:
        print(f"  ⚠️  {v['error_type']} {v['affected_table']} (cho qua, ghi lại):\n     {v['details']}")
    return dat and bool(canh_bao)


def kich_ban_loi_pipeline(spark):
    """Kịch bản 7. Trả về True nếu mọi kỳ vọng đều đúng."""
    from deltalake import DeltaTable

    def _can_nang_ai():
        return DeltaTable(os.path.join(build_ai_ready.AI_DIR, AI)).to_pandas(columns=["weight_kg"])["weight_kg"].mean()

    truoc = _can_nang_ai()
    with inject_errors.gay_loi_pipeline("can_nang_pound"):
        print(f"  Phiên bản code đang chạy: {provenance.get_code_version()}")
        kq_std, _, run_id = chay_pipeline(spark, "kịch bản 7: code chuẩn hóa cân nặng bị lỗi (pound)")
    breaking = get_violations(severity="BREAKING", run_id=run_id)
    lech = [v for v in breaking if v["error_type"] == "PIPELINE_OUTPUT_DRIFT"]
    hop_dong = [v for v in breaking if v["affected_table"] == "standardized_patients" and v not in lech]
    for v in lech:
        print(f"  ⛔ [{v['severity']}] {v['error_type']} tại {v['affected_table']}\n     {v['details']}\n"
              f"     Bảng AI-ready được bảo vệ: {v['affected_ai_table']}")
    da_ghi = "standardized_patients" in kq_std["phien_ban"]
    sau = _can_nang_ai()
    print("  Cổng hợp đồng của standardized_patients: " + ("không chặn (cân nặng sai gần như vẫn nằm trong khoảng hợp lý "
                                                         "[20, 400] kg)" if not hop_dong else "có chặn"))
    print(f"  standardized_patients: {'KHÔNG ghi, giữ phiên bản cũ' if not da_ghi else '❌ đã ghi'} | "
          f"cân nặng trung bình tầng AI-ready {truoc:.1f} -> {sau:.1f} kg")

    print("\n  Sửa code (tắt lỗi giả lập) rồi chạy lại:")
    _, _, run_sua = chay_pipeline(spark, "kịch bản 7: code đã sửa")
    da_dong = all(v["status"] == "RESOLVED" for v in get_violations() if v["id"] in {x["id"] for x in lech})
    print(f"  -> {'✅' if da_dong else '❌'} vi phạm PIPELINE_OUTPUT_DRIFT {'đã chuyển RESOLVED' if da_dong else 'vẫn mở'}"
          f" ({run_sua})")
    return bool(lech) and not hop_dong and not da_ghi and abs(sau - truoc) < 1e-9 and da_dong


def kich_ban_phan_quyen(vien):
    """Kịch bản 4. Trả về True nếu mọi kỳ vọng đều đúng."""
    audit_dau = id_lon_nhat("audit_log")
    bang_raw = f"raw_patient_hospital_{vien}"
    cac_lan_thu = [
        ("researcher_bob", bang_raw, False),
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
            and any(a["ai"] == "researcher_bob" and a["bang_nao"] == bang_raw and a["ket_qua"] == "DENIED"
                    for a in audit)
            and any(a["ai"] == "analyst_dave" and a["lam_gi"].startswith("mask:patient_id") for a in audit))


def kich_ban_provenance(spark):
    """Kịch bản 5. Trả về True nếu mọi kỳ vọng đều đúng."""
    from deltalake import DeltaTable
    from pyspark.sql import functions as F

    phien_ban = provenance.get_latest_version(AI)
    if phien_ban is None:
        print("  ❌ Chưa có phiên bản AI-ready nào")
        return False
    cay = provenance.trace_version(phien_ban["id"])
    for dong in provenance.format_trace(cay):
        if "└─ lô" not in dong:
            print("  " + dong)
    lo_nguon = provenance.source_batches(phien_ban["id"])
    theo_loai = Counter(b["table_name"].split("_hospital_")[0] for b in lo_nguon)
    print(f"  -> {len(lo_nguon)} lô nguồn ở tầng raw, từ {len({b['hospital'] for b in lo_nguon})} bệnh viện: "
          f"{dict(theo_loai)}")
    for b in lo_nguon[:3]:
        print(f"     lô #{b['id']} {b['table_name']} / {b['batch']} | {b['row_count']} dòng "
              f"| sha256 {b['checksum'][:16]}… | {b['source_file']}")
    print("     ...")
    kiem_tra = provenance.verify_version(phien_ban["id"])
    print(f"  Kiểm tra lại checksum: {'OK' if kiem_tra['ok'] else 'CÓ VẤN ĐỀ'} ({kiem_tra['da_kiem']} file / bảng)")

    print("\n  Ai đó chạy một job Spark ngoài pipeline, ghi đè bảng standardized_patients (sửa 1 ô cân nặng):")
    duong_dan = os.path.join(standardize.STD_DIR, "standardized_patients")
    goc = spark.read.format("delta").load(duong_dan)
    ma_dau = goc.select(F.min("patient_id")).first()[0]
    (goc.withColumn("weight_kg", F.when(F.col("patient_id") == ma_dau, F.lit(999.0)).otherwise(F.col("weight_kg")))
     .write.format("delta").mode("overwrite").save(duong_dan))
    with provenance.pipeline_run(ACTOR, "thử tạo AI-ready từ bảng bị ghi ngoài pipeline") as run_id:
        ai_df = build_ai_ready.main(spark=spark)
    vp = get_violations(run_id=run_id)
    bi_chan = ai_df is None and any(v["error_type"] == "PROVENANCE_UNVERIFIED" for v in vp)
    print(f"  -> {'⛔ Bị chặn (PROVENANCE_UNVERIFIED)' if bi_chan else '❌ KHÔNG bị chặn'} | lần chạy {run_id}: "
          f"{trang_thai_run(run_id)}")

    print("\n  Lịch sử phiên bản Delta của standardized_patients (mới nhất trước):")
    for h in DeltaTable(duong_dan).history()[:4]:
        print(f"     v{h['version']:<3} {h.get('operation', ''):<8} {h.get('engineInfo', '')}")

    print("\n  Chạy lại pipeline để khôi phục tầng standardized và AI-ready từ dữ liệu raw:")
    _, ai_df, run_id = chay_pipeline(spark, "khôi phục sau khi bảng standardized bị ghi ngoài pipeline")
    khoi_phuc = ai_df is not None and trang_thai_run(run_id) in ("SUCCESS", "PARTIAL")
    print(f"  -> {'✅ Đã khôi phục' if khoi_phuc else '❌ Chưa khôi phục'} ({run_id})")

    # Phiên bản AI-ready cũ vẫn kiểm tra được dù bảng đã có phiên bản mới: nhờ time travel của Delta
    kiem_tra_cu = provenance.verify_version(phien_ban["id"])
    print(f"  Kiểm tra lại phiên bản AI-ready cũ #{phien_ban['id']} (Delta v{phien_ban['table_version']}) bằng "
          f"time travel: {'OK' if kiem_tra_cu['ok'] else 'CÓ VẤN ĐỀ'}")
    return kiem_tra["ok"] and len(lo_nguon) > 0 and bi_chan and khoi_phuc and kiem_tra_cu["ok"]


def kich_ban_audit():
    """Kịch bản 6. Trả về True nếu mọi kỳ vọng đều đúng."""
    that = verify_audit_chain()
    print(f"  metadata.db: {'✅' if that['ok'] else '❌'} {that['ly_do']} ({that['so_dong']} dòng, "
          f"băm cuối {str(that['hash_cuoi'])[:16]}…)")

    with tempfile.TemporaryDirectory() as thu_muc:
        ban_sao = backup_database(os.path.join(thu_muc, "ban_sao.db"))
        conn = sqlite3.connect(ban_sao)
        dong = conn.execute("SELECT id FROM audit_log WHERE ket_qua = 'DENIED' ORDER BY id LIMIT 1").fetchone()
        conn.execute("UPDATE audit_log SET ket_qua = 'ALLOWED' WHERE id = ?", (dong[0],))
        conn.commit()
        conn.close()
        bi_sua = verify_audit_chain(ban_sao)
    print(f"  Bản sao bị sửa dòng #{dong[0]} (DENIED -> ALLOWED): "
          f"{'⛔ phát hiện - ' + bi_sua['ly_do'] if not bi_sua['ok'] else '❌ KHÔNG phát hiện'}")
    return that["ok"] and not bi_sua["ok"] and bi_sua["dong_loi"] == dong[0]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Chạy toàn bộ kịch bản demo governance")
    parser.add_argument("--reset", action="store_true", help="xóa metadata.db cũ trước khi chạy")
    parser.add_argument("--top", type=int, help="chỉ dùng k bệnh viện đông bệnh nhân nhất (mặc định: mọi viện)")
    args = parser.parse_args(argv)
    ket_qua = []

    # -----------------------------------------------------------------------
    tieu_de("BƯỚC 0: Khởi tạo metadata.db (không nạp dữ liệu giả)")
    if args.reset and os.path.exists(DEFAULT_DB_PATH):
        for duoi in ("", "-wal", "-shm"):
            if os.path.exists(DEFAULT_DB_PATH + duoi):
                os.remove(DEFAULT_DB_PATH + duoi)
        print(f"Đã xóa DB cũ: {DEFAULT_DB_PATH}")
    init_database(seed_sample_data=False)
    print(f"DB: {DEFAULT_DB_PATH}")

    tieu_de("BƯỚC 1: Tách dữ liệu eICU demo theo bệnh viện + chuyển nguồn khác về schema chuẩn")
    split_hospitals.main(args.top)
    nguon_khac = source_adapters.main()

    tieu_de("BƯỚC 2: Nạp tầng RAW, chia 3 đợt và đăng ký catalog (ingest_raw)")
    ingest_raw.main()

    tieu_de("BƯỚC 3: Khởi động Spark + Delta Lake")
    spark = get_spark()
    sc = spark.sparkContext
    print(f"  Spark {spark.version} | master {sc.master} | {sc.defaultParallelism} luồng xử lý song song"
          + (f" | giao diện {sc.uiWebUrl}" if sc.uiWebUrl else ""))

    # -----------------------------------------------------------------------
    kq_std, ai_df, run_id = chay_pipeline(spark, "kịch bản 1: dữ liệu thật")
    breaking = get_violations(severity="BREAKING", run_id=run_id)
    lo_kem_co_san = set(kq_std["bi_chan"])
    dat_1 = (ai_df is not None and len(ai_df) == kq_std["so_dong"]
             and trang_thai_run(run_id) in ("SUCCESS", "PARTIAL")
             and all(v["error_type"] in ("COMPLETENESS", "PLAUSIBILITY", "CONFORMANCE") for v in breaking))
    tieu_de("KỊCH BẢN 1: dữ liệu thật – chặn đúng các lô kém chất lượng có sẵn, phần còn lại lên AI-ready"
            + ("" if dat_1 else "  (❌ CHƯA ĐẠT)"))
    in_tom_tat(kq_std, ai_df, run_id, breaking)
    if breaking:
        print("  Các lô kém chất lượng có sẵn trong dữ liệu thật (bị chặn):")
        in_lo_bi_chan(breaking)
    print(f"  Cảnh báo (cho qua, đã ghi lại): {len(get_violations(severity='WARNING', run_id=run_id))}")
    ket_qua.append(("Kịch bản 1: dữ liệu thật qua 3 tầng, lô kém chất lượng có sẵn bị chặn đúng lô", dat_1))
    so_dong_ai = len(ai_df) if ai_df is not None else None

    if nguon_khac:
        tieu_de("KỊCH BẢN 1b: Nguồn không đồng nhất – Challenge 2012 (dạng dài EAV) lên AI-ready cùng eICU (CSV)")
        ket_qua.append(("Kịch bản 1b: hai hệ thống nguồn khác định dạng cùng lên AI-ready, lineage từ tham số gốc, "
                        "cảnh báo nhãn khác định nghĩa", kich_ban_nguon_khac(run_id)))
    else:
        print("\n(Bỏ qua kịch bản 1b: chưa tải dữ liệu Challenge 2012, xem README mục 3)")

    # -----------------------------------------------------------------------
    vien = inject_errors.xep_hang_vien()[inject_errors.MAPPING_HANG["doi_ten"] - 1]
    tieu_de(f"KỊCH BẢN 2: Bệnh viện {vien} đổi tên cột admissionweight -> weight (inject_errors --loi doi_ten)")
    inject_errors.gay_loi("doi_ten", vien=vien)
    audit_dau = id_lon_nhat("audit_log")
    kq_std, ai_df, run_id = chay_pipeline(spark, f"kịch bản 2: viện {vien} đổi tên cột")
    breaking = get_violations(severity="BREAKING", run_id=run_id)
    doi_ten = [v for v in breaking if v["error_type"] == "SCHEMA_COLUMN_RENAMED"]
    chan = audit_moi(audit_dau, "BLOCKED")
    lo_loi = [b for b in provenance.get_batches(run_id) if b["batch"].startswith(f"hospital_{vien}/")
              and b["batch"].endswith("20-00-00")]

    tieu_de("KẾT QUẢ KỊCH BẢN 2: vi phạm và bảng AI-ready bị ảnh hưởng")
    in_tom_tat(kq_std, ai_df, run_id, breaking)
    for v in doi_ten:
        print(f"\n  ⛔ [{v['severity']}] {v['error_type']} | {v['hospital']} | bảng {v['affected_table']}"
              f" | lô {v['batch']}")
        print(f"     {v['details']}")
        print(f"     Bảng AI-ready bị ảnh hưởng: {v['affected_ai_table'] or '(không có)'} | hành động: {v['action']}")
    print("\n  Dòng audit_log BLOCKED của viện này:")
    for a in chan:
        if a["bang_nao"].endswith(f"_hospital_{vien}"):
            print(f"     ai={a['ai']} | làm gì={a['lam_gi']} | bảng={a['bang_nao']} | {a['ket_qua']}")
    print(f"\n  Provenance của đợt 3 viện {vien} trong lần chạy này:")
    for b in lo_loi:
        print(f"     lô #{b['id']} {b['table_name']:<26} {b['status']:<8} {b['row_count']:>5} dòng "
              f"| sha256 {b['checksum'][:16]}…")
    dat_2 = (len(doi_ten) == 1 and doi_ten[0]["affected_table"] == f"raw_patient_hospital_{vien}"
             and AI in doi_ten[0]["affected_ai_table"].split(",")
             and any(a["bang_nao"] == f"raw_patient_hospital_{vien}" for a in chan)
             and {(b["table_name"], b["status"]) for b in lo_loi} == {
                 (f"raw_patient_hospital_{vien}", "BLOCKED"), (f"raw_vital_hospital_{vien}", "SKIPPED"),
                 (f"raw_lab_hospital_{vien}", "SKIPPED")}
             and set(kq_std["bi_chan"]) - lo_kem_co_san == {f"raw_patient_hospital_{vien} / {lo_loi[0]['batch'].split('/')[1]}"}
             and trang_thai_run(run_id) == "PARTIAL")
    ket_qua.append((f"Kịch bản 2: viện {vien} đổi tên cột bị chặn đúng lô, chỉ ra {AI}, có audit BLOCKED", dat_2))

    # -----------------------------------------------------------------------
    bang = f"raw_patient_hospital_{vien}"
    tieu_de(f"KỊCH BẢN 3: Kỹ sư dữ liệu DUYỆT thay đổi schema của viện {vien} ('weight' chính là 'admissionweight')")
    ma_duyet = approve_rename("engineer_alice", bang, "weight", "admissionweight",
                              "Viện đổi tên cột cân nặng, nội dung không đổi")
    print(f"  engineer_alice duyệt #{ma_duyet} (ghi audit) -> bệnh viện KHÔNG phải gửi lại dữ liệu")
    kq_std, ai_df, run_id = chay_pipeline(spark, f"kịch bản 3: đã duyệt đổi tên cột của viện {vien}")
    breaking = get_violations(severity="BREAKING", run_id=run_id)
    vp_doi_ten = [v for v in get_violations(table=bang) if v["error_type"] == "SCHEMA_COLUMN_RENAMED"]
    lo_duyet = [b for b in provenance.get_batches(run_id) if b["mapping"]]
    duong_di = get_impact_paths(bang, "weight")
    dat_3 = (set(kq_std["bi_chan"]) == lo_kem_co_san and ai_df is not None and len(ai_df) == so_dong_ai
             and not any(v["error_type"].startswith("SCHEMA_") for v in breaking)
             and bool(vp_doi_ten) and all(v["status"] == "RESOLVED" for v in vp_doi_ten)
             and [(b["table_name"], b["status"]) for b in lo_duyet] == [(bang, "ACCEPTED")] and bool(duong_di))
    tieu_de(f"KẾT QUẢ KỊCH BẢN 3: lô của viện {vien} qua cổng nhờ ánh xạ đã duyệt" + ("" if dat_3 else "  (❌ CHƯA ĐẠT)"))
    in_tom_tat(kq_std, ai_df, run_id, breaking)
    for v in vp_doi_ten:
        print(f"  Vi phạm #{v['id']} {v['error_type']}: {v['status']} – {v['resolution']}")
    for b in lo_duyet:
        print(f"  Provenance lô #{b['id']} {b['table_name']} / {b['batch']}: {b['status']}, ánh xạ: {b['mapping']}")
    print(f"  Lineage theo tên cột mới: {duong_di[0] if duong_di else '(không có)'}")
    ket_qua.append((f"Kịch bản 3: duyệt đổi tên cột thì lô viện {vien} qua cổng, vi phạm RESOLVED, "
                    "provenance ghi ánh xạ", dat_3))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 4: Phân quyền được THỰC THI khi đọc dữ liệu (catalog_access.read_table)")
    ket_qua.append(("Kịch bản 4: nhà nghiên cứu đọc raw bị từ chối, cột định danh bị ẩn với chuyên viên phân tích",
                    kich_ban_phan_quyen(vien)))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 5: Provenance – AI-ready được tạo từ những lô nào, bảng bị ghi ngoài pipeline thì sao")
    ket_qua.append(("Kịch bản 5: truy ngược AI-ready về lô nguồn, bảng bị ghi ngoài pipeline bị chặn, time travel",
                    kich_ban_provenance(spark)))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 6: Sổ audit chống sửa (chuỗi băm HMAC-SHA256)")
    ket_qua.append(("Kịch bản 6: sổ audit toàn vẹn, sửa một dòng là bị phát hiện", kich_ban_audit()))

    # -----------------------------------------------------------------------
    tieu_de("KỊCH BẢN 7: Pipeline đổi code (giả lập: chuẩn hóa cân nặng sang pound) – đúng hợp đồng nhưng sai nghĩa")
    ket_qua.append(("Kịch bản 7: lỗi do pipeline đổi code lọt qua hợp đồng nhưng bị chặn vì đầu ra lệch, "
                    "sửa code thì vi phạm RESOLVED", kich_ban_loi_pipeline(spark)))

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
