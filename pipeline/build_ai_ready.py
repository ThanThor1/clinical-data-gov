"""
Tạo tầng AI-READY bằng Apache Spark: standardized_patients (+ standardized_labs) -> ai_ready_icu_mortality.

  1. Cổng provenance: chỉ dùng bảng standardized do pipeline tạo ra (checksum phiên bản Delta hiện tại
     khớp một phiên bản đã ghi trong dataset_versions). Bảng bị ghi ngoài pipeline -> chặn,
     vi phạm PROVENANCE_UNVERIFIED.
  2. Giả danh patient_id -> patient_hash bằng HMAC-SHA256 (khóa bí mật, governance/privacy.py),
     tính bằng hàm có sẵn của Spark nên chạy ngay trên executor, không cần UDF Python; khóa không
     xuất hiện trong kế hoạch truy vấn / giao diện Spark UI.
  3. Đặc trưng xét nghiệm: creatinine_max, glucose_mean theo từng bệnh nhân (groupBy trên bảng lab).
  4. gate1 (schema + hợp đồng + quyền riêng tư) rồi gate2 (chất lượng), trên hồ sơ tính bằng Spark;
     rồi kiểm tra đầu ra có lệch khi code pipeline đổi không (governance/pipeline_drift.py).
  5. Ghi bảng Delta Lake qua write_table_spark, ghi phiên bản (đầu vào = 2 phiên bản standardized),
     đăng ký catalog.

Chạy: python -m pipeline.build_ai_ready   (từ gốc repo)
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_access import register_dataset
from catalog_access.data_access import is_delta_table, read_table_spark, to_catalog_path, write_table_spark
from governance.contracts import catalog_metadata, find_contract
from governance.gates import check_gate
from governance.pipeline_drift import check_pipeline_drift
from governance.privacy import pseudonymize_spark
from governance.profiling import profile_spark
from governance.provenance import pipeline_run, record_dataset_version, verify_input
from pipeline.inject_errors import loi_pipeline
from pipeline.spark_session import get_spark

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
STD_DIR = os.path.join(DATA_DIR, "standardized")
AI_DIR = os.path.join(DATA_DIR, "ai_ready")

AI = "ai_ready_icu_mortality"
ACTOR = "pipeline_service"

COT_AI = ["patient_hash", "age", "gender", "weight_kg", "heartrate_mean",
          "creatinine_max", "glucose_mean", "icu_mortality"]


def calc_lab_features(lab):
    """Mỗi bệnh nhân một dòng: creatinine cao nhất, glucose trung bình (mg/dL)."""
    from pyspark.sql import functions as F

    ten = F.col("lab_name")
    return (lab.where(ten.isin("creatinine", "glucose"))
            .groupBy("patient_id")
            .agg(F.max(F.when(ten == "creatinine", F.col("lab_value"))).alias("creatinine_max"),
                 F.round(F.avg(F.when(ten == "glucose", F.col("lab_value"))), 2).alias("glucose_mean")))


def build_ai_ready(benh_nhan, xet_nghiem=None, secret=None):
    from pyspark.sql import functions as F

    # Đặc trưng xét nghiệm (bệnh nhân không làm xét nghiệm -> trống)
    if xet_nghiem is not None:
        benh_nhan = benh_nhan.join(calc_lab_features(xet_nghiem), "patient_id", "left")
    else:
        benh_nhan = (benh_nhan.withColumn("creatinine_max", F.lit(None).cast("double"))
                     .withColumn("glucose_mean", F.lit(None).cast("double")))

    # Giả danh patient_id -> patient_hash (HMAC-SHA256, không dò ngược được nếu không có khóa)
    benh_nhan = pseudonymize_spark(benh_nhan, "patient_id", "patient_hash", secret)
    return benh_nhan.select(
        "patient_hash", "age", "gender", "weight_kg",
        F.col("heartrate_mean").cast("double").alias("heartrate_mean"),
        "creatinine_max", "glucose_mean",
        # Nhãn icu_mortality: 1 nếu tử vong trong ICU, còn lại 0 (lỗi pipeline giả lập: đảo nhãn)
        (F.when(F.col("unitdischargestatus") == "Expired", 0).otherwise(1) if loi_pipeline() == "dao_nhan_tu_vong"
         else F.when(F.col("unitdischargestatus") == "Expired", 1).otherwise(0)).alias("icu_mortality"),
    )


def _tao_ai_ready(spark, db_path, std_dir, ai_dir):
    path_p = os.path.join(std_dir, "standardized_patients")
    path_l = os.path.join(std_dir, "standardized_labs")
    if not is_delta_table(path_p):
        print(f"Không tìm thấy bảng Delta {path_p}")
        return None

    # Cổng provenance: đầu vào phải đúng là phiên bản pipeline đã ghi (khớp checksum)
    dau_vao = []
    ma_p = verify_input("standardized_patients", path_p, db_path)
    if ma_p is None:
        print("AI_READY BỊ CHẶN: standardized_patients không truy được nguồn gốc")
        return None
    dau_vao.append(("dataset_version", ma_p))
    benh_nhan = read_table_spark(spark, ACTOR, "standardized_patients", path=path_p, db_path=db_path)

    xet_nghiem = None
    if is_delta_table(path_l):
        ma_l = verify_input("standardized_labs", path_l, db_path)
        if ma_l is None:
            print("AI_READY BỊ CHẶN: standardized_labs không truy được nguồn gốc")
            return None
        dau_vao.append(("dataset_version", ma_l))
        xet_nghiem = read_table_spark(spark, ACTOR, "standardized_labs", path=path_l, db_path=db_path)

    # Giữ trong bộ nhớ: bảng được dùng 2 lần (lập hồ sơ cho cổng, rồi ghi Delta)
    ai = build_ai_ready(benh_nhan, xet_nghiem).cache()
    out = os.path.join(ai_dir, AI)
    try:
        # Check gate: gate1 (schema + hợp đồng + quyền riêng tư) rồi gate2 (chất lượng)
        ho_so = profile_spark(ai, find_contract(AI))
        print(f"Tạo {ho_so['so_dong']} dòng AI-ready từ standardized_patients"
              + (" + standardized_labs" if xet_nghiem is not None else ""))
        for gate in ("gate1", "gate2"):
            dat, loi = check_gate(AI, ho_so, gate, db_path)
            if not dat:
                print(f"AI_READY BỊ CHẶN ở {gate}:")
                print("\n".join(loi))
                return None
        # Code pipeline đổi mà đầu ra lệch so với phiên bản trước -> chặn (governance/pipeline_drift.py)
        dat, lech = check_pipeline_drift(AI, ho_so, dau_vao, db_path)
        if not dat:
            print("AI_READY BỊ CHẶN: đầu ra lệch khi code pipeline đổi:")
            print("\n".join(lech))
            return None

        # Lineage standardized -> ai_ready đã được ghi đủ ở standardize.py (record_sample_lineage)

        # Lưu bảng Delta, ghi provenance
        write_table_spark(spark, ACTOR, AI, ai, path=out, db_path=db_path, so_file=1)
    finally:
        ai.unpersist()
    print(f"Đã lưu bảng Delta {out}")
    record_dataset_version(AI, "ai_ready", out, ho_so["so_dong"], dau_vao, db_path=db_path, ho_so=ho_so)

    # Đăng ký catalog; tầng AI-ready đã giả danh hóa nên hợp đồng không còn cột nhạy cảm
    register_dataset(
        AI,
        hospital="Toàn hệ thống",
        tier="ai_ready",
        owner="Nhóm nghiên cứu AI",
        registered_by=ACTOR,
        db_path=db_path,
        file_path=to_catalog_path(out),
        row_count=ho_so["so_dong"],
        **catalog_metadata(AI),
    )
    # Bảng AI-ready chỉ vài nghìn dòng: trả về dạng pandas cho tiện in / so sánh (đọc bằng deltalake)
    from deltalake import DeltaTable
    return DeltaTable(out).to_pandas()


def main(db_path=None, std_dir=STD_DIR, ai_dir=AI_DIR, spark=None):
    """Tạo bảng AI-ready từ tầng standardized. Trả về DataFrame pandas đã lưu, None nếu bị chặn."""
    os.makedirs(ai_dir, exist_ok=True)
    spark = spark or get_spark()
    with pipeline_run(ACTOR, "tạo tầng AI-ready", db_path):
        return _tao_ai_ready(spark, db_path, std_dir, ai_dir)


if __name__ == "__main__":
    main()
