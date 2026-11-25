"""
Tạo tầng AI-READY bằng Apache Spark, hai bảng:
  - ai_ready_icu_mortality : standardized_patients (+ standardized_labs) -> 6 đặc trưng, nhãn tử vong lúc ra ICU
  - ai_ready_mortality_24h : standardized_patients + standardized_vitals + standardized_labs -> 23 đặc trưng của
    24 giờ đầu nằm ICU, nhãn tử vong trong bệnh viện; đúng đầu vào của mô hình XGBoost của nhóm AI
    (intelligence/ml_mortality/, xem build_mortality_24h)
Mỗi bảng đi qua các bước:

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
from governance.lineage import COT_XET_NGHIEM_24H
from governance.gates import check_gate
from governance.pipeline_drift import check_pipeline_drift
from governance.privacy import pseudonymize_spark
from governance.profiling import profile_spark
from governance.provenance import pipeline_run, record_dataset_version, verify_input
from governance.sources import load_sources
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

# ----- Bảng cho mô hình dự đoán tử vong trong bệnh viện từ 24 giờ đầu (intelligence/ml_mortality) -----
AI_24H = "ai_ready_mortality_24h"
CUA_SO_PHUT = 24 * 60
TUOI_TOI_THIEU = 18
NAM_ICU_TOI_THIEU_PHUT = 240

COT_SINH_HIEU_24H = ("heartrate_mean", "heartrate_min", "heartrate_max", "systemicmean_max", "sao2_mean", "sao2_min",
                     "respiration_mean", "respiration_max", "temperature_mean")
# 23 đặc trưng, đúng thứ tự của notebook merge_features (mô hình đọc theo tên cột)
DAC_TRUNG_24H = ["age", "male_sex", "bmi", "gcs", *COT_SINH_HIEU_24H, *COT_XET_NGHIEM_24H]
COT_AI_24H = ["patient_hash", "source_system", *DAC_TRUNG_24H, "mortality"]

# Tên xét nghiệm (chữ thường, bỏ khoảng trắng hai đầu) -> nhóm (giống LAB_MAP của notebook process_lab)
TEN_XET_NGHIEM_24H = {
    "creatinine": "creatinine", "bun": "bun", "lactate": "lactate",
    "wbc": "wbc", "wbc x 1000": "wbc", "hemoglobin": "hemoglobin", "hgb": "hemoglobin",
    "platelets": "platelets", "platelets x 1000": "platelets", "bilirubin": "bilirubin",
    "total bilirubin": "bilirubin", "glucose": "glucose", "bedside glucose": "glucose",
}


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


def source_system_col(cot_vien="source_hospital"):
    """Cột hệ thống nguồn (tên trong sources/*.yaml) suy từ mã viện gửi dữ liệu."""
    from pyspark.sql import functions as F

    cac_nguon = load_sources()
    mac_dinh = next(n["name"] for n in cac_nguon if n["hospitals"] == "default")
    ket_qua = F.lit(mac_dinh)
    for nguon in cac_nguon:
        if nguon["hospitals"] != "default":
            ket_qua = F.when(F.col(cot_vien).isin(nguon["hospitals"]), F.lit(nguon["name"])).otherwise(ket_qua)
    return ket_qua


def _trong_khoang(cot, thap=None, cao=None):
    """Giá trị ngoài [thap, cao] -> trống."""
    from pyspark.sql import functions as F

    ngoai = F.lit(False)
    if thap is not None:
        ngoai = ngoai | (cot < thap)
    if cao is not None:
        ngoai = ngoai | (cot > cao)
    return F.when(ngoai, F.lit(None).cast("double")).otherwise(cot)


def calc_vital_features_24h(sinh_hieu):
    """9 đặc trưng sinh hiệu của 24 giờ đầu (0 <= observation_offset <= 1440), mỗi bệnh nhân một dòng."""
    from pyspark.sql import functions as F

    return (sinh_hieu.where(F.col("observation_offset").between(0, CUA_SO_PHUT))
            .groupBy("patient_id")
            .agg(F.avg("heartrate").alias("heartrate_mean"), F.min("heartrate").alias("heartrate_min"),
                 F.max("heartrate").alias("heartrate_max"), F.max("systemicmean").alias("systemicmean_max"),
                 F.avg("sao2").alias("sao2_mean"), F.min("sao2").alias("sao2_min"),
                 F.avg("respiration").alias("respiration_mean"), F.max("respiration").alias("respiration_max"),
                 F.avg("temperature").alias("temperature_mean")))


def calc_lab_features_24h(lab):
    """10 đặc trưng xét nghiệm của 24 giờ đầu (0 <= lab_offset <= 1440), mỗi bệnh nhân một dòng."""
    from pyspark.sql import functions as F

    ten = F.lower(F.trim(F.col("lab_name")))
    nhom = F.lit(None).cast("string")
    for ten_goc, ten_nhom in TEN_XET_NGHIEM_24H.items():
        nhom = F.when(ten == ten_goc, F.lit(ten_nhom)).otherwise(nhom)
    gia_tri = F.col("lab_value")
    ham = {"max": F.max, "mean": F.avg}
    ket_qua = (lab.where(F.col("lab_offset").between(0, CUA_SO_PHUT) & gia_tri.isNotNull())
               .withColumn("_nhom", nhom).where(F.col("_nhom").isNotNull())
               .groupBy("patient_id")
               .agg(*[ham[c.rsplit("_", 1)[1]](F.when(F.col("_nhom") == c.rsplit("_", 1)[0], gia_tri)).alias(c)
                      for c in COT_XET_NGHIEM_24H]))
    # Bạch cầu trung bình > 100 (x1000/µL) là lỗi nhập liệu -> trống (như notebook process_lab)
    return ket_qua.withColumn("wbc_mean", _trong_khoang(F.col("wbc_mean"), cao=100.0))


def build_mortality_24h(benh_nhan, sinh_hieu=None, xet_nghiem=None, secret=None):
    """
    Bảng ai_ready_mortality_24h: cohort (tuổi >= 18, nằm ICU >= 240 phút hoặc nguồn không ghi, có nhãn bệnh viện),
    23 đặc trưng của 24 giờ đầu, nhãn mortality. Giá trị thiếu để trống (mô hình XGBoost tự xử lý), không điền.
    """
    from pyspark.sql import functions as F

    tinh_trang = F.col("hospitaldischargestatus")
    nam_icu = F.col("icu_los_min")
    cohort = benh_nhan.where((F.col("age") >= TUOI_TOI_THIEU)
                             & (nam_icu.isNull() | (nam_icu >= NAM_ICU_TOI_THIEU_PHUT))
                             & tinh_trang.isin("Alive", "Expired"))

    chieu_cao_m = F.col("height_cm") / 100
    bmi = _trong_khoang(_trong_khoang(F.col("weight_kg"), 20.0, 300.0) / (chieu_cao_m * chieu_cao_m), 10.0, 80.0)
    cohort = cohort.select(
        "patient_id", source_system_col().alias("source_system"), "age",
        F.when(F.col("gender") == "M", 1).when(F.col("gender") == "F", 0).alias("male_sex"),
        bmi.alias("bmi"), F.col("gcs").cast("int").alias("gcs"),
        F.when(tinh_trang == "Expired", 1).otherwise(0).alias("mortality"),
    )

    if sinh_hieu is not None:
        cohort = cohort.join(calc_vital_features_24h(sinh_hieu), "patient_id", "left")
    if xet_nghiem is not None:
        cohort = cohort.join(calc_lab_features_24h(xet_nghiem), "patient_id", "left")
    for cot in (*COT_SINH_HIEU_24H, *COT_XET_NGHIEM_24H):
        if cot not in cohort.columns:
            cohort = cohort.withColumn(cot, F.lit(None).cast("double"))

    cohort = pseudonymize_spark(cohort, "patient_id", "patient_hash", secret)
    return cohort.select(
        "patient_hash", "source_system", F.col("age").cast("int").alias("age"), "male_sex", "bmi", "gcs",
        *[F.col(c).cast("double").alias(c) for c in (*COT_SINH_HIEU_24H, *COT_XET_NGHIEM_24H)], "mortality")


def _kiem_tra_va_ghi(spark, db_path, ten_bang, bang, dau_vao, out, mo_ta_dau_vao):
    """gate1 + gate2 + lệch đầu ra khi code đổi, rồi ghi Delta, ghi phiên bản, đăng ký catalog. Trả về hồ sơ hoặc None."""
    ho_so = profile_spark(bang, find_contract(ten_bang))
    print(f"Tạo {ho_so['so_dong']} dòng {ten_bang} từ {mo_ta_dau_vao}")
    for gate in ("gate1", "gate2"):
        dat, loi = check_gate(ten_bang, ho_so, gate, db_path)
        if not dat:
            print(f"{ten_bang.upper()} BỊ CHẶN ở {gate}:")
            print("\n".join(loi))
            return None
    dat, lech = check_pipeline_drift(ten_bang, ho_so, dau_vao, db_path)
    if not dat:
        print(f"{ten_bang.upper()} BỊ CHẶN: đầu ra lệch khi code pipeline đổi:")
        print("\n".join(lech))
        return None
    write_table_spark(spark, ACTOR, ten_bang, bang, path=out, db_path=db_path, so_file=1)
    print(f"Đã lưu bảng Delta {out}")
    record_dataset_version(ten_bang, "ai_ready", out, ho_so["so_dong"], dau_vao, db_path=db_path, ho_so=ho_so)
    register_dataset(ten_bang, hospital="Toàn hệ thống", tier="ai_ready", owner="Nhóm nghiên cứu AI",
                     registered_by=ACTOR, db_path=db_path, file_path=to_catalog_path(out),
                     row_count=ho_so["so_dong"], **catalog_metadata(ten_bang))
    return ho_so


def _doc_dau_vao(spark, db_path, std_dir, ten_bang, bat_buoc, dau_vao, bang_ai):
    """
    Đọc một bảng standardized qua cổng provenance (checksum phải khớp một phiên bản pipeline đã ghi).
    Trả về (DataFrame hoặc None, đạt?). Bảng không bắt buộc chưa có -> (None, True).
    """
    duong_dan = os.path.join(std_dir, ten_bang)
    if not is_delta_table(duong_dan):
        if bat_buoc:
            print(f"Không tìm thấy bảng Delta {duong_dan}")
        return None, not bat_buoc
    ma = verify_input(ten_bang, duong_dan, db_path)
    if ma is None:
        print(f"{bang_ai.upper()} BỊ CHẶN: {ten_bang} không truy được nguồn gốc")
        return None, False
    dau_vao.append(("dataset_version", ma))
    return read_table_spark(spark, ACTOR, ten_bang, path=duong_dan, db_path=db_path), True


def _tao_mortality_24h(spark, db_path, std_dir, ai_dir):
    """Tạo ai_ready_mortality_24h. Trả về DataFrame pandas đã lưu, None nếu bị chặn / thiếu đầu vào."""
    dau_vao = []
    doc = {}
    for ten_bang, bat_buoc in (("standardized_patients", True), ("standardized_vitals", False),
                               ("standardized_labs", False)):
        doc[ten_bang], dat = _doc_dau_vao(spark, db_path, std_dir, ten_bang, bat_buoc, dau_vao, AI_24H)
        if not dat:
            return None
    thieu = {"height_cm", "icu_los_min", "hospitaldischargestatus", "gcs"} - set(doc["standardized_patients"].columns)
    if thieu:
        print(f"Bỏ qua {AI_24H}: standardized_patients chưa có cột {sorted(thieu)} (chạy lại pipeline.standardize)")
        return None
    bang = build_mortality_24h(doc["standardized_patients"], doc["standardized_vitals"],
                               doc["standardized_labs"]).cache()
    out = os.path.join(ai_dir, AI_24H)
    try:
        ho_so = _kiem_tra_va_ghi(spark, db_path, AI_24H, bang, dau_vao, out,
                                 " + ".join(t for t, df in doc.items() if df is not None))
    finally:
        bang.unpersist()
    if ho_so is None:
        return None
    from deltalake import DeltaTable
    return DeltaTable(out).to_pandas()


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


def main(db_path=None, std_dir=STD_DIR, ai_dir=AI_DIR, spark=None, ket_qua_24h=None):
    """
    Tạo hai bảng AI-ready từ tầng standardized (cùng một lần chạy pipeline). Trả về DataFrame pandas của
    ai_ready_icu_mortality, None nếu bị chặn. Truyền dict `ket_qua_24h` để nhận thêm bảng ai_ready_mortality_24h
    (khóa "df": DataFrame pandas, None nếu bị chặn).
    """
    os.makedirs(ai_dir, exist_ok=True)
    spark = spark or get_spark()
    with pipeline_run(ACTOR, "tạo tầng AI-ready", db_path):
        ket_qua = _tao_ai_ready(spark, db_path, std_dir, ai_dir)
        bang_24h = _tao_mortality_24h(spark, db_path, std_dir, ai_dir)
        if ket_qua_24h is not None:
            ket_qua_24h["df"] = bang_24h
        return ket_qua


if __name__ == "__main__":
    main()
