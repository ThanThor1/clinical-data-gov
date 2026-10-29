"""
Tạo tầng AI-READY: standardized_patients (+ standardized_labs) -> ai_ready_icu_mortality.

  1. Cổng provenance: chỉ dùng file standardized do pipeline tạo ra (checksum khớp một phiên bản
     đã ghi trong dataset_versions). File bị sửa tay -> chặn, vi phạm PROVENANCE_UNVERIFIED.
  2. Giả danh patient_id -> patient_hash bằng HMAC-SHA256 (khóa bí mật, governance/privacy.py).
  3. Đặc trưng xét nghiệm: creatinine_max, glucose_mean theo từng bệnh nhân.
  4. gate1 (schema + hợp đồng + quyền riêng tư) rồi gate2 (chất lượng).
  5. Ghi file qua write_table, ghi phiên bản (đầu vào = 2 phiên bản standardized), đăng ký catalog.

Chạy: python -m pipeline.build_ai_ready   (từ gốc repo)
"""

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_access import read_table, register_dataset, write_table
from catalog_access.data_access import to_catalog_path
from governance.contracts import catalog_metadata
from governance.gates import check_gate
from governance.privacy import pseudonymize_series
from governance.provenance import pipeline_run, record_dataset_version, verify_input

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
STD_DIR = os.path.join(DATA_DIR, "standardized")
AI_DIR = os.path.join(DATA_DIR, "ai_ready")

AI = "ai_ready_icu_mortality"
ACTOR = "pipeline_service"

COT_AI = ["patient_hash", "age", "gender", "weight_kg", "heartrate_mean",
          "creatinine_max", "glucose_mean", "icu_mortality"]


def calc_lab_features(df_labs):
    """Mỗi bệnh nhân một dòng: creatinine cao nhất, glucose trung bình (mg/dL)."""
    if df_labs is None or df_labs.empty:
        return pd.DataFrame(columns=["patient_id", "creatinine_max", "glucose_mean"])
    lab = df_labs.assign(lab_value=pd.to_numeric(df_labs["lab_value"], errors="coerce"))
    dac_trung = pd.DataFrame({
        "creatinine_max": lab[lab["lab_name"] == "creatinine"].groupby("patient_id")["lab_value"].max(),
        "glucose_mean": lab[lab["lab_name"] == "glucose"].groupby("patient_id")["lab_value"].mean().round(2),
    })
    return dac_trung.rename_axis("patient_id").reset_index()


def build_ai_ready(df, df_labs=None, secret=None):
    df = df.copy()
    # 1. Tạo nhãn icu_mortality
    df["icu_mortality"] = (df["unitdischargestatus"] == "Expired").astype(int)

    # 2. Giả danh patient_id -> patient_hash (HMAC-SHA256, không dò ngược được nếu không có khóa)
    df["patient_hash"] = pseudonymize_series(df["patient_id"], secret)

    # 3. Đặc trưng xét nghiệm (bệnh nhân không làm xét nghiệm -> trống)
    dac_trung = calc_lab_features(df_labs)
    if dac_trung.empty:
        df["creatinine_max"] = float("nan")
        df["glucose_mean"] = float("nan")
    else:
        df = df.merge(dac_trung, on="patient_id", how="left")

    # 4. Chọn cột theo hợp đồng (patient_id KHÔNG đi tiếp lên tầng AI-ready)
    return df[COT_AI]


def _tao_ai_ready(db_path, std_dir, ai_dir):
    path_p = os.path.join(std_dir, "standardized_patients.csv")
    path_l = os.path.join(std_dir, "standardized_labs.csv")
    if not os.path.exists(path_p):
        print(f"Không tìm thấy {path_p}")
        return None

    # Cổng provenance: đầu vào phải đúng là file pipeline đã tạo (khớp checksum đã ghi)
    dau_vao = []
    ma_p = verify_input("standardized_patients", path_p, db_path)
    if ma_p is None:
        print("AI_READY BỊ CHẶN: standardized_patients không truy được nguồn gốc")
        return None
    dau_vao.append(("dataset_version", ma_p))
    df = read_table(ACTOR, "standardized_patients", path=path_p, db_path=db_path)

    df_l = None
    if os.path.exists(path_l):
        ma_l = verify_input("standardized_labs", path_l, db_path)
        if ma_l is None:
            print("AI_READY BỊ CHẶN: standardized_labs không truy được nguồn gốc")
            return None
        dau_vao.append(("dataset_version", ma_l))
        df_l = read_table(ACTOR, "standardized_labs", path=path_l, db_path=db_path)
    print(f"Đọc {len(df)} dòng từ standardized_patients"
          + (f", {len(df_l)} dòng từ standardized_labs" if df_l is not None else ""))

    ai_df = build_ai_ready(df, df_l)
    # Đợt không có vital (viện B) có thể làm cột này bị đọc thành chuỗi -> ép về float
    ai_df["heartrate_mean"] = pd.to_numeric(ai_df["heartrate_mean"], errors="coerce").astype(float)

    # Check gate: gate1 (schema + hợp đồng + quyền riêng tư) rồi gate2 (chất lượng)
    for gate in ("gate1", "gate2"):
        dat, loi = check_gate(AI, ai_df, gate, db_path)
        if not dat:
            print(f"AI_READY BỊ CHẶN ở {gate}:")
            print("\n".join(loi))
            return None

    # Lineage standardized -> ai_ready đã được ghi đủ ở standardize.py (record_sample_lineage)

    # Lưu, ghi provenance
    out = os.path.join(ai_dir, f"{AI}.csv")
    write_table(ACTOR, AI, ai_df, path=out, db_path=db_path)
    print(f"Đã lưu {len(ai_df)} dòng vào {out}")
    record_dataset_version(AI, "ai_ready", out, len(ai_df), dau_vao, db_path=db_path)

    # Đăng ký catalog; tầng AI-ready đã giả danh hóa nên hợp đồng không còn cột nhạy cảm
    register_dataset(
        AI,
        hospital="Toàn hệ thống",
        tier="ai_ready",
        owner="Nhóm nghiên cứu AI",
        registered_by=ACTOR,
        db_path=db_path,
        file_path=to_catalog_path(out),
        row_count=len(ai_df),
        **catalog_metadata(AI),
    )
    return ai_df


def main(db_path=None, std_dir=STD_DIR, ai_dir=AI_DIR):
    """Tạo bảng AI-ready từ tầng standardized. Trả về DataFrame đã lưu, None nếu bị chặn."""
    os.makedirs(ai_dir, exist_ok=True)
    with pipeline_run(ACTOR, "tạo tầng AI-ready", db_path):
        return _tao_ai_ready(db_path, std_dir, ai_dir)


if __name__ == "__main__":
    main()
