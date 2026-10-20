import os
import hashlib
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from governance.gates import check_gate
from governance.lineage import record_lineage

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
STD_DIR = os.path.join(DATA_DIR, "standardized")
AI_DIR = os.path.join(DATA_DIR, "ai_ready")
os.makedirs(AI_DIR, exist_ok=True)

# Chuỗi bí mật – KHÔNG commit lên Git
SECRET = os.environ.get("PATIENT_HASH_SECRET", "demo_secret_2026")

def hash_patient_id(patient_id):
    raw = f"{patient_id}{SECRET}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def build_ai_ready(df):
    # 1. Tạo nhãn icu_mortality
    df["icu_mortality"] = (df["unitdischargestatus"] == "Expired").astype(int)

    # 2. Giả danh patient_id -> patient_hash
    df["patient_hash"] = df["patient_id"].astype(str).apply(hash_patient_id)

    # 3. Chọn cột theo hợp đồng
    cols = ["patient_hash", "age", "gender", "weight_kg", "heartrate_mean", "icu_mortality"]
    return df[cols]

def main():
    path = os.path.join(STD_DIR, "standardized_patients.csv")
    if not os.path.exists(path):
        print(f"Không tìm thấy {path}")
        return

    df = pd.read_csv(path)
    print(f"Đọc {len(df)} dòng từ standardized_patients")

    ai_df = build_ai_ready(df)

    # Check gate
    dat, loi = check_gate("ai_ready_icu_mortality", ai_df, "gate1")
    if not dat:
        print("AI_READY BỊ CHẶN:")
        print("\n".join(loi))
        return

    # Record lineage
    record_lineage("standardized_patients", "ai_ready_icu_mortality",
                   {"patient_id": "patient_hash",
                    "unitdischargestatus": "icu_mortality"})

    # Lưu
    out = os.path.join(AI_DIR, "ai_ready_icu_mortality.csv")
    ai_df.to_csv(out, index=False)
    print(f"Đã lưu {len(ai_df)} dòng vào {out}")

if __name__ == "__main__":
    main()