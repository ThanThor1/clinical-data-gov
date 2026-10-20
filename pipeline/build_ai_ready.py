import os
import hashlib
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_access import register_dataset
from governance.contracts import find_contract, get_sensitive_columns
from governance.gates import check_gate

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
    """Tạo bảng AI-ready từ standardized_patients. Trả về DataFrame đã lưu, None nếu bị chặn."""
    path = os.path.join(STD_DIR, "standardized_patients.csv")
    if not os.path.exists(path):
        print(f"Không tìm thấy {path}")
        return None

    df = pd.read_csv(path)
    print(f"Đọc {len(df)} dòng từ standardized_patients")

    ai_df = build_ai_ready(df)
    # Đợt không có vital (viện B) có thể làm cột này bị đọc thành chuỗi -> ép về float
    ai_df["heartrate_mean"] = pd.to_numeric(ai_df["heartrate_mean"], errors="coerce").astype(float)

    # Check gate: gate1 (schema + hợp đồng) rồi gate2 (chất lượng)
    for gate in ("gate1", "gate2"):
        dat, loi = check_gate("ai_ready_icu_mortality", ai_df, gate)
        if not dat:
            print(f"AI_READY BỊ CHẶN ở {gate}:")
            print("\n".join(loi))
            return None

    # Lineage standardized -> ai_ready đã được ghi đủ ở standardize.py (record_sample_lineage)

    # Lưu
    out = os.path.join(AI_DIR, "ai_ready_icu_mortality.csv")
    ai_df.to_csv(out, index=False)
    print(f"Đã lưu {len(ai_df)} dòng vào {out}")

    # Đăng ký catalog; tầng AI-ready đã giả danh hóa nên hợp đồng không còn cột nhạy cảm
    hop_dong = find_contract("ai_ready_icu_mortality")
    register_dataset(
        "ai_ready_icu_mortality",
        hospital="Toàn hệ thống",
        tier="ai_ready",
        owner="Nhóm nghiên cứu AI",
        sensitive_columns=",".join(get_sensitive_columns(hop_dong)) if hop_dong else "",
        registered_by="pipeline_build_ai_ready",
    )
    return ai_df

if __name__ == "__main__":
    main()