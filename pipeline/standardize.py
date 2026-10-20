import os
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog_access import register_dataset
from governance.contracts import find_contract, get_sensitive_columns
from governance.gates import check_gate
from governance.lineage import record_sample_lineage

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
STD_DIR = os.path.join(DATA_DIR, "standardized")
os.makedirs(STD_DIR, exist_ok=True)

# Loại dữ liệu -> tên file trong thư mục đợt. Tên bảng logic là raw_<loại>_hospital_X
# (khớp hợp đồng contracts/raw_patient.yaml, raw_lab.yaml, raw_vital.yaml).
FILE_THEO_LOAI = {
    "patient": "patient.csv",
    "lab": "lab.csv",
    "vital": "vitalPeriodic.csv",
}


def load_and_check_gate(hospital, batch_name, data_type, gate):
    """
       Đọc file raw của 1 đợt, 1 loại dữ liệu (patient | lab | vital). Check gate.
       Trả về df nếu đạt, None nếu bị chặn.
    """
    file_path = os.path.join(RAW_DIR, f"hospital_{hospital}", batch_name, FILE_THEO_LOAI[data_type])
    if not os.path.exists(file_path):
        print(f"[BỎ QUA] Không có file {file_path}")
        return None
    df = pd.read_csv(file_path)
    table_name = f"raw_{data_type}_hospital_{hospital}"

    dat, loi = check_gate(table_name, df, gate)
    if not dat:
        print(f"[CHẶN] {table_name} ở {gate}:")
        print("\n".join(loi))
        return None
    return df


def clean_patient(df, hospital):
    df = df.rename(
        columns={
            "patientunitstayid": "patient_id",
            "admissionweight": "weight_kg"
        }
    )

    # age: "> 89" -> 90, ép số, giữ trống
    df["age"] = df["age"].astype(str).str.replace("> 89", "90", regex=False)
    df["age"] = pd.to_numeric(df['age'], errors="coerce")

    # gender
    df['gender'] = df['gender'].map({'Male': 'M', 'Female': 'F'}).fillna('U')
    df['source_hospital'] = hospital
    return df[['patient_id', 'age', 'gender', "weight_kg", "unitdischargestatus", "source_hospital"]]


def clean_lab(df, hospital):
    df = df.rename(columns={
        "patientunitstayid": "patient_id",
        "labname": "lab_name",
        "labresult": "lab_value",
    })
    df["source_hospital"] = hospital
    return df[["patient_id", "lab_name", "lab_value", "source_hospital"]]


def calc_heartrate_mean(df_vital):
    if df_vital is None or df_vital.empty:
        return pd.DataFrame(columns=["patient_id", "heartrate_mean"])
    df = df_vital.rename(columns={
        "patientunitstayid": "patient_id"
    })

    hr = df.groupby("patient_id")["heartrate"].mean().reset_index()
    hr.rename(columns={"heartrate": "heartrate_mean"}, inplace=True)
    return hr


def register_standardized(table_name):
    """Đăng ký bảng tầng standardized vào catalog, cột nhạy cảm lấy từ hợp đồng dữ liệu."""
    hop_dong = find_contract(table_name)
    register_dataset(
        table_name,
        hospital="Toàn hệ thống",
        tier="standardized",
        owner="Nhóm Kỹ thuật dữ liệu",
        sensitive_columns=",".join(get_sensitive_columns(hop_dong)) if hop_dong else "",
        registered_by="pipeline_standardize",
    )


def check_both_gates(table_name, df):
    """Chạy gate1 rồi gate2 cho bảng tầng standardized. Trả về True nếu qua cả hai."""
    for gate in ("gate1", "gate2"):
        dat, loi = check_gate(table_name, df, gate)
        if not dat:
            print(f"[CHẶN] {table_name} ở {gate}:")
            print("\n".join(loi))
            return False
    return True


def main():
    """
    Chuẩn hóa mọi đợt raw đã qua cổng. Trả về dict tóm tắt:
    so_dot, so_dot_dat, bi_chan (danh sách 'bảng / đợt'), so_dong (số dòng standardized_patients
    đã lưu, None nếu bị chặn).
    """
    # Ghi lineage đầy đủ raw (3 viện) -> standardized -> ai_ready TRƯỚC mọi check_gate,
    # để khi có vi phạm, violation_engine tìm được bảng AI-ready bị ảnh hưởng.
    record_sample_lineage()

    all_patients, all_labs = [], []
    ket_qua = {"so_dot": 0, "so_dot_dat": 0, "bi_chan": [], "so_dong": None}

    for hospital in ["A", "B", "C"]:
        hospital_dir = os.path.join(RAW_DIR, f"hospital_{hospital}")
        if not os.path.isdir(hospital_dir):
            continue
        # Sort theo tên = sort theo thời gian gửi
        batches = sorted(os.listdir(hospital_dir))

        for batch in batches:
            print(f"\n--- Viện {hospital} | Đợt {batch} ---")
            ket_qua["so_dot"] += 1
            dot_dat = True

            # Patient
            df_p = load_and_check_gate(hospital, batch, "patient", "gate1")
            if df_p is not None:
                df_p = load_and_check_gate(hospital, batch, "patient", "gate2")
            if df_p is None:
                ket_qua["bi_chan"].append(f"raw_patient_hospital_{hospital} / {batch}")
                continue
            df_p = clean_patient(df_p, hospital)

            # Vital -> heartrate_mean
            df_v = load_and_check_gate(hospital, batch, "vital", "gate1")
            if df_v is not None:
                df_v = load_and_check_gate(hospital, batch, "vital", "gate2")
            if df_v is not None:
                hr = calc_heartrate_mean(df_v)
                df_p = df_p.merge(hr, on="patient_id", how="left")
            else:
                dot_dat = False
                ket_qua["bi_chan"].append(f"raw_vital_hospital_{hospital} / {batch}")
            if "heartrate_mean" not in df_p.columns:
                df_p["heartrate_mean"] = float("nan")
            # Đợt không có vital (viện B) -> merge với bảng rỗng cho cột kiểu object -> ép về float
            df_p["heartrate_mean"] = pd.to_numeric(df_p["heartrate_mean"], errors="coerce").astype(float)

            # Lab
            df_l = load_and_check_gate(hospital, batch, "lab", "gate1")
            if df_l is not None:
                df_l = load_and_check_gate(hospital, batch, "lab", "gate2")
            if df_l is not None:
                all_labs.append(clean_lab(df_l, hospital))
            else:
                dot_dat = False
                ket_qua["bi_chan"].append(f"raw_lab_hospital_{hospital} / {batch}")

            all_patients.append(df_p)
            ket_qua["so_dot_dat"] += int(dot_dat)

    if not all_patients:
        print("Không có dữ liệu nào qua cổng!")
        return ket_qua

    final_p = pd.concat(all_patients, ignore_index=True)
    final_l = pd.concat(all_labs, ignore_index=True) if all_labs else pd.DataFrame()

    # Tầng standardized cũng phải qua cả gate1 (schema) và gate2 (chất lượng)
    if not check_both_gates("standardized_patients", final_p):
        print("STANDARDIZED_PATIENTS BỊ CHẶN, không ghi file.")
        return ket_qua
    if not final_l.empty and not check_both_gates("standardized_labs", final_l):
        print("STANDARDIZED_LABS BỊ CHẶN, không ghi file.")
        final_l = pd.DataFrame()

    # Ghi file và đăng ký catalog (chỉ bảng đã qua cổng)
    final_p.to_csv(os.path.join(STD_DIR, "standardized_patients.csv"), index=False)
    register_standardized("standardized_patients")
    if not final_l.empty:
        final_l.to_csv(os.path.join(STD_DIR, "standardized_labs.csv"), index=False)
        register_standardized("standardized_labs")
    ket_qua["so_dong"] = len(final_p)
    print(f"\nĐã lưu standardized! ({len(final_p)} bệnh nhân)")
    return ket_qua


if __name__ == "__main__":
    main()
