import os
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from governance.gates import check_gate
from governance.lineage import record_lineage

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
STD_DIR = os.path.join(DATA_DIR, "standardized")
os.makedirs(STD_DIR, exist_ok=True)


def load_and_check_gate(hospital, batch_name, data_type, gate):
    """
       Đọc file raw của 1 đợt, 1 loại dữ liệu. Check gate.
       Trả về df nếu đạt, None nếu bị chặn.
    """
    file_path = os.path.join(RAW_DIR, f"hospital_{hospital}", batch_name, f"{data_type}.csv")
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


def main():
    all_patients, all_labs = [], []

    for hospital in ["A", "B", "C"]:
        hospital_dir = os.path.join(RAW_DIR, f"hospital_{hospital}")
        if not os.path.isdir(hospital_dir):
            continue
        # Sort theo tên = sort theo thời gian gửi
        batches = sorted(os.listdir(hospital_dir))

        for batch in batches:
            print(f"\n--- Viện {hospital} | Đợt {batch} ---")

            # Patient
            df_p = load_and_check_gate(hospital, batch, "patient", "gate1")
            if df_p is None: continue
            df_p = load_and_check_gate(hospital, batch, "patient", "gate2")
            if df_p is None: continue
            df_p = clean_patient(df_p, hospital)

            # Vital -> heartrate_mean
            df_v = load_and_check_gate(hospital, batch, "vitalPeriodic", "gate1")
            if df_v is not None:
                df_v = load_and_check_gate(hospital, batch, "vitalPeriodic", "gate2")
                if df_v is not None:
                    hr = calc_heartrate_mean(df_v)
                    df_p = df_p.merge(hr, on="patient_id", how="left")
            else:
                df_p["heartrate_mean"] = pd.NA

            # Lab
            df_l = load_and_check_gate(hospital, batch, "lab", "gate1")
            if df_l is not None:
                df_l = load_and_check_gate(hospital, batch, "lab", "gate2")
                if df_l is not None:
                    df_l = clean_lab(df_l, hospital)
                    all_labs.append(df_l)

            all_patients.append(df_p)

    if not all_patients:
        print("Không có dữ liệu nào qua cổng!")
        return

    final_p = pd.concat(all_patients, ignore_index=True)
    final_l = pd.concat(all_labs, ignore_index=True) if all_labs else pd.DataFrame()

    # Check gate standardized
    dat, loi = check_gate("standardized_patients", final_p, "gate1")
    if not dat:
        print("STANDARDIZED_PATIENTS BỊ CHẶN:");
        print("\n".join(loi));
        return

    # Record lineage (ví dụ cho A; bạn copy đủ cho B, C từ record_sample_lineage)
    record_lineage("raw_patient_hospital_A", "standardized_patients",
                   {"patientunitstayid": "patient_id", "admissionweight": "weight_kg"})

    # Ghi file
    final_p.to_csv(os.path.join(STD_DIR, "standardized_patients.csv"), index=False)
    if not final_l.empty:
        final_l.to_csv(os.path.join(STD_DIR, "standardized_labs.csv"), index=False)
    print("\nĐã lưu standardized!")


if __name__ == "__main__":
    main()
