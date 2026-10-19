import os
import pandas as pd

LANDING_DIR = "../data/01_landing/"
STAGING_DIR = "../data/02_staging/"

# Đường dẫn đến các file lab.csv, patient.csv,
LAB_DATA_ORIGINAL = LANDING_DIR + "lab.csv"
PATIENT_DATA_ORIGINAL = LANDING_DIR + "patient.csv"
VITAL_PERIODIC_DATA_ORIGINAL = LANDING_DIR + "vitalPeriodic.csv"

HOSPITAL_NAMES = ["hospital_A", "hospital_B", "hospital_C"]


def main():
    # BƯỚC 1: Đọc dữ liệu ban đầu
    patient_df = pd.read_csv(PATIENT_DATA_ORIGINAL)
    lab_df = pd.read_csv(LAB_DATA_ORIGINAL)
    vitalPeriodic_df = pd.read_csv(VITAL_PERIODIC_DATA_ORIGINAL)

    # BƯỚC 2: Tìm 3 bệnh viện đông bệnh nhân nhất
    top_3_hospital_ids = patient_df["hospitalid"].value_counts().head(3).index.tolist()
    print("Ba bệnh viên đông bệnh nhân nhất: ", top_3_hospital_ids)

    # BƯỚC 3: Lặp qua từng bệnh viện và tách dữ liệu
    for hospital_id, hospital_name in zip(top_3_hospital_ids, HOSPITAL_NAMES):
        print(f"\n--- Đang xử lý {hospital_name} (ID: {hospital_id}) ---")

        target_dir = os.path.join(STAGING_DIR,hospital_name)
        os.makedirs(target_dir, exist_ok=True)

        # 3.2. Lọc bảng patient của bệnh viện này
        patient_subset = patient_df[patient_df["hospitalid"]==hospital_id]

        # 3.3. Lấy danh sách các patientunitstayid của bệnh viện này
        valid_stay_ids = patient_subset["patientunitstayid"].unique()

        # 3.4. Lọc bảng lab và vital dựa trên valid_stay_ids
        lab_subset = lab_df[lab_df["patientunitstayid"].isin(valid_stay_ids)]
        vital_subset = vitalPeriodic_df[vitalPeriodic_df["patientunitstayid"].isin(valid_stay_ids)]

        # 3.5. Lưu 3 file ra thư mục staging
        patient_subset.to_csv(os.path.join(target_dir, "patient.csv"), index=False)
        lab_subset.to_csv(os.path.join(target_dir, "lab.csv"), index=False)
        vital_subset.to_csv(os.path.join(target_dir, "vitalPeriodic.csv"), index=False)

        print(f"✅ Đã lưu dữ liệu {hospital_id} vào {target_dir}")

    print("\n🎉 Hoàn tất tách dữ liệu!")


if __name__ == "__main__":
    main()


