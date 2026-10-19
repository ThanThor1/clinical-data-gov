import os
import pandas as pd

STAGING_DIR = ""
HOSPITAL_NAMES = ["hospital_A", "hospital_B", "hospital_C"]


def verify():
    print("🔍 BẮT ĐẦU KIỂM TRA DỮ LIỆU SAU KHI TÁCH...\n")

    total_patients = 0
    total_labs = 0
    total_vitals = 0

    for hosp_name in HOSPITAL_NAMES:
        print(f"--- Đang kiểm tra {hosp_name} ---")
        hosp_dir = os.path.join(STAGING_DIR, hosp_name)

        # Đọc dữ liệu
        p_df = pd.read_csv(os.path.join(hosp_dir, "patient.csv"))
        l_df = pd.read_csv(os.path.join(hosp_dir, "lab.csv"))
        v_df = pd.read_csv(os.path.join(hosp_dir, "vitalPeriodic.csv"))

        # 1. Kiểm tra bệnh viện (hospitalid)
        unique_hosp_ids = p_df["hospitalid"].unique()
        if len(unique_hosp_ids) == 1:
            print(f"  ✅ ID bệnh viện đồng nhất: {unique_hosp_ids[0]}")
        else:
            print(f"  ❌ CẢNH BÁO: Lẫn lộn nhiều ID bệnh viện: {unique_hosp_ids}")

        # 2. Kiểm tra chéo ID bệnh nhân (Foreign Key)
        patient_ids = set(p_df["patientunitstayid"].unique())
        lab_ids = set(l_df["patientunitstayid"].unique())
        vital_ids = set(v_df["patientunitstayid"].unique())

        # Tìm những ID có trong lab/vital nhưng KHÔNG có trong patient
        orphan_labs = lab_ids - patient_ids
        orphan_vitals = vital_ids - patient_ids

        if len(orphan_labs) == 0:
            print("  ✅ Toàn bộ xét nghiệm (lab) đều thuộc về bệnh nhân của bệnh viện này.")
        else:
            print(f"  ❌ CẢNH BÁO: Có {len(orphan_labs)} ID bệnh nhân lạ trong file lab!")

        if len(orphan_vitals) == 0:
            print("  ✅ Toàn bộ sinh tồn (vitalPeriodic) đều thuộc về bệnh nhân của bệnh viện này.")
        else:
            print(f"  ❌ CẢNH BÁO: Có {len(orphan_vitals)} ID bệnh nhân lạ trong file vitalPeriodic!")

        # Cộng dồn để check tổng
        total_patients += len(p_df)
        total_labs += len(l_df)
        total_vitals += len(v_df)

        print(f"  ℹ️ Số lượng: {len(p_df)} bệnh nhân, {len(l_df)} lab, {len(v_df)} vital\n")

    print("📊 TỔNG KẾT:")
    print(f"  - Tổng bệnh nhân của 3 viện: {total_patients}")
    print(f"  - Tổng dòng lab của 3 viện: {total_labs}")
    print(f"  - Tổng dòng vital của 3 viện: {total_vitals}")
    print(
        "\n(So sánh số 'Tổng bệnh nhân' này với tổng số dòng của file patient.csv gốc ban đầu xem có bị mất mát gì không).")


if __name__ == "__main__":
    verify()