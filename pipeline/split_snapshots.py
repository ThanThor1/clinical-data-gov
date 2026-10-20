import os
import shutil
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Đường dẫn cấu hình (tính từ vị trí file, chạy được từ bất kỳ thư mục nào)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING_DIR = os.path.join(ROOT, "data", "02_staging")
SAVE_DIR = os.path.join(ROOT, "data", "raw")
HOSPITALS = ["hospital_A", "hospital_B", "hospital_C"]


def split_into_timestamps():
    print("🚀 Bắt đầu chia dữ liệu thành 3 đợt gửi theo thời gian...")

    # Tạo 3 mốc thời gian giả lập (cách nhau 6 tiếng)
    # Ví dụ: Hôm nay lúc 08:00, 14:00, 20:00
    base_time = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)
    timestamps = [
        base_time.strftime("%Y-%m-%d_%H-%M-%S"),
        (base_time + timedelta(hours=6)).strftime("%Y-%m-%d_%H-%M-%S"),
        (base_time + timedelta(hours=12)).strftime("%Y-%m-%d_%H-%M-%S")
    ]
    print(f"Các mốc thời gian sẽ tạo: {timestamps}\n")

    for hosp in HOSPITALS:
        print(f"--- Đang xử lý {hosp} ---")
        hosp_dir = os.path.join(STAGING_DIR, hosp)
        save_dir = os.path.join(SAVE_DIR, hosp)

        # 0. Xóa các đợt cũ của viện (sinh ra từ lần chạy trước, có thể khác ngày)
        #    để data/raw/hospital_X chỉ còn đúng 3 đợt của lần chia này
        if os.path.isdir(save_dir):
            shutil.rmtree(save_dir)

        # 1. Đọc 3 file dữ liệu gốc của bệnh viện này
        p_df = pd.read_csv(os.path.join(hosp_dir, "patient.csv"))
        l_df = pd.read_csv(os.path.join(hosp_dir, "lab.csv"))
        v_df = pd.read_csv(os.path.join(hosp_dir, "vitalPeriodic.csv"))

        # 2. Lấy danh sách ID bệnh nhân duy nhất và chia làm 3
        # Dùng unique() để không bị trùng ID
        all_ids = p_df["patientunitstayid"].unique()

        # Nếu bệnh viện có ít hơn 3 bệnh nhân, array_split vẫn hoạt động (sẽ có mảng rỗng)
        split_ids = np.array_split(all_ids, 3)

        # 3. Lặp qua 3 phần và lưu vào thư mục timestamp tương ứng
        for i, batch_ids in enumerate(split_ids):
            if len(batch_ids) == 0:
                print(f"  ⚠️ Đợt {i + 1} không có bệnh nhân nào, bỏ qua.")
                continue

            time_folder = timestamps[i]
            target_dir = os.path.join(save_dir, time_folder)
            os.makedirs(target_dir, exist_ok=True)

            # Lọc dữ liệu dựa trên danh sách ID của đợt này
            p_batch = p_df[p_df["patientunitstayid"].isin(batch_ids)]
            l_batch = l_df[l_df["patientunitstayid"].isin(batch_ids)]
            v_batch = v_df[v_df["patientunitstayid"].isin(batch_ids)]

            # Lưu ra file
            p_batch.to_csv(os.path.join(target_dir, "patient.csv"), index=False)
            l_batch.to_csv(os.path.join(target_dir, "lab.csv"), index=False)
            v_batch.to_csv(os.path.join(target_dir, "vitalPeriodic.csv"), index=False)

            print(
                f"  ✅ Đợt {i + 1} ({time_folder}): {len(p_batch)} bệnh nhân, {len(l_batch)} lab, {len(v_batch)} vital")

    print("\n🎉 Hoàn tất chia dữ liệu theo thời gian!")


if __name__ == "__main__":
    split_into_timestamps()