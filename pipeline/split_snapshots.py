import glob
import os
import shutil
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Đường dẫn cấu hình (tính từ vị trí file, chạy được từ bất kỳ thư mục nào)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING_DIR = os.path.join(ROOT, "data", "02_staging")
SAVE_DIR = os.path.join(ROOT, "data", "raw")


def ma_vien(ten_thu_muc):
    """'hospital_146' -> '146'."""
    return os.path.basename(ten_thu_muc)[len("hospital_"):]


def sap_xep_vien(cac_ma):
    """Sắp xếp mã viện: số theo giá trị (73 trước 146), chữ (A, B, C của dữ liệu giả) theo bảng chữ cái."""
    return sorted(cac_ma, key=lambda m: (0, int(m), "") if str(m).isdigit() else (1, 0, str(m)))


def danh_sach_vien(thu_muc_goc):
    """Mã các bệnh viện có thư mục hospital_<mã> trong thư mục gốc."""
    return sap_xep_vien(ma_vien(d) for d in glob.glob(os.path.join(thu_muc_goc, "hospital_*")) if os.path.isdir(d))


def split_into_timestamps(staging_dir=None, save_root=None, in_chi_tiet=None):
    """
    Chia dữ liệu mỗi viện (staging_dir/hospital_X) thành 3 đợt, ghi vào save_root/hospital_X/<thời điểm>/.
    Mọi thư mục hospital_* cũ trong save_root bị xóa trước (để không lẫn viện / đợt của lần chia trước).
    """
    staging_dir = staging_dir or STAGING_DIR
    save_root = save_root or SAVE_DIR
    cac_vien = danh_sach_vien(staging_dir)
    if in_chi_tiet is None:
        in_chi_tiet = len(cac_vien) <= 10
    print(f"🚀 Bắt đầu chia dữ liệu của {len(cac_vien)} bệnh viện thành 3 đợt gửi theo thời gian...")
    for cu in glob.glob(os.path.join(save_root, "hospital_*")):
        shutil.rmtree(cu)

    # Tạo 3 mốc thời gian giả lập (cách nhau 6 tiếng)
    # Ví dụ: Hôm nay lúc 08:00, 14:00, 20:00
    base_time = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)
    timestamps = [
        base_time.strftime("%Y-%m-%d_%H-%M-%S"),
        (base_time + timedelta(hours=6)).strftime("%Y-%m-%d_%H-%M-%S"),
        (base_time + timedelta(hours=12)).strftime("%Y-%m-%d_%H-%M-%S")
    ]
    print(f"Các mốc thời gian sẽ tạo: {timestamps}\n")

    so_dot = 0
    for ma in cac_vien:
        hosp = f"hospital_{ma}"
        if in_chi_tiet:
            print(f"--- Đang xử lý {hosp} ---")
        hosp_dir = os.path.join(staging_dir, hosp)
        save_dir = os.path.join(save_root, hosp)

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
                if in_chi_tiet:
                    print(f"  ⚠️ Đợt {i + 1} không có bệnh nhân nào, bỏ qua.")
                continue
            so_dot += 1

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

            if in_chi_tiet:
                print(f"  ✅ Đợt {i + 1} ({time_folder}): {len(p_batch)} bệnh nhân, {len(l_batch)} lab, "
                      f"{len(v_batch)} vital")

    print(f"\n🎉 Hoàn tất: {len(cac_vien)} bệnh viện, {so_dot} đợt gửi")
    return cac_vien


if __name__ == "__main__":
    split_into_timestamps()