"""
Mô phỏng nhiều bệnh viện: tách dữ liệu eICU demo (data/01_landing) thành từng bệnh viện
-> data/02_staging/hospital_<hospitalid>/{patient,lab,vitalPeriodic}.csv

Mặc định lấy TẤT CẢ bệnh viện (eICU demo có 186 viện, 2.520 bệnh nhân, khoảng 2 triệu dòng).
Đây là bước giả lập phía bệnh viện (nằm ngoài hệ thống governance) nên dùng pandas cho đơn giản.

Chạy:
    python -m pipeline.split_hospitals            # mọi bệnh viện
    python -m pipeline.split_hospitals --top 3    # chỉ 3 viện đông bệnh nhân nhất
"""

import argparse
import glob
import os
import shutil

import pandas as pd

ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

LANDING_DIR = os.path.join(ROOT, "data", "01_landing")
STAGING_DIR = os.path.join(ROOT, "data", "02_staging")

# Đường dẫn đến các file lab.csv, patient.csv,
LAB_DATA_ORIGINAL = os.path.join(LANDING_DIR, "lab.csv")
PATIENT_DATA_ORIGINAL = os.path.join(LANDING_DIR, "patient.csv")
VITAL_PERIODIC_DATA_ORIGINAL = os.path.join(LANDING_DIR, "vitalPeriodic.csv")


def main(top=None, landing_dir=None, staging_dir=None):
    """
    Tách dữ liệu theo bệnh viện. top=None: mọi viện; top=k: k viện đông bệnh nhân nhất.
    Trả về danh sách hospitalid đã tách (đông bệnh nhân trước).
    """
    landing_dir = landing_dir or LANDING_DIR
    staging_dir = staging_dir or STAGING_DIR

    # BƯỚC 1: Đọc dữ liệu ban đầu
    patient_df = pd.read_csv(os.path.join(landing_dir, "patient.csv"))
    lab_df = pd.read_csv(os.path.join(landing_dir, "lab.csv"))
    vitalPeriodic_df = pd.read_csv(os.path.join(landing_dir, "vitalPeriodic.csv"))

    # BƯỚC 2: Chọn bệnh viện (đông bệnh nhân trước; cùng số bệnh nhân thì mã nhỏ trước)
    so_benh_nhan = patient_df["hospitalid"].value_counts()
    cac_vien = sorted(so_benh_nhan.index, key=lambda h: (-so_benh_nhan[h], h))
    if top:
        cac_vien = cac_vien[:top]
    print(f"Tách {len(cac_vien)} bệnh viện, {int(so_benh_nhan[cac_vien].sum())} bệnh nhân")

    # BƯỚC 3: Xóa kết quả tách cũ (có thể khác số viện) rồi ghi lại
    for cu in glob.glob(os.path.join(staging_dir, "hospital_*")):
        shutil.rmtree(cu)

    # Gắn mã viện cho từng dòng lab / vital qua patientunitstayid, rồi gom nhóm MỘT lần
    vien_cua_bn = patient_df.set_index("patientunitstayid")["hospitalid"]
    nhom = {
        "patient.csv": patient_df.groupby("hospitalid"),
        "lab.csv": lab_df.groupby(lab_df["patientunitstayid"].map(vien_cua_bn)),
        "vitalPeriodic.csv": vitalPeriodic_df.groupby(vitalPeriodic_df["patientunitstayid"].map(vien_cua_bn)),
    }
    cot_goc = {"patient.csv": patient_df.columns, "lab.csv": lab_df.columns,
               "vitalPeriodic.csv": vitalPeriodic_df.columns}
    for hospital_id in cac_vien:
        target_dir = os.path.join(staging_dir, f"hospital_{hospital_id}")
        os.makedirs(target_dir, exist_ok=True)
        for ten_file, gom in nhom.items():
            # Viện không có dòng nào (ví dụ không đo sinh hiệu) vẫn có file, chỉ có dòng tiêu đề
            phan = gom.get_group(hospital_id) if hospital_id in gom.groups else pd.DataFrame(columns=cot_goc[ten_file])
            phan.to_csv(os.path.join(target_dir, ten_file), index=False)

    print(f"✅ Đã lưu {len(cac_vien)} bệnh viện vào {staging_dir}")
    return [str(h) for h in cac_vien]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tách dữ liệu eICU demo theo bệnh viện")
    parser.add_argument("--top", type=int, help="chỉ lấy k bệnh viện đông bệnh nhân nhất")
    main(parser.parse_args().top)
