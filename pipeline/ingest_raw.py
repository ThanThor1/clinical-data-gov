"""
Nạp dữ liệu vào tầng RAW (giữ nguyên bản gốc):
  1. split_snapshots: chia dữ liệu mỗi viện thành 3 đợt gửi
     -> data/raw/hospital_X/<YYYY-MM-DD_HH-MM-SS>/{patient,lab,vitalPeriodic}.csv
  2. Đăng ký 9 bảng raw (3 loại x 3 viện) vào catalog bằng register_dataset;
     cột nhạy cảm, mô tả, hợp đồng lấy từ hợp đồng dữ liệu (contracts/*.yaml).
     Vị trí file là mẫu data/raw/hospital_X/*/<file>.csv (mỗi đợt một thư mục),
     nhờ vậy read_table đọc được toàn bộ các đợt của một bảng.

Chạy: python -m pipeline.ingest_raw   (từ gốc repo)
"""

import glob
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_access import register_dataset
from catalog_access.data_access import to_catalog_path
from governance.contracts import catalog_metadata
from pipeline.split_snapshots import SAVE_DIR as RAW_DIR, split_into_timestamps

HOSPITALS = ["A", "B", "C"]

# Loại bảng raw -> bộ phận phụ trách ở bệnh viện nguồn
OWNER_THEO_LOAI = {
    "patient": "Đội IT",
    "lab": "Khoa xét nghiệm",
    "vital": "Đơn vị ICU",
}

# Loại dữ liệu -> tên file trong thư mục đợt. Tên bảng logic là raw_<loại>_hospital_X
# (khớp hợp đồng contracts/raw_patient.yaml, raw_lab.yaml, raw_vital.yaml).
FILE_THEO_LOAI = {
    "patient": "patient.csv",
    "lab": "lab.csv",
    "vital": "vitalPeriodic.csv",
}


def register_raw_tables(db_path=None, raw_dir=None):
    """Đăng ký 9 bảng raw vào catalog. Trả về danh sách tên bảng đã đăng ký."""
    raw_dir = raw_dir or RAW_DIR
    da_dang_ky = []
    for vien in HOSPITALS:
        for loai, bo_phan in OWNER_THEO_LOAI.items():
            ten_bang = f"raw_{loai}_hospital_{vien}"
            mau_file = os.path.join(raw_dir, f"hospital_{vien}", "*", FILE_THEO_LOAI[loai])
            so_dong = sum(len(pd.read_csv(f, usecols=[0])) for f in glob.glob(mau_file))
            thong_tin = catalog_metadata(ten_bang)
            register_dataset(
                ten_bang,
                hospital=f"Bệnh viện {vien}",
                tier="raw",
                owner=f"{bo_phan} Bệnh viện {vien}",
                registered_by="pipeline_ingest",
                db_path=db_path,
                file_path=to_catalog_path(mau_file),
                row_count=so_dong,
                **thong_tin,
            )
            print(f"  Đã đăng ký {ten_bang:<24} {so_dong:>6} dòng "
                  f"(cột nhạy cảm: {thong_tin['sensitive_columns'] or 'không có'})")
            da_dang_ky.append(ten_bang)
    return da_dang_ky


def main(db_path=None, staging_dir=None, raw_dir=None):
    split_into_timestamps(staging_dir, raw_dir)
    print("\nĐăng ký 9 bảng raw vào catalog:")
    return register_raw_tables(db_path, raw_dir)


if __name__ == "__main__":
    main()
