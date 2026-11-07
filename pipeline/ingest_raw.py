"""
Nạp dữ liệu vào tầng RAW (giữ nguyên bản gốc như bệnh viện gửi lên):
  1. split_snapshots: chia dữ liệu mỗi viện thành 3 đợt gửi
     -> data/raw/hospital_<mã>/<YYYY-MM-DD_HH-MM-SS>/{patient,lab,vitalPeriodic}.csv
  2. Đăng ký 3 bảng raw cho mỗi viện (186 viện eICU + nguồn khác) vào catalog bằng register_dataset;
     cột nhạy cảm, mô tả, hợp đồng lấy từ hợp đồng dữ liệu (contracts/*.yaml), hệ thống nguồn và định dạng
     gốc lấy từ sổ đăng ký nguồn (sources/*.yaml).
     Vị trí file là mẫu data/raw/hospital_<mã>/*/<file>.csv (mỗi đợt một thư mục),
     nhờ vậy read_table đọc được toàn bộ các đợt của một bảng.

Chạy: python -m pipeline.ingest_raw   (từ gốc repo)
"""

import glob
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_access import register_dataset
from catalog_access.data_access import to_catalog_path
from governance.contracts import catalog_metadata
from governance.sources import source_of_hospital
from pipeline.split_snapshots import SAVE_DIR as RAW_DIR, danh_sach_vien, split_into_timestamps

# Bản sao lưu của inject_errors chỉ đúng với lần chia đợt hiện tại -> chia lại thì xóa
BACKUP_DIR = os.path.join(os.path.dirname(RAW_DIR), "_backup_inject")

# Loại bảng raw -> bộ phận phụ trách ở bệnh viện nguồn
OWNER_THEO_LOAI = {
    "patient": "Đội IT",
    "lab": "Khoa xét nghiệm",
    "vital": "Đơn vị ICU",
}

# Loại dữ liệu -> tên file trong thư mục đợt. Tên bảng logic là raw_<loại>_hospital_<mã>
# (khớp hợp đồng contracts/raw_patient.yaml, raw_lab.yaml, raw_vital.yaml).
FILE_THEO_LOAI = {
    "patient": "patient.csv",
    "lab": "lab.csv",
    "vital": "vitalPeriodic.csv",
}


def _dem_dong(duong_dan):
    """Số dòng dữ liệu của file CSV (không tính dòng tiêu đề)."""
    with open(duong_dan, "rb") as f:
        return max(sum(1 for _ in f) - 1, 0)


def register_raw_tables(db_path=None, raw_dir=None):
    """Đăng ký 3 bảng raw cho mỗi viện có trong raw_dir. Trả về danh sách tên bảng đã đăng ký."""
    raw_dir = raw_dir or RAW_DIR
    cac_vien = danh_sach_vien(raw_dir)
    da_dang_ky = []
    for vien in cac_vien:
        nguon = source_of_hospital(vien)
        for loai, bo_phan in OWNER_THEO_LOAI.items():
            ten_bang = f"raw_{loai}_hospital_{vien}"
            mau_file = os.path.join(raw_dir, f"hospital_{vien}", "*", FILE_THEO_LOAI[loai])
            so_dong = sum(_dem_dong(f) for f in glob.glob(mau_file))
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
                source_system=nguon["name"],
                source_format=nguon["format"],
                **thong_tin,
            )
            da_dang_ky.append(ten_bang)
    print(f"  Đã đăng ký {len(da_dang_ky)} bảng raw của {len(cac_vien)} bệnh viện "
          f"(cột nhạy cảm theo hợp đồng, ví dụ raw_patient: {catalog_metadata('raw_patient_hospital_x')['sensitive_columns']})")
    return da_dang_ky


def main(db_path=None, staging_dir=None, raw_dir=None):
    split_into_timestamps(staging_dir, raw_dir)
    if raw_dir is None and os.path.isdir(BACKUP_DIR):
        shutil.rmtree(BACKUP_DIR)
    print("\nĐăng ký bảng raw vào catalog:")
    return register_raw_tables(db_path, raw_dir)


if __name__ == "__main__":
    main()
