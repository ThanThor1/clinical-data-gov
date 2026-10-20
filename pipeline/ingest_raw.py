"""
Nạp dữ liệu vào tầng RAW (giữ nguyên bản gốc):
  1. split_snapshots: chia dữ liệu mỗi viện thành 3 đợt gửi
     -> data/raw/hospital_X/<YYYY-MM-DD_HH-MM-SS>/{patient,lab,vitalPeriodic}.csv
  2. Đăng ký 9 bảng raw (3 loại x 3 viện) vào catalog bằng register_dataset;
     cột nhạy cảm lấy từ hợp đồng dữ liệu (contracts/*.yaml, trường sensitive: true).

Chạy: python -m pipeline.ingest_raw   (từ gốc repo)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from catalog_access import register_dataset
from governance.contracts import find_contract, get_sensitive_columns
from pipeline.split_snapshots import split_into_timestamps

HOSPITALS = ["A", "B", "C"]

# Loại bảng raw -> bộ phận phụ trách ở bệnh viện nguồn
OWNER_THEO_LOAI = {
    "patient": "Đội IT",
    "lab": "Khoa xét nghiệm",
    "vital": "Đơn vị ICU",
}


def register_raw_tables(db_path=None):
    """Đăng ký 9 bảng raw vào catalog. Trả về danh sách tên bảng đã đăng ký."""
    da_dang_ky = []
    for vien in HOSPITALS:
        for loai, bo_phan in OWNER_THEO_LOAI.items():
            ten_bang = f"raw_{loai}_hospital_{vien}"
            hop_dong = find_contract(ten_bang)
            nhay_cam = ",".join(get_sensitive_columns(hop_dong)) if hop_dong else ""
            register_dataset(
                ten_bang,
                hospital=f"Bệnh viện {vien}",
                tier="raw",
                owner=f"{bo_phan} Bệnh viện {vien}",
                sensitive_columns=nhay_cam,
                registered_by="pipeline_ingest",
                db_path=db_path,
            )
            print(f"  Đã đăng ký {ten_bang:<24} (cột nhạy cảm: {nhay_cam or 'không có'})")
            da_dang_ky.append(ten_bang)
    return da_dang_ky


def main(db_path=None):
    split_into_timestamps()
    print("\nĐăng ký 9 bảng raw vào catalog:")
    return register_raw_tables(db_path)


if __name__ == "__main__":
    main()
