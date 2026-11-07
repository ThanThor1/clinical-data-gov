"""
Gây lỗi giả lập cho đợt 3 (thư mục thời điểm *20-00-00) của một bệnh viện, để thử hệ thống governance.

Mỗi loại lỗi mặc định gây cho viện theo THỨ HẠNG số bệnh nhân (hạng 1 = đông nhất):
    doi_ten        : hạng 2  - đổi tên cột admissionweight -> weight
    doi_kieu       : hạng 1  - cân nặng thành chuỗi có đơn vị ("70.5 kg")
    xoa_cot        : hạng 3  - xóa cột gender
    nhip_tim_500   : hạng 1  - 5% dòng sinh hiệu có nhịp tim = 500
    thieu_can_nang : hạng 2  - xóa trắng 50% cân nặng
Với eICU demo: hạng 1, 2, 3 là viện 146, 123, 157 (157 và 171 cùng 25 bệnh nhân, mã nhỏ xếp trước);
với dữ liệu giả: A, B, C.

Chạy:
    python -m pipeline.inject_errors --loi doi_ten                # viện hạng 2
    python -m pipeline.inject_errors --loi doi_ten --vien 264     # chỉ định viện
    python -m pipeline.inject_errors --sua-lai --vien 123         # khôi phục đợt 3 từ bản sao lưu

Lỗi do PIPELINE (code biến đổi sai, dữ liệu nguồn vẫn đúng) - bật bằng biến môi trường cho một lần chạy:
    PIPELINE_LOI_GIA_LAP=can_nang_pound   python -m pipeline.standardize
    PIPELINE_LOI_GIA_LAP=dao_nhan_tu_vong python -m pipeline.build_ai_ready
Phiên bản code của lần chạy có thêm '+loi.<tên>' (governance/provenance.get_code_version).
"""

import os
import argparse
import glob
import shutil
from contextlib import contextmanager

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
BACKUP_DIR = os.path.join(DATA_DIR, "_backup_inject")

# Cố định hạt giống ngẫu nhiên: chạy lại vẫn hỏng đúng những dòng đó (kết quả đánh giá lặp lại được)
HAT_GIONG = 42

# Lỗi pipeline giả lập: cả hai đều cho ra dữ liệu ĐÚNG hợp đồng nhưng SAI nghĩa (cổng hợp đồng không bắt được)
BIEN_LOI_PIPELINE = "PIPELINE_LOI_GIA_LAP"
LOI_PIPELINE = {
    "can_nang_pound": ("standardized_patients", "standardize đổi cân nặng sang pound (x2,20462) mà vẫn ghi vào weight_kg"),
    "dao_nhan_tu_vong": ("ai_ready_icu_mortality", "build_ai_ready đảo nhãn: Expired -> 0, còn lại -> 1"),
}


def loi_pipeline() -> str:
    """Lỗi pipeline giả lập đang bật ('' nếu không có)."""
    return os.environ.get(BIEN_LOI_PIPELINE, "")


@contextmanager
def gay_loi_pipeline(loai):
    """Bật lỗi pipeline `loai` (một khóa của LOI_PIPELINE) trong khối with, ra khỏi khối thì tắt."""
    if loai not in LOI_PIPELINE:
        raise ValueError(f"Lỗi pipeline '{loai}' không hợp lệ, chỉ nhận {sorted(LOI_PIPELINE)}")
    cu = os.environ.get(BIEN_LOI_PIPELINE)
    os.environ[BIEN_LOI_PIPELINE] = loai
    try:
        yield
    finally:
        if cu is None:
            os.environ.pop(BIEN_LOI_PIPELINE, None)
        else:
            os.environ[BIEN_LOI_PIPELINE] = cu


def get_batch_3_dir(hospital, raw_dir=None):
    """Tìm thư mục đợt 3 (chứa 20-00-00) của viện"""
    hospital_dir = os.path.join(raw_dir or RAW_DIR, f"hospital_{hospital}")
    if not os.path.isdir(hospital_dir):
        return None
    for folder in os.listdir(hospital_dir):
        if "20-00-00" in folder:
            return os.path.join(hospital_dir, folder)
    return None


def backup_batch_3(hospital, batch_dir, backup_dir=None):
    """Sao lưu đợt 3 trước khi làm hỏng"""
    backup_hosp_dir = os.path.join(backup_dir or BACKUP_DIR, f"hospital_{hospital}")
    os.makedirs(backup_hosp_dir, exist_ok=True)

    # Chỉ copy các file .csv
    for file in os.listdir(batch_dir):
        if file.endswith(".csv"):
            src = os.path.join(batch_dir, file)
            dst = os.path.join(backup_hosp_dir, file)
            if not os.path.exists(dst):  # Chỉ backup 1 lần duy nhất
                shutil.copy2(src, dst)
                print(f"Đã backup: {file}")


def inject_error(error_type, hospital, raw_dir=None, backup_dir=None):
    batch_dir = get_batch_3_dir(hospital, raw_dir)
    if not batch_dir:
        print(f"Không tìm thấy đợt 3 của viện {hospital}")
        return

    print(f"--- Gây lỗi '{error_type}' cho viện {hospital} (đợt 3) ---")

    # 1. Backup trước
    backup_batch_3(hospital, batch_dir, backup_dir)

    patient_file = os.path.join(batch_dir, "patient.csv")
    vital_file = os.path.join(batch_dir, "vitalPeriodic.csv")

    # 2. Thực hiện lỗi
    if error_type == "doi_ten":
        # B: admissionweight -> weight
        df = pd.read_csv(patient_file)
        df = df.rename(columns={"admissionweight": "weight"})
        df.to_csv(patient_file, index=False)
        print("Đã đổi tên cột admissionweight -> weight")

    elif error_type == "doi_kieu":
        # A: admissionweight từ số -> chuỗi có đơn vị ("70.5 kg")
        # Lưu ý: chỉ astype(str) là KHÔNG đủ với pandas 3 - ô trống vẫn là NaN, ghi ra CSV
        # vẫn là "70.5" nên đọc lại thành số như cũ, lỗi biến mất.
        df = pd.read_csv(patient_file)
        df["admissionweight"] = df["admissionweight"].map(
            lambda x: f"{x} kg" if pd.notna(x) else "")
        df.to_csv(patient_file, index=False)
        print("Đã đổi kiểu cột admissionweight -> chuỗi (ví dụ '70.5 kg')")

    elif error_type == "xoa_cot":
        # C: xóa cột gender
        df = pd.read_csv(patient_file)
        if "gender" in df.columns:
            df = df.drop(columns=["gender"])
        df.to_csv(patient_file, index=False)
        print("Đã xóa cột gender")

    elif error_type == "nhip_tim_500":
        # A: gán 500 cho 5% số dòng
        if not os.path.exists(vital_file):
            print("Lỗi: Không có file vitalPeriodic.csv để gán nhịp tim 500!")
            return
        df = pd.read_csv(vital_file)
        so_dong = len(df)
        so_dong_hong = max(1, int(so_dong * 0.05))  # 5% số dòng (ít nhất 1 dòng)

        # Chọn ngẫu nhiên 5% số dòng
        hong_indices = df.sample(n=so_dong_hong, random_state=HAT_GIONG).index
        df.loc[hong_indices, "heartrate"] = 500
        df.to_csv(vital_file, index=False)
        print(f"Đã gán nhịp tim 500 cho {so_dong_hong}/{so_dong} dòng")

    elif error_type == "thieu_can_nang":
        # B: xóa trắng 50% admissionweight
        df = pd.read_csv(patient_file)
        so_dong = len(df)
        so_dong_trong = max(1, int(so_dong * 0.5))  # 50%

        # Chọn ngẫu nhiên 50% số dòng
        trong_indices = df.sample(n=so_dong_trong, random_state=HAT_GIONG).index
        df.loc[trong_indices, "admissionweight"] = pd.NA
        df.to_csv(patient_file, index=False)
        print(f"Đã xóa trắng {so_dong_trong}/{so_dong} dòng admissionweight")
    else:
        print("Lỗi không hợp lệ!")


def sua_lai(hospital, raw_dir=None, backup_dir=None):
    """Khôi phục đợt 3 từ backup"""
    batch_dir = get_batch_3_dir(hospital, raw_dir)
    backup_hosp_dir = os.path.join(backup_dir or BACKUP_DIR, f"hospital_{hospital}")

    if not batch_dir or not os.path.isdir(backup_hosp_dir):
        print(f"Không có bản backup hoặc không tìm thấy đợt 3 cho viện {hospital}")
        return

    print(f"--- Khôi phục (sửa lại) đợt 3 cho viện {hospital} ---")
    for file in os.listdir(backup_hosp_dir):
        if file.endswith(".csv"):
            src = os.path.join(backup_hosp_dir, file)
            dst = os.path.join(batch_dir, file)
            shutil.copy2(src, dst)
            print(f"Đã khôi phục: {file}")


# Mỗi loại lỗi gây cho viện có thứ hạng (theo số bệnh nhân) cố định
MAPPING_HANG = {
    "doi_ten": 2,
    "doi_kieu": 1,
    "xoa_cot": 3,
    "nhip_tim_500": 1,
    "thieu_can_nang": 2,
}


def xep_hang_vien(raw_dir=None):
    """
    Mã các viện của nguồn MẶC ĐỊNH (eICU) trong tầng raw, đông bệnh nhân trước (cùng số bệnh nhân thì mã nhỏ
    trước). Nguồn khác (ví dụ Challenge 2012, 4.000 lượt nằm ICU) không xếp hạng, để lỗi giả lập luôn rơi vào
    đúng các viện eICU như mô tả ở đầu file.
    """
    from governance.sources import is_default_source

    so_benh_nhan = {}
    for thu_muc in glob.glob(os.path.join(raw_dir or RAW_DIR, "hospital_*")):
        ma = os.path.basename(thu_muc)[len("hospital_"):]
        if not is_default_source(ma):
            continue
        so_benh_nhan[ma] = sum(len(pd.read_csv(f, usecols=[0]))
                               for f in glob.glob(os.path.join(thu_muc, "*", "patient.csv")))
    return sorted(so_benh_nhan, key=lambda m: (-so_benh_nhan[m], int(m) if m.isdigit() else 0, m))


def gay_loi(error_type, raw_dir=None, backup_dir=None, vien=None):
    """Gây lỗi `error_type` cho đợt 3 của viện `vien` (mặc định theo MAPPING_HANG). Trả về mã viện."""
    if vien is None:
        vien = xep_hang_vien(raw_dir)[MAPPING_HANG[error_type] - 1]
    inject_error(error_type, vien, raw_dir, backup_dir)
    return vien


def main(argv=None):
    parser = argparse.ArgumentParser(description="Gây lỗi giả lập cho đợt 3")
    parser.add_argument("--loi", type=str, choices=list(MAPPING_HANG), help="Loại lỗi muốn gây ra")
    parser.add_argument("--sua-lai", action="store_true", help="Khôi phục đợt 3 từ backup")
    parser.add_argument("--vien", type=str, help="Mã viện (ví dụ 123); với --loi thì bỏ trống = theo thứ hạng")

    args = parser.parse_args(argv)

    if args.sua_lai:
        if not args.vien:
            print("Vui lòng nhập --vien <mã viện> khi dùng --sua-lai")
        else:
            sua_lai(args.vien)
    elif args.loi:
        gay_loi(args.loi, vien=args.vien)
    else:
        print("Vui lòng truyền --loi <ten_loi> hoặc --sua-lai --vien <mã viện>")


if __name__ == "__main__":
    main()
