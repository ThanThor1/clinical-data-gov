import os
import argparse
import shutil
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
BACKUP_DIR = os.path.join(DATA_DIR, "_backup_inject")


def get_batch_3_dir(hospital):
    """Tìm thư mục đợt 3 (chứa 20-00-00) của viện"""
    hospital_dir = os.path.join(RAW_DIR, f"hospital_{hospital}")
    if not os.path.isdir(hospital_dir):
        return None
    for folder in os.listdir(hospital_dir):
        if "20-00-00" in folder:
            return os.path.join(hospital_dir, folder)
    return None


def backup_batch_3(hospital, batch_dir):
    """Sao lưu đợt 3 trước khi làm hỏng"""
    backup_hosp_dir = os.path.join(BACKUP_DIR, f"hospital_{hospital}")
    os.makedirs(backup_hosp_dir, exist_ok=True)

    # Chỉ copy các file .csv
    for file in os.listdir(batch_dir):
        if file.endswith(".csv"):
            src = os.path.join(batch_dir, file)
            dst = os.path.join(backup_hosp_dir, file)
            if not os.path.exists(dst):  # Chỉ backup 1 lần duy nhất
                shutil.copy2(src, dst)
                print(f"Đã backup: {file}")


def inject_error(error_type, hospital):
    batch_dir = get_batch_3_dir(hospital)
    if not batch_dir:
        print(f"Không tìm thấy đợt 3 của viện {hospital}")
        return

    print(f"--- Gây lỗi '{error_type}' cho viện {hospital} (đợt 3) ---")

    # 1. Backup trước
    backup_batch_3(hospital, batch_dir)

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
        hong_indices = df.sample(n=so_dong_hong).index
        df.loc[hong_indices, "heartrate"] = 500
        df.to_csv(vital_file, index=False)
        print(f"Đã gán nhịp tim 500 cho {so_dong_hong}/{so_dong} dòng")

    elif error_type == "thieu_can_nang":
        # B: xóa trắng 50% admissionweight
        df = pd.read_csv(patient_file)
        so_dong = len(df)
        so_dong_trong = max(1, int(so_dong * 0.5))  # 50%

        # Chọn ngẫu nhiên 50% số dòng
        trong_indices = df.sample(n=so_dong_trong).index
        df.loc[trong_indices, "admissionweight"] = pd.NA
        df.to_csv(patient_file, index=False)
        print(f"Đã xóa trắng {so_dong_trong}/{so_dong} dòng admissionweight")
    else:
        print("Lỗi không hợp lệ!")


def sua_lai(hospital):
    """Khôi phục đợt 3 từ backup"""
    batch_dir = get_batch_3_dir(hospital)
    backup_hosp_dir = os.path.join(BACKUP_DIR, f"hospital_{hospital}")

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


# Mỗi loại lỗi gây cho một viện cố định
MAPPING_VIEN = {
    "doi_ten": "B",
    "doi_kieu": "A",
    "xoa_cot": "C",
    "nhip_tim_500": "A",
    "thieu_can_nang": "B",
}


def gay_loi(error_type):
    """Gây lỗi `error_type` cho viện tương ứng (đợt 3). Trả về viện bị gây lỗi."""
    vien = MAPPING_VIEN[error_type]
    inject_error(error_type, vien)
    return vien


def main(argv=None):
    parser = argparse.ArgumentParser(description="Gây lỗi giả lập cho đợt 3")
    parser.add_argument("--loi", type=str, choices=list(MAPPING_VIEN), help="Loại lỗi muốn gây ra")
    parser.add_argument("--sua-lai", action="store_true", help="Khôi phục đợt 3 từ backup")
    parser.add_argument("--vien", type=str, choices=["A", "B", "C"], help="Viện muốn sửa lại (dành cho --sua-lai)")

    args = parser.parse_args(argv)

    if args.sua_lai:
        if not args.vien:
            print("Vui lòng nhập --vien A/B/C khi dùng --sua-lai")
        else:
            sua_lai(args.vien)
    elif args.loi:
        gay_loi(args.loi)
    else:
        print("Vui lòng truyền --loi <ten_loi> hoặc --sua-lai --vien <A/B/C>")


if __name__ == "__main__":
    main()
