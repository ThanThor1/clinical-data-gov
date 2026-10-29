"""
Quản lý khóa bí mật dùng chung
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit & Báo cáo)

Hệ thống dùng 2 khóa:
- Khóa giả danh bệnh nhân (governance/privacy.py)  : PATIENT_HASH_SECRET / PATIENT_HASH_KEY_FILE
- Khóa chuỗi băm sổ audit (catalog_access/audit.py): AUDIT_LOG_SECRET    / AUDIT_LOG_KEY_FILE

Mỗi khóa lấy theo thứ tự:
  1. biến môi trường chứa chính khóa (ít nhất 16 ký tự), hoặc
  2. file khóa (đường dẫn trong biến môi trường *_KEY_FILE, mặc định thư mục .secrets/ ở gốc repo).
Chưa có file thì tự sinh khóa ngẫu nhiên 256 bit, quyền 600.
Thư mục .secrets/ nằm trong .gitignore: KHÔNG commit khóa, KHÔNG viết khóa vào code.
"""

import os
import secrets

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SECRETS_DIR = os.path.join(ROOT, ".secrets")
DO_DAI_KHOA_TOI_THIEU = 16


def get_key(bien_khoa: str, bien_file: str, ten_file_mac_dinh: str) -> bytes:
    """
    bien_khoa         : tên biến môi trường chứa khóa, ví dụ "PATIENT_HASH_SECRET"
    bien_file         : tên biến môi trường chứa đường dẫn file khóa, ví dụ "PATIENT_HASH_KEY_FILE"
    ten_file_mac_dinh : tên file trong .secrets/ nếu không đặt bien_file, ví dụ "patient_hash.key"
    """
    tu_moi_truong = os.environ.get(bien_khoa)
    if tu_moi_truong is not None:
        if len(tu_moi_truong) < DO_DAI_KHOA_TOI_THIEU:
            raise ValueError(f"{bien_khoa} phải dài ít nhất {DO_DAI_KHOA_TOI_THIEU} ký tự")
        return tu_moi_truong.encode("utf-8")

    duong_dan = os.environ.get(bien_file) or os.path.join(SECRETS_DIR, ten_file_mac_dinh)
    if not os.path.exists(duong_dan):
        os.makedirs(os.path.dirname(os.path.abspath(duong_dan)), mode=0o700, exist_ok=True)
        # O_EXCL: hai tiến trình chạy cùng lúc thì chỉ một bên tạo được khóa
        try:
            fd = os.open(duong_dan, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(secrets.token_hex(32))
            print(f"[keys] Đã tạo khóa mới: {duong_dan} (không commit file này)")
        except FileExistsError:
            pass
    with open(duong_dan, "r", encoding="utf-8") as f:
        khoa = f.read().strip()
    if len(khoa) < DO_DAI_KHOA_TOI_THIEU:
        raise ValueError(f"File khóa {duong_dan} quá ngắn (cần ít nhất {DO_DAI_KHOA_TOI_THIEU} ký tự)")
    return khoa.encode("utf-8")
