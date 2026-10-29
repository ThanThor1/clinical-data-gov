"""
Giả danh hóa (pseudonymization) và kiểm tra quyền riêng tư cho tầng AI-ready.

1. patient_id -> patient_hash = HMAC-SHA256(khóa bí mật, patient_id)
   SHA-256 thường KHÔNG đủ: patientunitstayid chỉ là số nguyên vài trăm nghìn, ai có danh sách
   mã (hoặc thử hết các số) là băm lại và dò ngược ra bệnh nhân trong chưa tới 1 giây.
   Với HMAC, không có khóa thì không tính được mã giả danh -> không dò ngược được.

2. Khóa bí mật (catalog_access/keys.py) lấy theo thứ tự:
     - biến môi trường PATIENT_HASH_SECRET (ít nhất 16 ký tự), hoặc
     - file khóa PATIENT_HASH_KEY_FILE (mặc định .secrets/patient_hash.key ở gốc repo).
   Chưa có file thì tự sinh khóa ngẫu nhiên 256 bit, quyền 600. Thư mục .secrets/ nằm trong
   .gitignore: KHÔNG commit khóa, KHÔNG viết khóa vào code.

3. check_privacy(): luật riêng cho bảng có hợp đồng tier: ai_ready (gate1 gọi):
     - PRIVACY_SENSITIVE_COLUMN : có cột định danh (cột sensitive: true ở bất kỳ hợp đồng nào)
     - PRIVACY_UNDECLARED_COLUMN: có cột không khai báo trong hợp đồng (nguyên tắc tối thiểu hóa
       dữ liệu: tầng AI-ready chỉ chứa đúng những gì đã được duyệt)

Chạy thử: python -m governance.privacy
"""

import hashlib
import hmac
from typing import Optional

import pandas as pd

from catalog_access.keys import get_key
from governance.contracts import load_contracts


def get_secret() -> bytes:
    """Khóa bí mật dùng cho HMAC giả danh (xem thứ tự lấy khóa ở đầu file)."""
    return get_key("PATIENT_HASH_SECRET", "PATIENT_HASH_KEY_FILE", "patient_hash.key")


def _chuan_hoa_ma(gia_tri) -> str:
    """141234, 141234.0 và '141234' phải ra cùng một mã giả danh."""
    if isinstance(gia_tri, float) and gia_tri.is_integer():
        gia_tri = int(gia_tri)
    return str(gia_tri).strip()


def pseudonymize_id(gia_tri, secret: Optional[bytes] = None) -> str:
    """Mã giả danh (64 ký tự hex) của một mã bệnh nhân."""
    khoa = secret if secret is not None else get_secret()
    return hmac.new(khoa, _chuan_hoa_ma(gia_tri).encode("utf-8"), hashlib.sha256).hexdigest()


def pseudonymize_series(cot: pd.Series, secret: Optional[bytes] = None) -> pd.Series:
    """Giả danh cả một cột (đọc khóa một lần cho cả cột)."""
    khoa = secret if secret is not None else get_secret()
    return cot.map(lambda x: pseudonymize_id(x, khoa))


def sensitive_column_names(contracts_dir: Optional[str] = None) -> set[str]:
    """Tên mọi cột được đánh dấu sensitive: true ở bất kỳ hợp đồng nào (patientunitstayid, patient_id, ...)."""
    return {ten for hd in load_contracts(contracts_dir)
            for ten, luat in hd["columns"].items() if luat.get("sensitive")}


def check_privacy(ten_bang: str, df: pd.DataFrame, hop_dong: dict,
                  contracts_dir: Optional[str] = None) -> list[dict]:
    """
    Kiểm tra quyền riêng tư cho bảng tầng AI-ready. Bảng tầng khác -> không kiểm tra.
    Trả về danh sách vấn đề (dict: error_type, column, message, observed, expected),
    gates.py gắn mức độ BREAKING cho tất cả.
    """
    if not hop_dong or hop_dong.get("tier") != "ai_ready":
        return []

    van_de = []
    nhay_cam = sensitive_column_names(contracts_dir)
    khai_bao = set(hop_dong["columns"])
    for cot in df.columns:
        if cot in nhay_cam:
            van_de.append({
                "error_type": "PRIVACY_SENSITIVE_COLUMN", "column": cot,
                "message": f"Bảng AI-ready '{ten_bang}' chứa cột định danh '{cot}' (phải giả danh hóa hoặc bỏ)",
                "observed": f"có cột '{cot}'", "expected": "không có cột định danh",
            })
        elif cot not in khai_bao:
            van_de.append({
                "error_type": "PRIVACY_UNDECLARED_COLUMN", "column": cot,
                "message": f"Bảng AI-ready '{ten_bang}' có cột '{cot}' không khai báo trong hợp đồng "
                           f"(tầng AI-ready chỉ được chứa các cột đã duyệt)",
                "observed": f"có cột '{cot}'", "expected": f"chỉ các cột {sorted(khai_bao)}",
            })
    return van_de


if __name__ == "__main__":
    khoa_thu = b"khoa-thu-nghiem-chi-de-minh-hoa"
    print("HMAC-SHA256('141234')   :", pseudonymize_id(141234, khoa_thu))
    print("SHA-256 thường('141234'):", hashlib.sha256(b"141234").hexdigest(), "<- ai cũng tính lại được")
    print("Cột định danh theo hợp đồng:", sorted(sensitive_column_names()))
