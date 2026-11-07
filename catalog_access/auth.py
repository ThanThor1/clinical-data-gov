"""
Đăng nhập dashboard bằng mật khẩu.

Mật khẩu KHÔNG lưu dạng rõ: policies.yaml (mục `passwords`) chỉ lưu băm PBKDF2-HMAC-SHA256 có muối ngẫu nhiên,
dạng  pbkdf2_sha256$<số vòng>$<muối hex>$<băm hex>.
- Tài khoản phải có trong mục `users` VÀ có băm mật khẩu mới đăng nhập được; tài khoản dịch vụ
  (pipeline_service) không có băm nên không đăng nhập dashboard được.
- Mỗi lần đăng nhập (đúng hay sai) ghi một dòng audit_log: ai = tài khoản, làm gì = login, ALLOWED / DENIED.

Tạo băm cho một mật khẩu mới (rồi dán vào policies.yaml):
    python -m catalog_access.auth "<mật khẩu>"
"""

import hashlib
import hmac
import os
import secrets
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catalog_access.access import load_policies
from catalog_access.audit import log_event

THUAT_TOAN = "pbkdf2_sha256"
SO_VONG = 200_000


def hash_password(mat_khau: str, muoi: Optional[bytes] = None, so_vong: int = SO_VONG) -> str:
    """Băm mật khẩu: pbkdf2_sha256$<số vòng>$<muối hex>$<băm hex> (muối 16 byte ngẫu nhiên nếu không truyền)."""
    muoi = muoi or secrets.token_bytes(16)
    bam = hashlib.pbkdf2_hmac("sha256", mat_khau.encode("utf-8"), muoi, so_vong)
    return f"{THUAT_TOAN}${so_vong}${muoi.hex()}${bam.hex()}"


def verify_password(mat_khau: str, chuoi_bam: str) -> bool:
    """Mật khẩu có khớp chuỗi băm không (so sánh thời gian hằng, chuỗi băm sai dạng -> False)."""
    try:
        thuat_toan, so_vong, muoi, bam = chuoi_bam.split("$")
        if thuat_toan != THUAT_TOAN:
            return False
        tinh_lai = hashlib.pbkdf2_hmac("sha256", mat_khau.encode("utf-8"), bytes.fromhex(muoi), int(so_vong))
    except (ValueError, AttributeError):
        return False
    return hmac.compare_digest(tinh_lai.hex(), bam)


# Băm giả để tài khoản không tồn tại vẫn tốn đúng chừng ấy thời gian (không dò được tài khoản nào có thật)
_BAM_GIA = hash_password("khong-dung", b"\0" * 16)


def authenticate(nguoi_dung: str, mat_khau: str, policy_path: Optional[str] = None,
                 db_path: Optional[str] = None) -> bool:
    """Kiểm tra tài khoản + mật khẩu theo policies.yaml, ghi audit (login ALLOWED / DENIED). Trả về True nếu đúng."""
    policies = load_policies(policy_path)
    ten = str(nguoi_dung or "").strip().lower()
    chuoi_bam = (policies.get("passwords") or {}).get(ten) if ten in (policies.get("users") or {}) else None
    dung = verify_password(mat_khau or "", chuoi_bam or _BAM_GIA) and chuoi_bam is not None
    log_event(ai=ten or "unknown_user", hanh_dong="login", bang="", ket_qua="ALLOWED" if dung else "DENIED",
              db_path=db_path)
    return dung


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Cách dùng: python -m catalog_access.auth "<mật khẩu>"')
        sys.exit(1)
    print(hash_password(sys.argv[1]))
