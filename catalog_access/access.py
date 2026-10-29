"""
Module Kiểm soát Phân quyền Truy cập (RBAC - Role-Based Access Control)
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit & Báo cáo)

Nhiệm vụ:
- Cung cấp hàm check_access(nguoi_dung, bang) cho cả nhóm gọi.
- Xác thực quyền truy cập dựa trên các vai trò cấu hình trong policies.yaml:
  1. Kỹ sư dữ liệu (data_engineer): đọc/ghi mọi tầng (raw, standardized, ai_ready).
  2. Chuyên viên phân tích (analyst): đọc standardized + ai_ready, cột nhạy cảm bị ẩn.
  3. Nhà nghiên cứu (researcher): chỉ đọc được tầng ai_ready.
  4. Khách (guest): chỉ xem catalog, bị từ chối mọi tầng dữ liệu.
- Tự động ghi nhận mọi lần truy cập (thành công hoặc bị từ chối) vào sổ audit_log.
- require_access(...): như check_access nhưng ném AccessDenied khi bị từ chối
  (lớp đọc/ghi dữ liệu catalog_access/data_access.py dùng hàm này).
"""

import os
import sys
from typing import Any, Dict, List, Optional
import yaml

# Thêm thư mục gốc dự án vào sys.path nếu cần
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from catalog_access.init_db import get_db_connection
from catalog_access.audit import log_event

DEFAULT_POLICY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "policies.yaml"))

HANH_DONG_DOC = ("read", "select", "query")
HANH_DONG_GHI = ("write", "update", "delete", "insert")


class AccessDenied(PermissionError):
    """Bị từ chối truy cập theo policies.yaml (dòng DENIED đã được ghi vào audit_log)."""

    def __init__(self, nguoi_dung: str, bang: str, action: str, reason: str):
        super().__init__(reason)
        self.nguoi_dung = nguoi_dung
        self.bang = bang
        self.action = action
        self.reason = reason


def load_policies(policy_path: Optional[str] = None) -> Dict[str, Any]:
    """Tải tập luật phân quyền từ file YAML."""
    path = policy_path or DEFAULT_POLICY_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"Không tìm thấy file chính sách phân quyền tại: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_role(user_identifier: str, policies: Optional[Dict[str, Any]] = None) -> str:
    """
    Xác định vai trò (role) của người dùng hoặc định danh truyền vào.
    Hỗ trợ cả tên người dùng cụ thể và tên vai trò tiếng Việt/tiếng Anh.
    """
    if not policies:
        policies = load_policies()

    user_clean = str(user_identifier).strip().lower()

    # Ánh xạ từ ngữ tiếng Việt hoặc bí danh
    role_aliases = {
        "kỹ sư dữ liệu": "data_engineer",
        "ky su du lieu": "data_engineer",
        "data engineer": "data_engineer",
        "data_engineer": "data_engineer",
        "chuyên viên phân tích": "analyst",
        "chuyen vien phan tich": "analyst",
        "analyst": "analyst",
        "nhà nghiên cứu": "researcher",
        "nha nghien cuu": "researcher",
        "researcher": "researcher",
        "khách": "guest",
        "khach": "guest",
        "guest": "guest",
        "viewer": "guest"
    }

    if user_clean in role_aliases:
        return role_aliases[user_clean]

    # Kiểm tra trong danh sách users của policies.yaml
    users_map = policies.get("users", {})
    if user_clean in users_map:
        return users_map[user_clean]

    # Kiểm tra nếu truyền trực tiếp tên vai trò hợp lệ
    roles_map = policies.get("roles", {})
    if user_clean in roles_map:
        return user_clean

    # Mặc định lấy default_role
    return policies.get("default_role", "guest")


def get_role_info(nguoi_dung: str, policies: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Thông tin vai trò của người dùng (kèm khóa 'role'); vai trò không tồn tại -> không có quyền gì."""
    policies = policies or load_policies()
    role_key = resolve_role(nguoi_dung, policies)
    return {"role": role_key, **policies.get("roles", {}).get(role_key, {})}


def get_table_tier(table_name: str, db_path: Optional[str] = None) -> str:
    """
    Xác định tầng dữ liệu (tier) của bảng:
    1. Tra cứu trong bảng datasets của metadata.db
    2. Nếu chưa đăng ký, suy luận theo tiền tố tên bảng (raw_, standardized_, ai_ready_)
    """
    table_clean = table_name.strip()
    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT tier FROM datasets WHERE name = ?;", (table_clean,))
        row = cursor.fetchone()
        conn.close()
        if row and row["tier"]:
            return row["tier"].lower()
    except Exception:
        pass

    # Quy tắc suy luận dự phòng dựa vào tên bảng
    lower_name = table_clean.lower()
    if lower_name.startswith("raw") or "hospital_" in lower_name:
        return "raw"
    if lower_name.startswith("standardized") or "std_" in lower_name:
        return "standardized"
    if lower_name.startswith("ai_ready") or "ml_" in lower_name:
        return "ai_ready"

    # Không đoán được -> coi như tầng thô (chặt nhất)
    return "raw"


def evaluate_access(
    nguoi_dung: str,
    bang: str,
    action: str = "read",
    policy_path: Optional[str] = None,
    db_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Quyết định cho phép hay từ chối, KHÔNG ghi audit.
    Trả về dict: allowed (bool), reason (str), role, tier.
    """
    policies = load_policies(policy_path)
    role_info = get_role_info(nguoi_dung, policies)
    role_key = role_info["role"]
    ten_vai_tro = role_info.get("display_name", role_key)

    allowed_tiers = role_info.get("allowed_tiers", [])
    tier = get_table_tier(bang, db_path)

    if action == "view_catalog":
        is_allowed = "catalog" in allowed_tiers
        reason = "Đọc danh mục catalog" if is_allowed else "Không có quyền xem catalog"
    elif tier not in allowed_tiers:
        is_allowed = False
        reason = f"Từ chối vi phạm bảo mật: Vai trò [{ten_vai_tro}] bị cấm truy cập dữ liệu tầng [{tier}] (bảng '{bang}')"
    elif action in HANH_DONG_DOC and role_info.get("can_read", False):
        is_allowed = True
        reason = f"Được phép: Vai trò [{ten_vai_tro}] có quyền đọc tầng [{tier}]"
    elif action in HANH_DONG_GHI and role_info.get("can_write", False):
        is_allowed = True
        reason = f"Được phép: Vai trò [{ten_vai_tro}] có quyền ghi tầng [{tier}]"
    else:
        is_allowed = False
        reason = f"Từ chối: Vai trò [{ten_vai_tro}] không có quyền thực hiện hành động '{action}'"

    return {"allowed": is_allowed, "reason": reason, "role": role_key, "tier": tier}


def check_access(
    nguoi_dung: str,
    bang: str,
    action: str = "read",
    policy_path: Optional[str] = None,
    db_path: Optional[str] = None,
    auto_audit: bool = True
) -> bool:
    """
    Kiểm tra quyền truy cập của người dùng trên bảng dữ liệu.
    Đây là hàm chuẩn dùng chung cho cả nhóm theo phân công trong PDF.

    Tham số:
        nguoi_dung (str): Tên tài khoản hoặc tên vai trò (e.g. 'researcher_bob', 'Nhà nghiên cứu').
        bang (str): Tên bảng dữ liệu cần truy cập (e.g. 'raw_patient_hospital_B', 'ai_ready_icu_mortality').
        action (str): Hành động yêu cầu ('read', 'write', 'view_catalog'). Mặc định là 'read'.
        policy_path (str, optional): Đường dẫn policies.yaml.
        db_path (str, optional): Đường dẫn metadata.db.
        auto_audit (bool): Tự động ghi vào sổ audit log (mặc định True).

    Trả về:
        bool: True nếu được phép, False nếu bị từ chối.
    """
    quyet_dinh = evaluate_access(nguoi_dung, bang, action, policy_path, db_path)

    # Tự động ghi vào sổ audit log
    if auto_audit:
        try:
            log_event(
                ai=nguoi_dung,
                hanh_dong=f"{action}:{quyet_dinh['tier']}",
                bang=bang,
                ket_qua="ALLOWED" if quyet_dinh["allowed"] else "DENIED",
                db_path=db_path
            )
        except Exception as e:
            print(f"[Warning] Không thể ghi audit log: {e}")

    return quyet_dinh["allowed"]


def require_access(
    nguoi_dung: str,
    bang: str,
    action: str = "read",
    policy_path: Optional[str] = None,
    db_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Như check_access (có ghi audit) nhưng ném AccessDenied khi bị từ chối.
    Dùng ở mọi chỗ đọc/ghi dữ liệu thật để quyền được THỰC THI chứ không chỉ để tham khảo.
    """
    quyet_dinh = evaluate_access(nguoi_dung, bang, action, policy_path, db_path)
    log_event(
        ai=nguoi_dung,
        hanh_dong=f"{action}:{quyet_dinh['tier']}",
        bang=bang,
        ket_qua="ALLOWED" if quyet_dinh["allowed"] else "DENIED",
        db_path=db_path
    )
    if not quyet_dinh["allowed"]:
        raise AccessDenied(nguoi_dung, bang, action, quyet_dinh["reason"])
    return quyet_dinh


def get_hidden_columns(
    nguoi_dung: str,
    sensitive_columns: List[str],
    policy_path: Optional[str] = None
) -> List[str]:
    """Các cột nhạy cảm phải ẩn với người dùng này (vai trò không có can_view_sensitive)."""
    role_info = get_role_info(nguoi_dung, load_policies(policy_path))
    if role_info.get("can_view_sensitive", False):
        return []
    return [c for c in sensitive_columns if c]


def can_view_audit(nguoi_dung: str, policy_path: Optional[str] = None) -> bool:
    """Người dùng có được xem sổ audit_log không (policies.yaml: can_view_audit)."""
    return bool(get_role_info(nguoi_dung, load_policies(policy_path)).get("can_view_audit", False))


if __name__ == "__main__":
    # Test phân quyền trực tiếp:
    print("--- KIỂM TRA PHÂN QUYỀN (RBAC) ---")

    # 1. Kỹ sư dữ liệu đọc tầng raw -> ALLOWED
    r1 = check_access("engineer_alice", "raw_patient_hospital_A")
    print(f"Kỹ sư dữ liệu đọc raw_patient_hospital_A: {'ĐƯỢC PHÉP' if r1 else 'TỪ CHỐI'}")

    # 2. Nhà nghiên cứu đọc dữ liệu AI-ready -> ALLOWED
    r2 = check_access("researcher_bob", "ai_ready_icu_mortality")
    print(f"Nhà nghiên cứu đọc ai_ready_icu_mortality: {'ĐƯỢC PHÉP' if r2 else 'TỪ CHỐI'}")

    # 3. Nhà nghiên cứu đọc dữ liệu thô (raw) -> DENIED (Đúng tiêu chí nghiệm thu)
    r3 = check_access("nhà nghiên cứu", "raw_patient_hospital_B")
    print(f"Nhà nghiên cứu đọc raw_patient_hospital_B: {'ĐƯỢC PHÉP' if r3 else 'TỪ CHỐI (Ghi vào audit log)'}")

    # 4. Khách đọc bảng dữ liệu -> DENIED
    r4 = check_access("guest_charlie", "standardized_patients")
    print(f"Khách đọc standardized_patients: {'ĐƯỢC PHÉP' if r4 else 'TỪ CHỐI'}")

    # 5. Chuyên viên phân tích đọc standardized -> ALLOWED nhưng cột patient_id bị ẩn
    r5 = check_access("analyst_dave", "standardized_patients")
    print(f"Chuyên viên phân tích đọc standardized_patients: {'ĐƯỢC PHÉP' if r5 else 'TỪ CHỐI'}, "
          f"cột bị ẩn: {get_hidden_columns('analyst_dave', ['patient_id'])}")
