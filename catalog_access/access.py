"""
Module Kiểm soát Phân quyền Truy cập (RBAC - Role-Based Access Control)
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit & Báo cáo)

Nhiệm vụ:
- Cung cấp hàm check_access(nguoi_dung, bang) cho cả nhóm gọi.
- Xác thực quyền truy cập dựa trên 3 vai trò cấu hình trong policies.yaml:
  1. Kỹ sư dữ liệu (data_engineer): đọc được mọi tầng (raw, standardized, ai_ready).
  2. Nhà nghiên cứu (researcher): chỉ đọc được tầng ai_ready.
  3. Khách (guest): chỉ xem catalog, bị từ chối mọi tầng dữ liệu.
- Tự động ghi nhận mọi lần truy cập (thành công hoặc bị từ chối) vào sổ audit_log.
"""

import os
import sys
from typing import Any, Dict, Optional, Tuple
import yaml

# Thêm thư mục gốc dự án vào sys.path nếu cần
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from catalog_access.init_db import get_db_connection, DEFAULT_DB_PATH
from catalog_access.audit import log_event

DEFAULT_POLICY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "policies.yaml"))


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

    return "raw"


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
    policies = load_policies(policy_path)
    role_key = resolve_role(nguoi_dung, policies)
    role_info = policies.get("roles", {}).get(role_key, {})

    allowed_tiers = role_info.get("allowed_tiers", [])
    tier = get_table_tier(bang, db_path)

    is_allowed = False
    reason = ""

    if action == "view_catalog":
        is_allowed = "catalog" in allowed_tiers
        reason = "Đọc danh mục catalog" if is_allowed else "Không có quyền xem catalog"
    else:
        # Kiểm tra theo tầng dữ liệu
        if tier in allowed_tiers:
            # Kiểm tra quyền đọc/ghi
            if action in ("read", "select", "query") and role_info.get("can_read", False):
                is_allowed = True
                reason = f"Được phép: Vai trò [{role_info.get('display_name', role_key)}] có quyền đọc tầng [{tier}]"
            elif action in ("write", "update", "delete", "insert") and role_info.get("can_write", False):
                is_allowed = True
                reason = f"Được phép: Vai trò [{role_info.get('display_name', role_key)}] có quyền ghi tầng [{tier}]"
            else:
                is_allowed = False
                reason = f"Từ chối: Vai trò [{role_info.get('display_name', role_key)}] không có quyền thực hiện hành động '{action}'"
        else:
            is_allowed = False
            reason = f"Từ chối vi phạm bảo mật: Vai trò [{role_info.get('display_name', role_key)}] bị cấm truy cập dữ liệu tầng [{tier}] (bảng '{bang}')"

    # Tự động ghi vào sổ audit log
    if auto_audit:
        result_str = "ALLOWED" if is_allowed else "DENIED"
        try:
            log_event(
                ai=nguoi_dung,
                hanh_dong=f"{action}:{tier}",
                bang=bang,
                ket_qua=result_str,
                chi_tiet=reason,
                db_path=db_path
            )
        except Exception as e:
            print(f"[Warning] Không thể ghi audit log: {e}")

    return is_allowed


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
