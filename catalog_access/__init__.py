"""
Package catalog_access
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit & Báo cáo)

Cung cấp các hàm chuẩn cho toàn nhóm:
- check_access(nguoi_dung, bang): Kiểm tra quyền truy cập (Người 1, Người 2, cả nhóm dùng)
- log_event(ai, hanh_dong, bang, ket_qua): Ghi sổ audit (Người 1, Người 2, cả nhóm dùng)
- init_database(): Khởi tạo metadata.db dùng chung
- register_dataset(): Đăng ký bảng dữ liệu vào danh mục
- list_datasets(): Liệt kê bảng trong catalog
- get_audit_logs(): Đọc sổ kiểm toán
"""

from catalog_access.init_db import init_database, get_db_connection, DEFAULT_DB_PATH
from catalog_access.audit import log_event, get_audit_logs
from catalog_access.catalog import register_dataset, list_datasets, get_dataset, delete_dataset
from catalog_access.access import check_access, resolve_role, load_policies

__all__ = [
    "init_database",
    "get_db_connection",
    "DEFAULT_DB_PATH",
    "log_event",
    "get_audit_logs",
    "register_dataset",
    "list_datasets",
    "get_dataset",
    "delete_dataset",
    "check_access",
    "resolve_role",
    "load_policies"
]
