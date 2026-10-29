"""
Lớp đọc/ghi dữ liệu CÓ KIỂM SOÁT QUYỀN
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit & Báo cáo)

Mọi lần đọc/ghi bảng dữ liệu (pipeline, dashboard, script phân tích) đều đi qua hai hàm:
- read_table(nguoi_dung, bang)       : kiểm tra quyền đọc -> đọc CSV -> ẩn cột nhạy cảm nếu vai trò
                                       không có can_view_sensitive
- write_table(nguoi_dung, bang, df)  : kiểm tra quyền ghi -> ghi CSV
Bị từ chối thì ném AccessDenied; dòng DENIED đã nằm trong audit_log.

Vị trí file lấy từ cột file_path của catalog (tương đối với gốc repo). Bảng raw gồm nhiều đợt
gửi nên file_path là mẫu, ví dụ data/raw/hospital_A/*/patient.csv -> đọc và nối tất cả các đợt.
"""

import glob
import os
import sys
from typing import List, Optional
import pandas as pd

# Thêm thư mục gốc dự án vào sys.path nếu cần
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from catalog_access.access import AccessDenied, get_hidden_columns, require_access
from catalog_access.audit import log_event
from catalog_access.catalog import get_dataset

ROOT = parent_dir


def to_catalog_path(path: str) -> str:
    """Đường dẫn lưu trong catalog: tương đối với gốc repo nếu file nằm trong repo, không thì tuyệt đối."""
    tuyet_doi = os.path.abspath(path)
    if os.path.commonpath([tuyet_doi, ROOT]) == ROOT:
        return os.path.relpath(tuyet_doi, ROOT)
    return tuyet_doi


def from_catalog_path(path: str) -> str:
    """Ngược lại của to_catalog_path."""
    return path if os.path.isabs(path) else os.path.join(ROOT, path)


def resolve_files(bang: str, path: Optional[str] = None, db_path: Optional[str] = None) -> List[str]:
    """Danh sách file của bảng: `path` nếu truyền vào, không thì theo file_path trong catalog."""
    if path:
        return [path]
    thong_tin = get_dataset(bang, db_path)
    if not thong_tin or not thong_tin.get("file_path"):
        raise FileNotFoundError(f"Bảng '{bang}' chưa đăng ký vị trí file trong catalog")
    cac_file = sorted(glob.glob(from_catalog_path(thong_tin["file_path"])))
    if not cac_file:
        raise FileNotFoundError(f"Không tìm thấy file nào của bảng '{bang}' ({thong_tin['file_path']})")
    return cac_file


def read_table(
    nguoi_dung: str,
    bang: str,
    path: Optional[str] = None,
    db_path: Optional[str] = None,
    policy_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Đọc bảng `bang` với tư cách `nguoi_dung`.
    - Không có quyền đọc tầng của bảng -> AccessDenied (audit: DENIED).
    - Có quyền nhưng không được xem cột nhạy cảm -> các cột đó bị bỏ khỏi kết quả
      (audit thêm một dòng 'mask:<các cột>').
    `path`: đọc đúng file này (pipeline đọc từng đợt), không truyền thì lấy theo catalog.
    """
    require_access(nguoi_dung, bang, "read", policy_path, db_path)

    cac_file = resolve_files(bang, path, db_path)
    if len(cac_file) == 1:
        df = pd.read_csv(cac_file[0])
    else:
        df = pd.concat([pd.read_csv(f) for f in cac_file], ignore_index=True)

    thong_tin = get_dataset(bang, db_path) or {}
    nhay_cam = [c.strip() for c in (thong_tin.get("sensitive_columns") or "").split(",")]
    can_an = [c for c in get_hidden_columns(nguoi_dung, nhay_cam, policy_path) if c in df.columns]
    if can_an:
        df = df.drop(columns=can_an)
        log_event(ai=nguoi_dung, hanh_dong=f"mask:{','.join(can_an)}", bang=bang,
                  ket_qua="SUCCESS", db_path=db_path)
    return df


def write_table(
    nguoi_dung: str,
    bang: str,
    df: pd.DataFrame,
    path: Optional[str] = None,
    db_path: Optional[str] = None,
    policy_path: Optional[str] = None
) -> str:
    """
    Ghi `df` thành file CSV của bảng `bang` với tư cách `nguoi_dung`.
    Không có quyền ghi -> AccessDenied, file KHÔNG bị đụng tới. Trả về đường dẫn đã ghi.
    """
    require_access(nguoi_dung, bang, "write", policy_path, db_path)

    if path is None:
        thong_tin = get_dataset(bang, db_path)
        if not thong_tin or not thong_tin.get("file_path"):
            raise FileNotFoundError(f"Bảng '{bang}' chưa đăng ký vị trí file trong catalog")
        if "*" in thong_tin["file_path"]:
            raise ValueError(f"Bảng '{bang}' gồm nhiều file, phải truyền path cụ thể khi ghi")
        path = from_catalog_path(thong_tin["file_path"])
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    df.to_csv(path, index=False)
    return path


__all__ = ["AccessDenied", "read_table", "write_table", "resolve_files",
           "to_catalog_path", "from_catalog_path"]
