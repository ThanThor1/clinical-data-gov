"""
Đọc hợp đồng dữ liệu (data contract) từ các file YAML trong thư mục contracts/.

Mỗi hợp đồng có trường `applies_to` là danh sách tên bảng, cho phép dùng mẫu `*`
(ví dụ "raw_patient_hospital_*" khớp với raw_patient_hospital_A, _B, _C).

Chạy thử: python -m governance.contracts
"""

import fnmatch
import os
from typing import Optional

import yaml

# Thư mục contracts/ nằm ở gốc repo (tính từ vị trí file này)
CONTRACTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "contracts"))

# Các kiểu dữ liệu được phép khai báo trong hợp đồng
CAC_KIEU_HOP_LE = {"int", "float", "string", "category"}

# Giá trị mặc định cho một cột nếu hợp đồng không ghi
MAC_DINH_COT = {
    "required": False,
    "nullable": True,
    "max_null_pct": None,
    "min": None,
    "max": None,
    "allowed_values": None,
    "sensitive": False,
}


def _chuan_hoa_hop_dong(hop_dong: dict, ten_file: str) -> dict:
    """Kiểm tra hợp đồng có đủ trường cần thiết và điền giá trị mặc định cho từng cột."""
    for truong in ("name", "applies_to", "owner", "version", "columns"):
        if truong not in hop_dong:
            raise ValueError(f"Hợp đồng '{ten_file}' thiếu trường bắt buộc '{truong}'")

    # applies_to có thể ghi 1 chuỗi hoặc 1 danh sách -> đưa về danh sách
    if isinstance(hop_dong["applies_to"], str):
        hop_dong["applies_to"] = [hop_dong["applies_to"]]

    cot_da_chuan_hoa = {}
    for ten_cot, luat in (hop_dong["columns"] or {}).items():
        luat = {**MAC_DINH_COT, **(luat or {})}
        if luat.get("type") not in CAC_KIEU_HOP_LE:
            raise ValueError(
                f"Hợp đồng '{ten_file}', cột '{ten_cot}': kiểu '{luat.get('type')}' không hợp lệ "
                f"(chỉ chấp nhận {sorted(CAC_KIEU_HOP_LE)})"
            )
        cot_da_chuan_hoa[ten_cot] = luat
    hop_dong["columns"] = cot_da_chuan_hoa
    hop_dong["file"] = ten_file
    return hop_dong


def load_contracts(contracts_dir: Optional[str] = None) -> list[dict]:
    """Đọc tất cả file *.yaml trong thư mục hợp đồng, trả về danh sách hợp đồng."""
    thu_muc = contracts_dir or CONTRACTS_DIR
    danh_sach = []
    for ten_file in sorted(os.listdir(thu_muc)):
        if not ten_file.endswith((".yaml", ".yml")):
            continue
        with open(os.path.join(thu_muc, ten_file), "r", encoding="utf-8") as f:
            hop_dong = yaml.safe_load(f)
        danh_sach.append(_chuan_hoa_hop_dong(hop_dong, ten_file))
    return danh_sach


def find_contract(ten_bang: str, contracts_dir: Optional[str] = None) -> Optional[dict]:
    """
    Tìm hợp đồng áp dụng cho `ten_bang`.
    Ưu tiên hợp đồng ghi đúng tên bảng; nếu không có thì dùng hợp đồng có mẫu `*` khớp.
    Không tìm thấy -> trả về None.
    """
    hop_dong_list = load_contracts(contracts_dir)

    # 1. Khớp chính xác tên bảng
    for hop_dong in hop_dong_list:
        if ten_bang in hop_dong["applies_to"]:
            return hop_dong

    # 2. Khớp theo mẫu (ví dụ raw_patient_hospital_*)
    for hop_dong in hop_dong_list:
        for mau in hop_dong["applies_to"]:
            if fnmatch.fnmatchcase(ten_bang, mau):
                return hop_dong
    return None


def get_sensitive_columns(hop_dong: dict) -> list[str]:
    """Danh sách cột nhạy cảm (sensitive: true) của hợp đồng, dùng cho register_dataset."""
    return [ten for ten, luat in hop_dong["columns"].items() if luat.get("sensitive")]


def get_required_columns(hop_dong: dict) -> list[str]:
    """Danh sách cột bắt buộc (required: true) của hợp đồng."""
    return [ten for ten, luat in hop_dong["columns"].items() if luat.get("required")]


if __name__ == "__main__":
    print(f"Thư mục hợp đồng: {CONTRACTS_DIR}\n")
    for hd in load_contracts():
        print(f"- {hd['name']} (v{hd['version']}, chủ sở hữu: {hd['owner']})")
        print(f"    áp dụng cho : {', '.join(hd['applies_to'])}")
        print(f"    cột bắt buộc: {', '.join(get_required_columns(hd))}")
        print(f"    cột nhạy cảm: {', '.join(get_sensitive_columns(hd)) or '(không có)'}")
