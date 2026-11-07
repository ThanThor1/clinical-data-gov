"""
Sổ đăng ký nguồn dữ liệu (sources/*.yaml): các HỆ THỐNG NGUỒN khác nhau gửi dữ liệu vào nền tảng.

Mỗi nguồn khai báo định dạng gốc, giấy phép, các viện thuộc nguồn, và cách ánh xạ về SCHEMA CHUẨN của tầng
raw (chính là các hợp đồng contracts/raw_*.yaml). Từ sổ đăng ký:
  - pipeline/source_adapters.py chuyển dữ liệu gốc về schema chuẩn (nguồn format != csv_wide)
  - record_source_lineage: lineage từ tham số GỐC của nguồn tới bảng raw (nối tiếp lineage raw -> AI-ready),
    sinh từ chính file ánh xạ nên không lệch với bộ chuyển
  - check_source_semantics: nguồn định nghĩa một cột khác hợp đồng (ví dụ nhãn tử vong trong bệnh viện
    so với lúc ra ICU) -> cảnh báo SEMANTIC_MISMATCH (dữ liệu vẫn nhận, nhưng người dùng phải biết)
  - catalog lưu hệ thống nguồn và định dạng của từng bảng raw (pipeline/ingest_raw.py)

Chạy thử: python -m governance.sources
"""

import copy
import glob
import os
from typing import Iterable, Optional

import yaml

from governance.contracts import contract_label, find_contract
from governance.lineage import record_lineage

SOURCES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sources"))
LOAI_BANG = ("patient", "vital", "lab")

_BO_NHO_DEM: dict = {}


def load_sources(sources_dir: Optional[str] = None) -> list[dict]:
    """Các nguồn trong sổ đăng ký (đọc lại khi file YAML đổi). Đúng một nguồn có `hospitals: default`."""
    thu_muc = os.path.abspath(sources_dir or SOURCES_DIR)
    cac_file = sorted(glob.glob(os.path.join(thu_muc, "*.yaml")))
    dau_vet = tuple((f, os.stat(f).st_mtime_ns) for f in cac_file)
    da_luu = _BO_NHO_DEM.get(thu_muc)
    if da_luu is None or da_luu[0] != dau_vet:
        cac_nguon = []
        for duong_dan in cac_file:
            with open(duong_dan, "r", encoding="utf-8") as f:
                nguon = yaml.safe_load(f)
            for truong in ("name", "system", "format", "hospitals"):
                if truong not in nguon:
                    raise ValueError(f"Nguồn '{os.path.basename(duong_dan)}' thiếu trường '{truong}'")
            if nguon["hospitals"] != "default":
                nguon["hospitals"] = [str(m) for m in nguon["hospitals"]]
            cac_nguon.append(nguon)
        if sum(1 for n in cac_nguon if n["hospitals"] == "default") != 1:
            raise ValueError("Sổ đăng ký nguồn phải có đúng một nguồn 'hospitals: default'")
        _BO_NHO_DEM[thu_muc] = da_luu = (dau_vet, cac_nguon)
    return copy.deepcopy(da_luu[1])


def source_of_hospital(ma_vien: str, sources_dir: Optional[str] = None) -> dict:
    """Nguồn của viện `ma_vien`: nguồn liệt kê mã này, không thì nguồn mặc định."""
    cac_nguon = load_sources(sources_dir)
    for nguon in cac_nguon:
        if nguon["hospitals"] != "default" and str(ma_vien) in nguon["hospitals"]:
            return nguon
    return next(n for n in cac_nguon if n["hospitals"] == "default")


def is_default_source(ma_vien: str, sources_dir: Optional[str] = None) -> bool:
    return source_of_hospital(ma_vien, sources_dir)["hospitals"] == "default"


def native_table(nguon: dict, tep: str = "records") -> str:
    """Tên nút lineage cho dữ liệu GỐC của nguồn, ví dụ 'nguon_challenge2012' / 'nguon_challenge2012_outcomes'."""
    return f"nguon_{nguon['name']}" + ("" if tep == "records" else f"_{tep}")


def mapping_edges(nguon: dict, ma_vien: str) -> list[tuple[str, str, str, str, str]]:
    """
    Các cạnh lineage (bảng gốc, tham số gốc, bảng raw, cột chuẩn, mô tả biến đổi) sinh từ file ánh xạ của nguồn.
    Nguồn csv_wide (đúng schema chuẩn) không có cạnh nào.
    """
    if nguon["format"] == "csv_wide":
        return []
    canh = []
    thieu = f"; {nguon['missing_value']} -> trống" if "missing_value" in nguon else ""
    ma_id = nguon["id_parameter"]
    for loai in LOAI_BANG:
        bang = f"raw_{loai}_hospital_{ma_vien}"
        canh.append((native_table(nguon), ma_id, bang, "patientunitstayid", "mã lượt nằm ICU"))
        if loai in ("vital", "lab"):
            cot_tg = "observationoffset" if loai == "vital" else "labresultoffset"
            canh.append((native_table(nguon), nguon["time_column"], bang, cot_tg, "hh:mm -> số phút"))
    for cot, luat in nguon.get("patient", {}).items():
        if luat["from"] == ma_id:
            continue
        mo_ta = [f"luật: {luat['rule']}"] if "rule" in luat else []
        if "values" in luat:
            mo_ta.append(", ".join(f"{k} -> {v}" for k, v in luat["values"].items()))
        canh.append((native_table(nguon, luat.get("file", "records")), luat["from"],
                     f"raw_patient_hospital_{ma_vien}", cot, "; ".join(mo_ta) + thieu if mo_ta else "direct" + thieu))
    for cot, luat in nguon.get("vital", {}).items():
        canh.append((native_table(nguon), luat["from"], f"raw_vital_hospital_{ma_vien}", cot, "direct" + thieu))
    for tham_so, luat in nguon.get("lab", {}).items():
        mo_ta = f"labname = '{luat['to']}'" + (f", x{luat['scale']} ({luat.get('note', '')})" if "scale" in luat else "")
        canh.append((native_table(nguon), tham_so, f"raw_lab_hospital_{ma_vien}", "labresult", mo_ta))
    return canh


def record_source_lineage(cac_vien: Iterable[str], db_path: Optional[str] = None) -> int:
    """Ghi lineage tham số gốc -> bảng raw cho mọi viện thuộc nguồn có ánh xạ. Trả về số cạnh."""
    so_canh = 0
    for ma in cac_vien:
        for nguon_bang, nguon_cot, dich_bang, dich_cot, mo_ta in mapping_edges(source_of_hospital(ma), ma):
            record_lineage(nguon_bang, dich_bang, {nguon_cot: dich_cot}, mo_ta, db_path)
            so_canh += 1
    return so_canh


def check_source_semantics(cac_vien: Iterable[str], db_path: Optional[str] = None) -> list[dict]:
    """
    So `semantics` của nguồn với `semantics` của cột trong hợp đồng raw. Khác nhau -> cảnh báo SEMANTIC_MISMATCH
    (không chặn: dữ liệu vẫn dùng được nhưng mang định nghĩa khác). Trả về các vi phạm đã ghi.
    """
    from governance.violation_engine import report_violation

    da_ghi = []
    for ma in cac_vien:
        nguon = source_of_hospital(ma)
        for cot, nghia in (nguon.get("semantics") or {}).items():
            for loai in LOAI_BANG:
                bang = f"raw_{loai}_hospital_{ma}"
                hop_dong = find_contract(bang)
                luat = (hop_dong or {}).get("columns", {}).get(cot)
                if not luat or not luat.get("semantics") or luat["semantics"] == nghia:
                    continue
                da_ghi.append(report_violation(
                    "SEMANTIC_MISMATCH", "WARNING", bang,
                    f"Nguồn '{nguon['name']}' định nghĩa '{cot}' là '{nghia}', hợp đồng {contract_label(hop_dong)} "
                    f"định nghĩa là '{luat['semantics']}' -> dữ liệu vẫn được nhận nhưng tầng sau gộp hai định nghĩa",
                    column=cot, blocked=False, db_path=db_path))
    return da_ghi


if __name__ == "__main__":
    for n in load_sources():
        vien = "mọi viện còn lại" if n["hospitals"] == "default" else ", ".join(n["hospitals"])
        print(f"- {n['name']}: {n['system']} | định dạng {n['format']} | {n.get('license', '')} | viện: {vien}")
        for canh in mapping_edges(n, "x" if n["hospitals"] == "default" else n["hospitals"][0])[:8]:
            print(f"    {canh[0]}.{canh[1]} -> {canh[2]}.{canh[3]}  [{canh[4]}]")
