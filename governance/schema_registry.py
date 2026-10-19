"""
Schema Registry: lưu lịch sử schema của từng bảng vào bảng `schema_versions`
và phát hiện thay đổi so với phiên bản trước.

Các loại thay đổi:
    NO_CHANGE       không đổi
    COLUMN_ADDED    thêm cột            -> không gây hỏng
    COLUMN_REMOVED  xóa cột             -> gây hỏng
    COLUMN_RENAMED  đổi tên cột         -> gây hỏng
    TYPE_CHANGED    đổi kiểu dữ liệu    -> gây hỏng

Lưu ý: ở đây chỉ PHÁT HIỆN thay đổi. Mức độ cuối cùng (BREAKING hay WARNING) do
governance/gates.py quyết định dựa trên hợp đồng (cột có bắt buộc không, kiểu có hợp không).

Chạy thử: python -m governance.schema_registry
"""

import difflib
import json
from typing import Optional

import pandas as pd
from pandas.api import types as ptypes

from catalog_access.init_db import get_db_connection

# Hai giá trị trung bình chênh nhau không quá 20% thì coi là "phân phối gần giống"
NGUONG_LECH_TRUNG_BINH = 0.20
# Độ giống tên cột (0..1, theo difflib) để coi là cùng một cột bị đổi tên
NGUONG_GIONG_TEN = 0.5

# Dữ liệu mẫu của Người 3 lưu kiểu theo kiểu SQL -> đổi về kiểu logic của ta
KIEU_SQL_CU = {"INTEGER": "int", "REAL": "float", "TEXT": "string", "BOOLEAN": "bool"}


# ---------------------------------------------------------------------------
# 1. Mô tả schema của một DataFrame
# ---------------------------------------------------------------------------

def logical_type(cot: pd.Series) -> str:
    """
    Đổi kiểu pandas sang kiểu logic: int | float | string | bool | datetime | unknown.
    Cột không có giá trị nào (toàn rỗng, hoặc bảng 0 dòng) -> 'unknown' vì không đoán được kiểu.
    """
    if cot.notna().sum() == 0:
        return "unknown"
    if ptypes.is_bool_dtype(cot):
        return "bool"
    if ptypes.is_integer_dtype(cot):
        return "int"
    if ptypes.is_float_dtype(cot):
        return "float"
    if ptypes.is_datetime64_any_dtype(cot):
        return "datetime"
    return "string"


def type_family(kieu: str) -> str:
    """int và float cùng họ 'number' (cột int có ô rỗng thì pandas đọc thành float)."""
    return "number" if kieu in ("int", "float") else kieu


def profile_dataframe(df: pd.DataFrame) -> dict:
    """
    Mô tả từng cột: kiểu logic, % ô rỗng, và giá trị trung bình nếu là cột số.
    Kết quả này được lưu vào schema_versions.columns_json.
    """
    mo_ta = {}
    for ten_cot in df.columns:
        cot = df[ten_cot]
        kieu = logical_type(cot)
        thong_tin = {
            "type": kieu,
            "null_pct": round(float(cot.isna().mean() * 100), 2) if len(cot) else 0.0,
        }
        if kieu in ("int", "float"):
            thong_tin["mean"] = round(float(cot.mean()), 4)
        mo_ta[str(ten_cot)] = thong_tin
    return mo_ta


def _doc_columns_json(chuoi_json: str) -> dict:
    """Đọc columns_json; hỗ trợ cả dạng cũ {"cot": "REAL"} của dữ liệu mẫu."""
    du_lieu = json.loads(chuoi_json)
    ket_qua = {}
    for ten_cot, gia_tri in du_lieu.items():
        if isinstance(gia_tri, str):
            gia_tri = {"type": KIEU_SQL_CU.get(gia_tri.upper(), gia_tri.lower())}
        ket_qua[ten_cot] = gia_tri
    return ket_qua


# ---------------------------------------------------------------------------
# 2. So sánh hai schema
# ---------------------------------------------------------------------------

def _cung_kieu(kieu_a: str, kieu_b: str) -> bool:
    """Hai kiểu tương thích (cùng họ). 'unknown' thì coi như khớp mọi kiểu."""
    if "unknown" in (kieu_a, kieu_b):
        return True
    return type_family(kieu_a) == type_family(kieu_b)


def _phan_phoi_gan_giong(cu: dict, moi: dict) -> bool:
    """So giá trị trung bình của hai cột số: lệch không quá 20% thì coi là gần giống."""
    if "mean" not in cu or "mean" not in moi:
        return False
    a, b = cu["mean"], moi["mean"]
    if pd.isna(a) or pd.isna(b):
        return False
    return abs(a - b) <= NGUONG_LECH_TRUNG_BINH * max(abs(a), abs(b), 1e-9)


def _tim_cot_doi_ten(cot_mat: list, cot_moi: list, cu: dict, moi: dict) -> list[tuple]:
    """
    Heuristic phát hiện đổi tên: một cột biến mất (cot_mat) và một cột mới xuất hiện (cot_moi)
    được ghép thành cặp đổi tên nếu CÙNG KIỂU và thỏa ít nhất một điều:
      - phân phối giá trị gần giống (trung bình lệch <= 20%), hoặc
      - tên gần giống (difflib >= 0.5), hoặc
      - chỉ có đúng 1 cột mất và 1 cột mới.
    Trả về danh sách cặp (tên_cũ, tên_mới).
    """
    cap_doi_ten = []
    con_lai = list(cot_moi)
    chi_mot_cap = len(cot_mat) == 1 and len(cot_moi) == 1

    for ten_cu in cot_mat:
        ung_vien = []
        for ten_moi in con_lai:
            if not _cung_kieu(cu[ten_cu]["type"], moi[ten_moi]["type"]):
                continue
            giong_phan_phoi = _phan_phoi_gan_giong(cu[ten_cu], moi[ten_moi])
            giong_ten = difflib.SequenceMatcher(None, ten_cu.lower(), ten_moi.lower()).ratio()
            if giong_phan_phoi or giong_ten >= NGUONG_GIONG_TEN or chi_mot_cap:
                ung_vien.append((giong_phan_phoi, giong_ten, ten_moi))
        if ung_vien:
            # Ưu tiên cột giống phân phối, sau đó tới cột giống tên nhất
            ung_vien.sort(reverse=True)
            ten_moi = ung_vien[0][2]
            cap_doi_ten.append((ten_cu, ten_moi))
            con_lai.remove(ten_moi)
    return cap_doi_ten


def compare_schemas(cu: dict, moi: dict) -> list[dict]:
    """
    So sánh schema cũ và mới (dạng {cột: {"type": ...}}), trả về danh sách thay đổi.
    Mỗi thay đổi là dict: change_type, column, new_column, old_type, new_type, is_breaking, message.
    """
    thay_doi = []
    cot_mat = [c for c in cu if c not in moi]
    cot_moi = [c for c in moi if c not in cu]

    # 1. Đổi tên (ghép cặp cột mất + cột mới)
    cap_doi_ten = _tim_cot_doi_ten(cot_mat, cot_moi, cu, moi)
    for ten_cu, ten_moi in cap_doi_ten:
        thay_doi.append({
            "change_type": "COLUMN_RENAMED", "column": ten_cu, "new_column": ten_moi,
            "old_type": cu[ten_cu]["type"], "new_type": moi[ten_moi]["type"], "is_breaking": True,
            "message": f"Cột '{ten_cu}' bị đổi tên thành '{ten_moi}'",
        })
    da_ghep_cu = {a for a, _ in cap_doi_ten}
    da_ghep_moi = {b for _, b in cap_doi_ten}

    # 2. Xóa cột (mất mà không ghép được)
    for ten in cot_mat:
        if ten not in da_ghep_cu:
            thay_doi.append({
                "change_type": "COLUMN_REMOVED", "column": ten, "new_column": None,
                "old_type": cu[ten]["type"], "new_type": None, "is_breaking": True,
                "message": f"Cột '{ten}' đã bị xóa",
            })

    # 3. Thêm cột
    for ten in cot_moi:
        if ten not in da_ghep_moi:
            thay_doi.append({
                "change_type": "COLUMN_ADDED", "column": ten, "new_column": ten,
                "old_type": None, "new_type": moi[ten]["type"], "is_breaking": False,
                "message": f"Thêm cột mới '{ten}' (kiểu {moi[ten]['type']})",
            })

    # 4. Đổi kiểu (cột có ở cả hai phiên bản)
    for ten in cu:
        if ten in moi and not _cung_kieu(cu[ten]["type"], moi[ten]["type"]):
            thay_doi.append({
                "change_type": "TYPE_CHANGED", "column": ten, "new_column": ten,
                "old_type": cu[ten]["type"], "new_type": moi[ten]["type"], "is_breaking": True,
                "message": f"Cột '{ten}' đổi kiểu từ {cu[ten]['type']} sang {moi[ten]['type']}",
            })
    return thay_doi


# ---------------------------------------------------------------------------
# 3. Đọc / ghi bảng schema_versions
# ---------------------------------------------------------------------------

def get_latest_schema(table_name: str, db_path: Optional[str] = None) -> tuple[int, Optional[dict]]:
    """Trả về (version, schema) mới nhất của bảng; chưa có thì (0, None)."""
    conn = get_db_connection(db_path)
    dong = conn.execute(
        "SELECT version, columns_json FROM schema_versions WHERE table_name = ? "
        "ORDER BY version DESC LIMIT 1", (table_name,)
    ).fetchone()
    conn.close()
    if dong is None:
        return 0, None
    return dong["version"], _doc_columns_json(dong["columns_json"])


def get_baseline_schema(table_name: str, db_path: Optional[str] = None) -> tuple[int, Optional[dict]]:
    """
    Phiên bản CHUẨN để so sánh: phiên bản hợp lệ (is_breaking = 0) mới nhất.
    Nhờ vậy một lô dữ liệu lỗi đã bị chặn không trở thành "chuẩn mới" cho các lô sau.
    Chưa có phiên bản hợp lệ nào thì dùng phiên bản mới nhất.
    """
    conn = get_db_connection(db_path)
    dong = conn.execute(
        "SELECT version, columns_json FROM schema_versions WHERE table_name = ? AND is_breaking = 0 "
        "ORDER BY version DESC LIMIT 1", (table_name,)
    ).fetchone()
    conn.close()
    if dong is None:
        return get_latest_schema(table_name, db_path)
    return dong["version"], _doc_columns_json(dong["columns_json"])


def set_schema_breaking(table_name: str, version: int, is_breaking: bool,
                        db_path: Optional[str] = None) -> None:
    """Cập nhật cờ is_breaking của một phiên bản (gates.py gọi sau khi đối chiếu hợp đồng)."""
    conn = get_db_connection(db_path)
    conn.execute("UPDATE schema_versions SET is_breaking = ? WHERE table_name = ? AND version = ?",
                 (int(is_breaking), table_name, version))
    conn.commit()
    conn.close()


def get_schema_history(table_name: str, db_path: Optional[str] = None) -> list[dict]:
    """Toàn bộ lịch sử phiên bản schema của một bảng (cũ -> mới)."""
    conn = get_db_connection(db_path)
    dong = conn.execute(
        "SELECT version, columns_json, change_type, is_breaking, recorded_at "
        "FROM schema_versions WHERE table_name = ? ORDER BY version", (table_name,)
    ).fetchall()
    conn.close()
    return [dict(d) for d in dong]


def register_schema(table_name: str, df: pd.DataFrame, db_path: Optional[str] = None,
                    save: bool = True) -> dict:
    """
    Ghi nhận schema của `df` cho bảng `table_name` và so với phiên bản CHUẨN
    (phiên bản hợp lệ gần nhất, xem get_baseline_schema).

    - Lần đầu: tạo version 1 (change_type = 'INITIAL').
    - Schema khác phiên bản mới nhất trong DB: tạo version mới = version mới nhất + 1.
    - Schema giống phiên bản mới nhất: KHÔNG tạo version mới
      (nhờ vậy 3 đợt snapshot giống nhau không sinh ra 3 phiên bản trùng).
    - save=False: chỉ so sánh, không ghi DB (dùng để chạy thử).

    Trả về dict: table_name, version, baseline_version, changes (list), is_breaking,
    change_type, is_new_version.
    """
    schema_moi = profile_dataframe(df)
    version_moi_nhat, schema_moi_nhat = get_latest_schema(table_name, db_path)
    version_chuan, schema_chuan = get_baseline_schema(table_name, db_path)

    if schema_chuan is None:
        thay_doi = []
        tom_tat = "INITIAL"
    else:
        thay_doi = compare_schemas(schema_chuan, schema_moi)
        tom_tat = ("; ".join(f"{t['change_type']}: {t['message']}" for t in thay_doi)
                   if thay_doi else "NO_CHANGE")
        if version_chuan != version_moi_nhat and not thay_doi:
            tom_tat = f"NO_CHANGE (khớp lại phiên bản chuẩn v{version_chuan})"

    co_version_moi = schema_moi_nhat is None or bool(compare_schemas(schema_moi_nhat, schema_moi))
    version = version_moi_nhat + 1 if co_version_moi else version_moi_nhat
    gay_hong = any(t["is_breaking"] for t in thay_doi)

    if save and co_version_moi:
        conn = get_db_connection(db_path)
        conn.execute(
            "INSERT INTO schema_versions (table_name, version, columns_json, change_type, is_breaking) "
            "VALUES (?, ?, ?, ?, ?)",
            (table_name, version, json.dumps(schema_moi, ensure_ascii=False), tom_tat, int(gay_hong)),
        )
        conn.commit()
        conn.close()

    return {
        "table_name": table_name,
        "version": version,
        "baseline_version": version_chuan,
        "changes": thay_doi,
        "is_breaking": gay_hong,
        "change_type": tom_tat,
        "is_new_version": co_version_moi,
    }


if __name__ == "__main__":
    import os
    import tempfile

    from catalog_access.init_db import init_database
    from governance.sample_data import make_patient

    db = os.path.join(tempfile.mkdtemp(), "thu_schema.db")
    init_database(db, seed_sample_data=False)

    sach = make_patient("B")
    print("1) Lần đầu:", register_schema("raw_patient_hospital_B", sach, db)["change_type"])
    print("2) Gửi lại y hệt:", register_schema("raw_patient_hospital_B", sach, db)["change_type"])
    loi = sach.rename(columns={"admissionweight": "weight"})
    kq = register_schema("raw_patient_hospital_B", loi, db)
    print(f"3) Đổi tên admissionweight -> weight: version {kq['version']}, "
          f"gây hỏng = {kq['is_breaking']}\n   {kq['change_type']}")
