"""
Cổng kiểm tra dữ liệu (quality gates). Dữ liệu chỉ được lên tầng trên khi qua cổng.

    gate1 - Schema + hợp đồng:
        * ghi schema vào schema_registry, phát hiện đổi tên / xóa / đổi kiểu cột
        * cột bắt buộc phải có mặt, kiểu dữ liệu phải đúng hợp đồng
    gate2 - Chất lượng theo khung Kahn:
        * Conformance  (tuân thủ) : đúng kiểu số, giá trị thuộc allowed_values
        * Completeness (đầy đủ)   : không vượt max_null_pct, cột nullable: false không có ô trống
        * Plausibility (hợp lý)   : giá trị trong [min, max]

Mức độ:
    BREAKING -> chặn dữ liệu (check_gate trả False)
    WARNING  -> cho qua nhưng ghi lại vào violations
    INFO     -> chỉ để biết (ví dụ thêm cột mới), không ghi violations

Ngưỡng 1% (CLAUDE.md mục 7): lỗi giá trị (sai định dạng / ngoài khoảng) chỉ BREAKING khi
> 1% số dòng bị lỗi; ít hơn thì WARNING.

Hàm dùng chung (CLAUDE.md mục 4): check_gate(ten_bang, du_lieu, cong) -> (dat, danh_sach_loi)

Chạy thử: python -m governance.gates
"""

from typing import Optional

import pandas as pd

from catalog_access import log_event
from governance.contracts import find_contract
from governance.schema_registry import logical_type, register_schema, set_schema_breaking
from governance.violation_engine import report_violation

CONG_HOP_LE = ("gate1", "gate2")

# Lỗi giá trị chỉ chặn khi vượt quá 1% số dòng
NGUONG_TY_LE_DONG = 1.0

# Loại thay đổi trong schema_registry -> loại lỗi governance
LOI_THEO_THAY_DOI = {
    "COLUMN_RENAMED": "SCHEMA_COLUMN_RENAMED",
    "COLUMN_REMOVED": "SCHEMA_COLUMN_REMOVED",
    "TYPE_CHANGED": "SCHEMA_TYPE_CHANGED",
    "COLUMN_ADDED": "SCHEMA_COLUMN_ADDED",
}


def _van_de(rule: str, error_type: str, severity: str, column: Optional[str], message: str,
            observed=None, expected=None) -> dict:
    """Tạo một dict mô tả vấn đề (dùng thống nhất trong cả gate1 và gate2)."""
    return {
        "rule": rule,
        "error_type": error_type,
        "severity": severity,
        "column": column,
        "message": message,
        "observed": observed,
        "expected": expected,
    }


# ---------------------------------------------------------------------------
# Hàm phụ
# ---------------------------------------------------------------------------

def _o_rong(cot: pd.Series) -> pd.Series:
    """Ô trống = NaN/None hoặc chuỗi chỉ có khoảng trắng."""
    return cot.isna() | cot.map(lambda x: isinstance(x, str) and x.strip() == "")


def _hop_kieu(cot: pd.Series, kieu_hop_dong: str) -> tuple[bool, str]:
    """
    Kiểm tra kiểu thực tế của cột có hợp với kiểu ghi trong hợp đồng không.
    Trả về (hợp?, kiểu thực tế).
      - string / category : chấp nhận mọi kiểu (ví dụ age raw lúc là số, lúc là "> 89")
      - float             : chấp nhận int hoặc float
      - int               : chấp nhận int, hoặc float mà mọi giá trị đều là số nguyên (40.0)
      - cột không có giá trị nào ('unknown') : bỏ qua
    """
    kieu = logical_type(cot)
    if kieu == "unknown" or kieu_hop_dong in ("string", "category"):
        return True, kieu
    if kieu_hop_dong == "float":
        return kieu in ("int", "float"), kieu
    if kieu_hop_dong == "int":
        if kieu == "int":
            return True, kieu
        if kieu == "float":
            return bool((cot.dropna() % 1 == 0).all()), kieu
    return False, kieu


def _theo_nguong(so_dong_loi: int, tong_dong: int, rule: str, error_type: str, column: str,
                 mo_ta: str, expected: str, vi_du: list) -> Optional[dict]:
    """Áp dụng luật 1%: > 1% số dòng lỗi -> BREAKING, có lỗi nhưng <= 1% -> WARNING."""
    if so_dong_loi == 0 or tong_dong == 0:
        return None
    ty_le = so_dong_loi / tong_dong * 100
    muc_do = "BREAKING" if ty_le > NGUONG_TY_LE_DONG else "WARNING"
    vi_du_str = ", ".join(str(v) for v in vi_du[:3])
    return _van_de(
        rule, error_type, muc_do, column,
        f"{so_dong_loi}/{tong_dong} dòng ({ty_le:.2f}%) {mo_ta}; ví dụ: {vi_du_str}",
        observed=f"{ty_le:.2f}% dòng lỗi",
        expected=f"{expected}; tối đa {NGUONG_TY_LE_DONG:g}% dòng lỗi",
    )


# ---------------------------------------------------------------------------
# Gate 1: schema + hợp đồng
# ---------------------------------------------------------------------------

def _gate1(ten_bang: str, df: pd.DataFrame, hop_dong: Optional[dict],
           db_path: Optional[str], ghi_nhan: bool) -> list[dict]:
    van_de = []
    luat_cot = hop_dong["columns"] if hop_dong else {}
    if hop_dong is None:
        van_de.append(_van_de("contract.exists", "NO_CONTRACT", "WARNING", None,
                              f"Bảng '{ten_bang}' chưa có hợp đồng dữ liệu, chỉ ghi nhận schema",
                              observed="không có hợp đồng", expected="có file contracts/*.yaml"))

    # 1. So sánh schema với phiên bản chuẩn trong schema_registry
    dang_ky = register_schema(ten_bang, df, db_path, save=ghi_nhan)
    da_bao = set()     # cột đã báo đổi tên / xóa -> không báo thêm "thiếu cột bắt buộc"
    for thay_doi in dang_ky["changes"]:
        cot = thay_doi["column"]
        luat = luat_cot.get(cot)
        bat_buoc = bool(luat and luat["required"])
        loai_loi = LOI_THEO_THAY_DOI[thay_doi["change_type"]]

        if thay_doi["change_type"] == "COLUMN_ADDED":
            van_de.append(_van_de("schema.column_added", loai_loi, "INFO", cot, thay_doi["message"],
                                  observed=thay_doi["new_type"], expected=None))

        elif thay_doi["change_type"] in ("COLUMN_RENAMED", "COLUMN_REMOVED"):
            da_bao.add(cot)
            muc_do = "BREAKING" if bat_buoc else "WARNING"
            ghi_chu = "cột BẮT BUỘC theo hợp đồng" if bat_buoc else "cột không bắt buộc"
            van_de.append(_van_de(
                f"schema.{thay_doi['change_type'].lower()}", loai_loi, muc_do, cot,
                f"{thay_doi['message']} ({ghi_chu}, so với schema v{dang_ky['baseline_version']})",
                observed=thay_doi["new_column"] or "(không còn cột này)", expected=cot))

        elif thay_doi["change_type"] == "TYPE_CHANGED":
            if luat is None:
                van_de.append(_van_de("schema.type_changed", loai_loi, "WARNING", cot,
                                      thay_doi["message"] + " (cột ngoài hợp đồng)",
                                      observed=thay_doi["new_type"], expected=thay_doi["old_type"]))
            elif _hop_kieu(df[cot], luat["type"])[0]:
                van_de.append(_van_de("schema.type_changed", loai_loi, "INFO", cot,
                                      thay_doi["message"] + " (vẫn đúng kiểu trong hợp đồng)",
                                      observed=thay_doi["new_type"], expected=luat["type"]))
            # Trường hợp sai kiểu so với hợp đồng: phần kiểm tra hợp đồng bên dưới sẽ báo

    # 2. Đối chiếu hợp đồng: cột bắt buộc có mặt, đúng kiểu
    for cot, luat in luat_cot.items():
        if cot not in df.columns:
            if luat["required"] and cot not in da_bao:
                van_de.append(_van_de("contract.required_column", "MISSING_REQUIRED_COLUMN",
                                      "BREAKING", cot, f"Thiếu cột bắt buộc '{cot}'",
                                      observed="không có cột", expected=f"cột '{cot}' ({luat['type']})"))
            continue
        hop, kieu_thuc = _hop_kieu(df[cot], luat["type"])
        if not hop:
            van_de.append(_van_de(
                "contract.type", "SCHEMA_TYPE_CHANGED",
                "BREAKING" if luat["required"] else "WARNING", cot,
                f"Cột '{cot}' có kiểu {kieu_thuc}, hợp đồng yêu cầu {luat['type']}",
                observed=kieu_thuc, expected=luat["type"]))

    # 3. Đánh dấu phiên bản schema vừa ghi là gây hỏng hay không (theo kết luận của gate)
    if ghi_nhan and dang_ky["is_new_version"]:
        co_chan = any(v["severity"] == "BREAKING" for v in van_de)
        set_schema_breaking(ten_bang, dang_ky["version"], co_chan, db_path)
    return van_de


# ---------------------------------------------------------------------------
# Gate 2: chất lượng theo khung Kahn
# ---------------------------------------------------------------------------

def _gate2(ten_bang: str, df: pd.DataFrame, hop_dong: Optional[dict]) -> list[dict]:
    if hop_dong is None:
        return [_van_de("contract.exists", "NO_CONTRACT", "WARNING", None,
                        f"Bảng '{ten_bang}' chưa có hợp đồng, bỏ qua kiểm tra chất lượng",
                        observed="không có hợp đồng", expected="có file contracts/*.yaml")]

    van_de = []
    tong = len(df)
    if tong == 0:
        return van_de    # bảng rỗng (ví dụ vital của viện B): không có gì để kiểm tra

    for cot, luat in hop_dong["columns"].items():
        if cot not in df.columns:
            continue     # thiếu cột là việc của gate1
        gia_tri = df[cot]
        rong = _o_rong(gia_tri)
        co_gia_tri = gia_tri[~rong]

        # --- Completeness: ô trống ---
        so_rong = int(rong.sum())
        ty_le_rong = so_rong / tong * 100
        if not luat["nullable"] and so_rong > 0:
            van_de.append(_van_de("completeness.nullable", "COMPLETENESS", "BREAKING", cot,
                                  f"Cột '{cot}' không được trống nhưng có {so_rong} ô trống",
                                  observed=f"{so_rong} ô trống", expected="0 ô trống"))
        elif luat["max_null_pct"] is not None and ty_le_rong > luat["max_null_pct"]:
            van_de.append(_van_de("completeness.max_null_pct", "COMPLETENESS", "BREAKING", cot,
                                  f"Cột '{cot}' trống {ty_le_rong:.1f}%, vượt ngưỡng {luat['max_null_pct']}%",
                                  observed=f"{ty_le_rong:.1f}%", expected=f"<= {luat['max_null_pct']}%"))

        # --- Conformance + Plausibility cho cột số ---
        if luat["type"] in ("int", "float"):
            so = pd.to_numeric(co_gia_tri, errors="coerce")
            sai_dinh_dang = so.isna()
            if luat["type"] == "int":
                sai_dinh_dang |= so.notna() & (so % 1 != 0)
            loi = _theo_nguong(int(sai_dinh_dang.sum()), tong, "conformance.type", "CONFORMANCE",
                               cot, f"không phải số {luat['type']}", f"kiểu {luat['type']}",
                               list(co_gia_tri[sai_dinh_dang]))
            if loi:
                van_de.append(loi)

            so_hop_le = so[~sai_dinh_dang]
            if luat["min"] is not None or luat["max"] is not None:
                thap = luat["min"] if luat["min"] is not None else float("-inf")
                cao = luat["max"] if luat["max"] is not None else float("inf")
                ngoai = (so_hop_le < thap) | (so_hop_le > cao)
                loi = _theo_nguong(int(ngoai.sum()), tong, "plausibility.range", "PLAUSIBILITY",
                                   cot, f"nằm ngoài khoảng [{luat['min']}, {luat['max']}]",
                                   f"trong [{luat['min']}, {luat['max']}]", list(so_hop_le[ngoai]))
                if loi:
                    van_de.append(loi)

        # --- Conformance: giá trị cho phép ---
        if luat["allowed_values"]:
            cho_phep = {str(v) for v in luat["allowed_values"]}
            sai = ~co_gia_tri.map(lambda x: str(x).strip()).isin(cho_phep)
            loi = _theo_nguong(int(sai.sum()), tong, "conformance.allowed_values", "CONFORMANCE",
                               cot, f"có giá trị ngoài tập {sorted(cho_phep)}",
                               f"thuộc {sorted(cho_phep)}", list(co_gia_tri[sai].unique()))
            if loi:
                van_de.append(loi)
    return van_de


# ---------------------------------------------------------------------------
# Hàm công khai
# ---------------------------------------------------------------------------

def check_gate_detailed(ten_bang: str, du_lieu: pd.DataFrame, cong: str,
                        db_path: Optional[str] = None, ghi_nhan: bool = True) -> list[dict]:
    """
    Chạy cổng kiểm tra và trả về danh sách vấn đề chi tiết, mỗi vấn đề là dict:
        rule, error_type, severity, column, message, observed, expected
    (nếu đã ghi vi phạm: thêm violation_id, affected_ai_tables).

    ghi_nhan=True : lưu schema, ghi violations (BREAKING/WARNING) và audit_log.
    ghi_nhan=False: chạy thử, không ghi gì vào DB.

    Lưu ý: check_gate gọi hàm này bên trong -> mỗi lô dữ liệu chỉ gọi MỘT trong hai hàm.
    """
    if cong not in CONG_HOP_LE:
        raise ValueError(f"Cổng '{cong}' không hợp lệ, chỉ nhận {CONG_HOP_LE}")

    hop_dong = find_contract(ten_bang)
    if cong == "gate1":
        van_de = _gate1(ten_bang, du_lieu, hop_dong, db_path, ghi_nhan)
    else:
        van_de = _gate2(ten_bang, du_lieu, hop_dong)

    if ghi_nhan:
        for v in van_de:
            if v["severity"] not in ("BREAKING", "CRITICAL", "WARNING"):
                continue
            # "Chưa có hợp đồng" đã ghi ở gate1 rồi, gate2 không ghi lại cho khỏi trùng
            if cong == "gate2" and v["error_type"] == "NO_CONTRACT":
                continue
            ket_qua = report_violation(v["error_type"], v["severity"], ten_bang, v["message"],
                                       column=v["column"], blocked=v["severity"] != "WARNING",
                                       db_path=db_path)
            v["violation_id"] = ket_qua["id"]
            v["affected_ai_tables"] = ket_qua["affected_ai_tables"]

        dat = not any(v["severity"] in ("BREAKING", "CRITICAL") for v in van_de)
        log_event("governance_gate", cong, ten_bang, "SUCCESS" if dat else "BLOCKED", db_path=db_path)
    return van_de


def format_issue(v: dict) -> str:
    """Đổi một vấn đề thành chuỗi tiếng Việt, ví dụ: '[BREAKING] SCHEMA_COLUMN_RENAMED (admissionweight): ...'"""
    cot = f" ({v['column']})" if v["column"] else ""
    return f"[{v['severity']}] {v['error_type']}{cot}: {v['message']}"


def check_gate(ten_bang: str, du_lieu: pd.DataFrame, cong: str,
               db_path: Optional[str] = None) -> tuple[bool, list[str]]:
    """
    Hàm dùng chung cho Người 1. cong ∈ {"gate1", "gate2"}.
    Trả về (đạt?, danh sách lỗi/cảnh báo dạng chuỗi tiếng Việt).
    Đạt = không có lỗi BREAKING nào (WARNING vẫn đạt nhưng được ghi vào violations).
    """
    van_de = check_gate_detailed(ten_bang, du_lieu, cong, db_path)
    dat = not any(v["severity"] in ("BREAKING", "CRITICAL") for v in van_de)
    return dat, [format_issue(v) for v in van_de if v["severity"] != "INFO"]


if __name__ == "__main__":
    import os
    import tempfile

    from catalog_access.init_db import init_database
    from governance.lineage import record_sample_lineage
    from governance.sample_data import make_patient

    db = os.path.join(tempfile.mkdtemp(), "thu_gates.db")
    init_database(db, seed_sample_data=False)
    record_sample_lineage(db)

    df = make_patient("B")
    print("Dữ liệu sạch, gate1:", check_gate("raw_patient_hospital_B", df, "gate1", db))
    print("Dữ liệu sạch, gate2:", check_gate("raw_patient_hospital_B", df, "gate2", db))
    print("\nĐổi tên admissionweight -> weight:")
    dat, loi = check_gate("raw_patient_hospital_B", df.rename(columns={"admissionweight": "weight"}),
                          "gate1", db)
    print("Đạt?", dat)
    for dong in loi:
        print(" -", dong)
