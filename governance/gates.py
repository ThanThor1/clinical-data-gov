"""
Cổng kiểm tra dữ liệu (quality gates). Dữ liệu chỉ được lên tầng trên khi qua cổng.

    gate1 - Schema + hợp đồng:
        * ghi schema vào schema_registry, phát hiện đổi tên / xóa / đổi kiểu cột
        * cột bắt buộc phải có mặt, kiểu dữ liệu phải đúng hợp đồng
        * bảng tầng ai_ready: luật quyền riêng tư (governance/privacy.py) - không cột định danh,
          không cột ngoài hợp đồng
    gate2 - Chất lượng theo khung Kahn:
        * Conformance  (tuân thủ) : đúng kiểu số, giá trị thuộc allowed_values, khớp pattern
        * Completeness (đầy đủ)   : không vượt max_null_pct, cột nullable: false không có ô trống
        * Plausibility (hợp lý)   : giá trị trong [min, max]

Mức độ:
    BREAKING -> chặn dữ liệu (check_gate trả False)
    WARNING  -> cho qua nhưng ghi lại vào violations
    INFO     -> chỉ để biết (ví dụ thêm cột mới), không ghi violations

Ngưỡng 1% (tài liệu phân công mục 7): lỗi giá trị (sai định dạng / ngoài khoảng) chỉ BREAKING khi
> 1% số dòng bị lỗi; ít hơn thì WARNING.
Số dòng lỗi tối thiểu (min_bad_rows trong hợp đồng, mặc định 5): lỗi theo tỷ lệ (kể cả % ô trống)
chỉ BREAKING khi có ít nhất chừng ấy dòng lỗi -> lô vài bệnh nhân không bị chặn oan vì 1-2 ô trống.

Cổng đọc HỒ SƠ dữ liệu (governance/profiling.py) chứ không đọc thẳng dữ liệu:
    check_gate(ten_bang, df_pandas, cong)        -> tự tính hồ sơ bằng pandas
    check_gate(ten_bang, ho_so, cong)            -> hồ sơ đã tính sẵn bằng Spark (pipeline thật)

Hàm dùng chung (tài liệu phân công mục 4): check_gate(ten_bang, du_lieu, cong) -> (dat, danh_sach_loi)

Chạy thử: python -m governance.gates
"""

from typing import Optional, Union

import pandas as pd

from catalog_access import log_event
from governance.contracts import MIN_DONG_LOI_MAC_DINH, contract_label, find_contract
from governance.privacy import check_privacy
from governance.profiling import profile_pandas
from governance.schema_registry import register_schema, set_schema_breaking
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

def _hop_kieu(thong_tin_cot: dict, kieu_hop_dong: str) -> tuple[bool, str]:
    """
    Kiểm tra kiểu thực tế của cột (trong hồ sơ) có hợp với kiểu ghi trong hợp đồng không.
    Trả về (hợp?, kiểu thực tế).
      - string / category : chấp nhận mọi kiểu (ví dụ age raw lúc là số, lúc là "> 89")
      - float             : chấp nhận int hoặc float
      - int               : chấp nhận int, hoặc float mà mọi giá trị đều là số nguyên (40.0)
      - cột không có giá trị nào ('unknown') : bỏ qua
    """
    kieu = thong_tin_cot["type"]
    if kieu == "unknown" or kieu_hop_dong in ("string", "category"):
        return True, kieu
    if kieu_hop_dong == "float":
        return kieu in ("int", "float"), kieu
    if kieu_hop_dong == "int":
        if kieu == "int":
            return True, kieu
        if kieu == "float":
            return bool(thong_tin_cot.get("integral")), kieu
    return False, kieu


def _theo_nguong(so_dong_loi: int, tong_dong: int, rule: str, error_type: str, column: str,
                 mo_ta: str, expected: str, vi_du: list, toi_thieu: int) -> Optional[dict]:
    """
    Áp dụng luật 1%: > 1% số dòng lỗi VÀ ít nhất `toi_thieu` dòng lỗi -> BREAKING,
    có lỗi nhưng chưa tới mức đó -> WARNING.
    """
    if so_dong_loi == 0 or tong_dong == 0:
        return None
    ty_le = so_dong_loi / tong_dong * 100
    muc_do = "BREAKING" if ty_le > NGUONG_TY_LE_DONG and so_dong_loi >= toi_thieu else "WARNING"
    vi_du_str = ", ".join(str(v) for v in vi_du[:3])
    ghi_chu = " (lô nhỏ, chưa đủ số dòng lỗi để chặn)" if ty_le > NGUONG_TY_LE_DONG and muc_do == "WARNING" else ""
    return _van_de(
        rule, error_type, muc_do, column,
        f"{so_dong_loi}/{tong_dong} dòng ({ty_le:.2f}%) {mo_ta}; ví dụ: {vi_du_str}{ghi_chu}",
        observed=f"{ty_le:.2f}% dòng lỗi",
        expected=f"{expected}; chặn khi > {NGUONG_TY_LE_DONG:g}% và >= {toi_thieu} dòng lỗi",
    )


def _lay_ho_so(ten_bang: str, du_lieu: Union[pd.DataFrame, dict], hop_dong: Optional[dict]) -> dict:
    """Hồ sơ của dữ liệu: tự tính nếu là DataFrame pandas, kiểm tra khớp hợp đồng nếu là hồ sơ có sẵn."""
    if isinstance(du_lieu, pd.DataFrame):
        return profile_pandas(du_lieu, hop_dong)
    if du_lieu.get("hop_dong", "") != contract_label(hop_dong):
        raise ValueError(f"Hồ sơ của '{ten_bang}' tính theo hợp đồng '{du_lieu.get('hop_dong')}', "
                         f"nhưng bảng áp dụng hợp đồng '{contract_label(hop_dong)}'")
    return du_lieu


# ---------------------------------------------------------------------------
# Gate 1: schema + hợp đồng
# ---------------------------------------------------------------------------

def _gate1(ten_bang: str, ho_so: dict, hop_dong: Optional[dict],
           db_path: Optional[str], ghi_nhan: bool) -> list[dict]:
    van_de = []
    luat_cot = hop_dong["columns"] if hop_dong else {}
    cot_du_lieu = ho_so["cot"]
    if hop_dong is None:
        van_de.append(_van_de("contract.exists", "NO_CONTRACT", "WARNING", None,
                              f"Bảng '{ten_bang}' chưa có hợp đồng dữ liệu, chỉ ghi nhận schema",
                              observed="không có hợp đồng", expected="có file contracts/*.yaml"))

    # 1. So sánh schema với phiên bản chuẩn trong schema_registry
    dang_ky = register_schema(ten_bang, ho_so, db_path, save=ghi_nhan)
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
            elif _hop_kieu(cot_du_lieu[cot], luat["type"])[0]:
                van_de.append(_van_de("schema.type_changed", loai_loi, "INFO", cot,
                                      thay_doi["message"] + " (vẫn đúng kiểu trong hợp đồng)",
                                      observed=thay_doi["new_type"], expected=luat["type"]))
            # Trường hợp sai kiểu so với hợp đồng: phần kiểm tra hợp đồng bên dưới sẽ báo

    # 2. Đối chiếu hợp đồng: cột bắt buộc có mặt, đúng kiểu
    for cot, luat in luat_cot.items():
        if cot not in cot_du_lieu:
            if luat["required"] and cot not in da_bao:
                van_de.append(_van_de("contract.required_column", "MISSING_REQUIRED_COLUMN",
                                      "BREAKING", cot, f"Thiếu cột bắt buộc '{cot}'",
                                      observed="không có cột", expected=f"cột '{cot}' ({luat['type']})"))
            continue
        hop, kieu_thuc = _hop_kieu(cot_du_lieu[cot], luat["type"])
        if not hop:
            van_de.append(_van_de(
                "contract.type", "SCHEMA_TYPE_CHANGED",
                "BREAKING" if luat["required"] else "WARNING", cot,
                f"Cột '{cot}' có kiểu {kieu_thuc}, hợp đồng yêu cầu {luat['type']}",
                observed=kieu_thuc, expected=luat["type"]))

    # 3. Tầng AI-ready: luật quyền riêng tư (cột định danh / cột ngoài hợp đồng -> chặn)
    for loi in check_privacy(ten_bang, list(cot_du_lieu), hop_dong):
        van_de.append(_van_de(f"privacy.{loi['error_type'].lower()}", loi["error_type"], "BREAKING",
                              loi["column"], loi["message"], observed=loi["observed"],
                              expected=loi["expected"]))

    # 4. Đánh dấu phiên bản schema vừa ghi là gây hỏng hay không (theo kết luận của gate)
    if ghi_nhan and dang_ky["is_new_version"]:
        co_chan = any(v["severity"] == "BREAKING" for v in van_de)
        set_schema_breaking(ten_bang, dang_ky["version"], co_chan, db_path)
    return van_de


# ---------------------------------------------------------------------------
# Gate 2: chất lượng theo khung Kahn
# ---------------------------------------------------------------------------

def _gate2(ten_bang: str, ho_so: dict, hop_dong: Optional[dict]) -> list[dict]:
    if hop_dong is None:
        return [_van_de("contract.exists", "NO_CONTRACT", "WARNING", None,
                        f"Bảng '{ten_bang}' chưa có hợp đồng, bỏ qua kiểm tra chất lượng",
                        observed="không có hợp đồng", expected="có file contracts/*.yaml")]

    van_de = []
    tong = ho_so["so_dong"]
    if tong == 0:
        return van_de    # bảng rỗng (ví dụ vital của viện không đo sinh hiệu): không có gì để kiểm tra
    toi_thieu = hop_dong.get("min_bad_rows", MIN_DONG_LOI_MAC_DINH)

    for cot, luat in hop_dong["columns"].items():
        if cot not in ho_so["cot"]:
            continue     # thiếu cột là việc của gate1
        thong_tin = ho_so["cot"][cot]
        vi_pham = thong_tin.get("vi_pham", {})

        # --- Completeness: ô trống ---
        so_rong = thong_tin["so_rong"]
        ty_le_rong = so_rong / tong * 100
        if not luat["nullable"] and so_rong > 0:
            van_de.append(_van_de("completeness.nullable", "COMPLETENESS", "BREAKING", cot,
                                  f"Cột '{cot}' không được trống nhưng có {so_rong} ô trống",
                                  observed=f"{so_rong} ô trống", expected="0 ô trống"))
        elif luat["max_null_pct"] is not None and ty_le_rong > luat["max_null_pct"]:
            du_dong = so_rong >= toi_thieu
            van_de.append(_van_de(
                "completeness.max_null_pct", "COMPLETENESS", "BREAKING" if du_dong else "WARNING", cot,
                f"Cột '{cot}' trống {ty_le_rong:.1f}%, vượt ngưỡng {luat['max_null_pct']}%"
                + ("" if du_dong else f" (chỉ {so_rong} ô trống trong lô {tong} dòng, chưa đủ {toi_thieu} để chặn)"),
                observed=f"{ty_le_rong:.1f}%", expected=f"<= {luat['max_null_pct']}%"))

        # --- Conformance + Plausibility (số đếm đã có sẵn trong hồ sơ) ---
        cac_luat = [
            ("conformance.type", "CONFORMANCE", f"không phải số {luat['type']}", f"kiểu {luat['type']}"),
            ("plausibility.range", "PLAUSIBILITY", f"nằm ngoài khoảng [{luat['min']}, {luat['max']}]",
             f"trong [{luat['min']}, {luat['max']}]"),
            ("conformance.pattern", "CONFORMANCE", f"không khớp mẫu {luat['pattern']}", f"khớp {luat['pattern']}"),
            ("conformance.allowed_values", "CONFORMANCE",
             f"có giá trị ngoài tập {sorted(str(v) for v in luat['allowed_values'] or [])}",
             f"thuộc {sorted(str(v) for v in luat['allowed_values'] or [])}"),
        ]
        for rule, error_type, mo_ta, expected in cac_luat:
            if rule not in vi_pham:
                continue
            so_dong_loi, vi_du = vi_pham[rule]
            loi = _theo_nguong(so_dong_loi, tong, rule, error_type, cot, mo_ta, expected, vi_du, toi_thieu)
            if loi:
                van_de.append(loi)
    return van_de


# ---------------------------------------------------------------------------
# Hàm công khai
# ---------------------------------------------------------------------------

def check_gate_detailed(ten_bang: str, du_lieu: Union[pd.DataFrame, dict], cong: str,
                        db_path: Optional[str] = None, ghi_nhan: bool = True) -> list[dict]:
    """
    Chạy cổng kiểm tra và trả về danh sách vấn đề chi tiết, mỗi vấn đề là dict:
        rule, error_type, severity, column, message, observed, expected
    (nếu đã ghi vi phạm: thêm violation_id, affected_ai_tables).

    du_lieu : DataFrame pandas, hoặc hồ sơ đã tính sẵn (profile_spark / profile_pandas) theo
              đúng hợp đồng của bảng.
    ghi_nhan=True : lưu schema, ghi violations (BREAKING/WARNING) và audit_log.
    ghi_nhan=False: chạy thử, không ghi gì vào DB.

    Lưu ý: check_gate gọi hàm này bên trong -> mỗi lô dữ liệu chỉ gọi MỘT trong hai hàm.
    """
    if cong not in CONG_HOP_LE:
        raise ValueError(f"Cổng '{cong}' không hợp lệ, chỉ nhận {CONG_HOP_LE}")

    hop_dong = find_contract(ten_bang)
    ho_so = _lay_ho_so(ten_bang, du_lieu, hop_dong)
    if cong == "gate1":
        van_de = _gate1(ten_bang, ho_so, hop_dong, db_path, ghi_nhan)
    else:
        van_de = _gate2(ten_bang, ho_so, hop_dong)

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


def check_gate(ten_bang: str, du_lieu: Union[pd.DataFrame, dict], cong: str,
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
