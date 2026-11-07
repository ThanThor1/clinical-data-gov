"""
Kiểm thử hồ sơ dữ liệu: cổng kiểm tra phải ra CÙNG kết luận dù hồ sơ tính bằng pandas hay bằng Spark.
Test cần Spark tự bỏ qua nếu máy chưa có Spark / Java.
"""

import numpy as np
import pandas as pd
import pytest

from governance.contracts import find_contract
from governance.gates import check_gate, check_gate_detailed
from governance.profiling import empty_profile, load_cached_profiles, profile_pandas, save_profiles
from governance.sample_data import make_patient, make_vital
from governance.schema_registry import type_family

BN = "raw_patient_hospital_B"


def _bien_the():
    """Dữ liệu sạch + các kiểu lỗi của inject_errors (và vài lỗi giá trị khác)."""
    p = make_patient("B")
    v = make_vital("A")
    thieu = p.copy()
    thieu.loc[thieu.index[:15], "admissionweight"] = np.nan
    gioi_sai = p.copy()
    gioi_sai.loc[gioi_sai.index[:8], "gender"] = "X"
    nhe = p.copy()
    nhe.loc[nhe.index[:6], "admissionweight"] = 0.5
    nhip_tim = v.copy()
    nhip_tim.loc[nhip_tim.index[:40], "heartrate"] = 500
    return {
        "sach": (BN, p),
        "doi_ten": (BN, p.rename(columns={"admissionweight": "weight"})),
        "doi_kieu": (BN, p.assign(admissionweight=p["admissionweight"].map(lambda x: f"{x} kg" if x == x else ""))),
        "xoa_cot": (BN, p.drop(columns=["gender"])),
        "thieu_can_nang": (BN, thieu),
        "gioi_tinh_sai": (BN, gioi_sai),
        "can_nang_vo_ly": (BN, nhe),
        "nhip_tim_500": ("raw_vital_hospital_A", nhip_tim),
    }


def _ket_luan(van_de):
    return sorted((v["rule"], v["severity"], v["column"] or "") for v in van_de)


def test_ho_so_pandas_co_du_thong_tin():
    p = make_patient("B")
    p.loc[0, "admissionweight"] = 0.5
    ho_so = profile_pandas(p, find_contract(BN))
    assert ho_so["so_dong"] == 30 and ho_so["hop_dong"] == "raw_patient v1.1"
    can_nang = ho_so["cot"]["admissionweight"]
    assert can_nang["type"] == "float" and can_nang["so_rong"] == 5        # 6 ô trống, 1 ô bị gán 0.5
    assert can_nang["vi_pham"]["plausibility.range"] == [1, [0.5]]
    assert "vi_pham" not in ho_so["cot"]["ethnicity"]                 # cột ngoài hợp đồng


def test_ho_so_rong_qua_cong(db_path):
    ho_so = empty_profile(["vitalperiodicid", "patientunitstayid", "observationoffset", "heartrate"],
                          find_contract("raw_vital_hospital_B"))
    assert ho_so["so_dong"] == 0 and ho_so["cot"]["heartrate"]["type"] == "unknown"
    assert check_gate("raw_vital_hospital_B", ho_so, "gate1", db_path) == (True, [])
    assert check_gate("raw_vital_hospital_B", ho_so, "gate2", db_path) == (True, [])


def test_ho_so_sai_hop_dong_bi_tu_choi(db_path):
    ho_so = profile_pandas(make_patient("B"), find_contract("standardized_patients"))
    with pytest.raises(ValueError, match="hợp đồng"):
        check_gate_detailed(BN, ho_so, "gate2", db_path, ghi_nhan=False)


def test_luu_va_dung_lai_ho_so_theo_checksum(db_path):
    hop_dong = find_contract(BN)
    ho_so = empty_profile(["admissionweight"], hop_dong)
    ho_so["cot"]["admissionweight"]["vi_pham"] = {"plausibility.range": [1, ["0.5"]]}
    save_profiles({"abc": ho_so}, hop_dong, db_path)
    assert load_cached_profiles(["abc", "khong_co"], hop_dong, db_path) == {"abc": ho_so}
    # Khác hợp đồng (ví dụ hợp đồng đổi phiên bản) -> không dùng lại
    assert load_cached_profiles(["abc"], find_contract("standardized_patients"), db_path) == {}


@pytest.mark.parametrize("ten", list(_bien_the()))
def test_pandas_va_spark_cho_cung_ket_luan(spark, tmp_path, db_path, ten):
    from pipeline.standardize import FileGui, doc_csv_spark
    from governance.profiling import profile_spark

    bang, df = _bien_the()[ten]
    duong_dan = tmp_path / "hospital_B" / "dot" / "file.csv"
    duong_dan.parent.mkdir(parents=True)
    df.to_csv(duong_dan, index=False)
    doc_lai = pd.read_csv(duong_dan)                                   # pandas đọc đúng file Spark đọc
    hop_dong = find_contract(bang)

    ho_so_pandas = profile_pandas(doc_lai, hop_dong)
    f = FileGui("B", "dot", "patient", str(duong_dan), tuple(doc_lai.columns), "")
    ho_so_spark = next(iter(profile_spark(doc_csv_spark(spark, [f]), hop_dong, nhom="_file",
                                          suy_kieu_tu_chuoi=True).values()))

    assert ho_so_spark["so_dong"] == ho_so_pandas["so_dong"]
    for cot, tt in ho_so_pandas["cot"].items():
        sp = ho_so_spark["cot"][cot]
        assert type_family(sp["type"]) == type_family(tt["type"]), cot
        assert sp["so_rong"] == tt["so_rong"], cot
        assert {r: n for r, (n, _) in sp.get("vi_pham", {}).items()} == {
            r: n for r, (n, _) in tt.get("vi_pham", {}).items()}, cot
    for cong in ("gate1", "gate2"):
        assert _ket_luan(check_gate_detailed(bang, ho_so_spark, cong, db_path, ghi_nhan=False)) == \
            _ket_luan(check_gate_detailed(bang, ho_so_pandas, cong, db_path, ghi_nhan=False)), (ten, cong)


def test_lo_nho_chi_canh_bao_lo_lon_moi_chan(db_path):
    """min_bad_rows = 5: lô 4 bệnh nhân thiếu 2 cân nặng (50%) chỉ cảnh báo, lô 30 thiếu 15 thì chặn."""
    nho = make_patient("B").head(4).copy()
    nho["admissionweight"] = [70.0, np.nan, np.nan, 80.0]
    van_de = check_gate_detailed(BN, nho, "gate2", db_path, ghi_nhan=False)
    assert [(v["error_type"], v["severity"]) for v in van_de] == [("COMPLETENESS", "WARNING")]
    assert "chưa đủ 5 để chặn" in van_de[0]["message"]

    lon = make_patient("B")
    lon.loc[lon.index[:15], "admissionweight"] = np.nan
    assert [(v["error_type"], v["severity"]) for v in check_gate_detailed(BN, lon, "gate2", db_path,
                                                                           ghi_nhan=False)] == [
        ("COMPLETENESS", "BREAKING")]


def test_mot_dong_vo_ly_trong_lo_nho_chi_canh_bao(db_path):
    nho = make_patient("B").head(4).copy()
    nho["admissionweight"] = [70.0, 0.5, 65.0, 80.0]                    # 1/4 dòng = 25% nhưng chỉ 1 dòng
    van_de = check_gate_detailed(BN, nho, "gate2", db_path, ghi_nhan=False)
    assert [(v["error_type"], v["severity"]) for v in van_de] == [("PLAUSIBILITY", "WARNING")]
    assert "lô nhỏ" in van_de[0]["message"]
