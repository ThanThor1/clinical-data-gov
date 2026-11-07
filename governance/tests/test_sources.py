"""Kiểm thử sổ đăng ký nguồn (governance/sources.py) và bộ chuyển nguồn eav_long (pipeline/source_adapters.py)."""

import copy

import pandas as pd
import pytest

from governance import sources
from governance.lineage import get_impact_paths, record_sample_lineage
from governance.sample_data import make_eav_source
from governance.violation_engine import get_violations
from pipeline import source_adapters

AI = "ai_ready_icu_mortality"


@pytest.fixture
def c2012():
    return sources.source_of_hospital("c2012")


@pytest.fixture
def staging(tmp_path, c2012):
    """Nguồn giả cùng định dạng Challenge 2012 đã qua bộ chuyển. Trả về (thư mục staging, kỳ vọng)."""
    landing = tmp_path / "landing"
    landing.mkdir()
    ky_vong = make_eav_source(landing)
    so_dong = source_adapters.chuyen_eav_long(c2012, str(landing), str(tmp_path / "staging"))
    assert so_dong == {"patient": 12, "vital": 36, "lab": 36}
    return tmp_path / "staging" / "hospital_c2012", ky_vong


def test_so_dang_ky_nguon():
    ten = {n["name"]: n for n in sources.load_sources()}
    assert set(ten) >= {"eicu_demo", "challenge2012"}
    assert ten["eicu_demo"]["hospitals"] == "default" and ten["challenge2012"]["format"] == "eav_long"
    assert sources.source_of_hospital("146")["name"] == "eicu_demo"
    assert sources.is_default_source("A") and not sources.is_default_source("c2012")


def test_bo_chuyen_dua_ve_schema_chuan(staging):
    thu_muc, ky_vong = staging
    p = pd.read_csv(thu_muc / "patient.csv", dtype=str, keep_default_na=False).set_index("patientunitstayid")
    assert list(p.columns) == ["age", "gender", "admissionweight", "unitdischargestatus"]
    dau = p.loc["200000"]
    # -1 -> trống; cân nặng lúc vào thiếu -> lấy giá trị hợp lệ đầu tiên (giờ thứ 2); tuổi ghi số nguyên
    assert (dau["gender"], dau["admissionweight"], dau["age"]) == ("", "77.5", "40")
    for ma, kv in ky_vong.items():
        dong = p.loc[str(ma)]
        assert (dong["unitdischargestatus"], dong["gender"] or None) == (kv["status"], kv["gender"])
        assert float(dong["admissionweight"]) == kv["weight"]


def test_sinh_hieu_va_xet_nghiem(staging):
    thu_muc, ky_vong = staging
    v = pd.read_csv(thu_muc / "vitalPeriodic.csv")
    assert list(v.columns) == ["patientunitstayid", "observationoffset", "heartrate"]
    dau = v[v["patientunitstayid"] == 200000]
    assert list(dau["observationoffset"]) == [10, 20, 30] and list(dau["heartrate"]) == ky_vong[200000]["hr"]
    lab = pd.read_csv(thu_muc / "lab.csv")
    assert list(lab.columns) == ["patientunitstayid", "labresultoffset", "labname", "labresult"]
    # Cholesterol không có trong ánh xạ -> bỏ; giờ 01:05 -> 65 phút
    assert set(lab["labname"]) == {"creatinine", "glucose", "magnesium"} and set(lab["labresultoffset"]) == {65}


def test_he_so_doi_don_vi(tmp_path, c2012):
    nguon = copy.deepcopy(c2012)
    nguon["lab"]["Mg"] = {"to": "magnesium", "scale": 0.5, "note": "thử"}
    make_eav_source(tmp_path, so_luot=2)
    source_adapters.chuyen_eav_long(nguon, str(tmp_path), str(tmp_path / "stg"))
    lab = pd.read_csv(tmp_path / "stg" / "hospital_c2012" / "lab.csv")
    assert set(lab.loc[lab["labname"] == "magnesium", "labresult"]) == {1.05}


def test_bo_qua_nguon_chua_tai_du_lieu(tmp_path, capsys):
    thu_muc = tmp_path / "sources"
    thu_muc.mkdir()
    (thu_muc / "a.yaml").write_text("name: a\nsystem: A\nformat: csv_wide\nhospitals: default\n", encoding="utf-8")
    (thu_muc / "b.yaml").write_text("name: b\nsystem: B\nformat: eav_long\nhospitals: [b1]\nlanding: khong_co\n"
                                    "files: {records: x.tar.gz}\n", encoding="utf-8")
    assert source_adapters.main(str(tmp_path / "stg"), str(thu_muc)) == []
    assert "Bỏ qua nguồn b" in capsys.readouterr().out


def test_lineage_tu_tham_so_goc_toi_ai_ready(db_path):
    record_sample_lineage(db_path, ["c2012"])
    assert sources.record_source_lineage(["c2012", "146"], db_path) > 20       # viện eICU không có cạnh nguồn
    duong_di = get_impact_paths("nguon_challenge2012", "Weight", db_path)
    assert duong_di == ["nguon_challenge2012.Weight -> raw_patient_hospital_c2012.admissionweight -> "
                        "standardized_patients.weight_kg -> ai_ready_icu_mortality.weight_kg"]
    assert get_impact_paths("nguon_challenge2012_outcomes", "In-hospital_death", db_path)[0].endswith(
        f"{AI}.icu_mortality")


def test_nguon_khac_dinh_nghia_nhan_thi_canh_bao(db_path):
    record_sample_lineage(db_path, ["c2012", "146"])
    assert sources.check_source_semantics(["146"], db_path) == []
    sources.check_source_semantics(["c2012"], db_path)
    v = get_violations(db_path)[0]
    assert (v["error_type"], v["severity"], v["affected_table"]) == ("SEMANTIC_MISMATCH", "WARNING",
                                                                     "raw_patient_hospital_c2012")
    assert "in_hospital_death" in v["details"] and v["affected_ai_table"] == AI and v["action"] == "cảnh báo"
