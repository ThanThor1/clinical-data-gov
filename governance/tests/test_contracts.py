"""Kiểm thử việc đọc và tìm hợp đồng dữ liệu."""

from governance.contracts import (
    CAC_KIEU_HOP_LE,
    find_contract,
    get_required_columns,
    get_sensitive_columns,
    load_contracts,
)


def test_doc_du_cac_hop_dong():
    ten = {hd["name"] for hd in load_contracts()}
    assert {
        "raw_patient", "raw_lab", "raw_vital",
        "standardized_patients", "ai_ready_icu_mortality",
    } <= ten


def test_moi_cot_co_kieu_hop_le():
    for hd in load_contracts():
        for ten_cot, luat in hd["columns"].items():
            assert luat["type"] in CAC_KIEU_HOP_LE, (hd["name"], ten_cot)


def test_mau_sao_khop_moi_benh_vien():
    for vien in "ABC":
        assert find_contract(f"raw_patient_hospital_{vien}")["name"] == "raw_patient"
        assert find_contract(f"raw_lab_hospital_{vien}")["name"] == "raw_lab"
        assert find_contract(f"raw_vital_hospital_{vien}")["name"] == "raw_vital"


def test_khop_chinh_xac_va_khong_co_hop_dong():
    assert find_contract("standardized_patients")["name"] == "standardized_patients"
    assert find_contract("ai_ready_icu_mortality")["name"] == "ai_ready_icu_mortality"
    assert find_contract("bang_khong_ton_tai") is None


def test_luat_phan_anh_du_lieu_that():
    raw = find_contract("raw_patient_hospital_B")["columns"]
    assert raw["age"]["type"] == "string"
    assert raw["admissionweight"]["max_null_pct"] == 30
    assert (raw["admissionweight"]["min"], raw["admissionweight"]["max"]) == (20, 400)
    assert {"Male", "Female"} <= set(raw["gender"]["allowed_values"])

    vital = find_contract("raw_vital_hospital_A")["columns"]
    assert (vital["heartrate"]["min"], vital["heartrate"]["max"]) == (20, 300)

    std = find_contract("standardized_patients")["columns"]
    assert std["age"]["type"] == "int"
    assert set(std["gender"]["allowed_values"]) == {"M", "F", "U"}


def test_cot_bat_buoc_va_nhay_cam():
    hd = find_contract("raw_patient_hospital_A")
    assert "admissionweight" in get_required_columns(hd)
    assert "patientunitstayid" in get_sensitive_columns(hd)
    # Tầng AI-ready không còn cột nhạy cảm
    assert get_sensitive_columns(find_contract("ai_ready_icu_mortality")) == []
