"""Kiểm thử giả danh hóa HMAC và luật quyền riêng tư của tầng AI-ready."""

import hashlib
import os
import stat

import pytest

from governance.contracts import load_contracts
from governance.gates import check_gate
from governance.lineage import record_sample_lineage
from governance.privacy import check_privacy, get_secret, pseudonymize_id, pseudonymize_series, sensitive_column_names
from governance.sample_data import build_ai_ready, make_hospital, standardize_patients
from governance.violation_engine import get_violations

AI = "ai_ready_icu_mortality"
KHOA = b"khoa-kiem-thu-0123456789abcdef"


@pytest.fixture
def ai_df():
    return build_ai_ready(standardize_patients({v: make_hospital(v) for v in "ABC"}))


def test_hmac_on_dinh_va_phu_thuoc_khoa():
    ma = pseudonymize_id(141234, KHOA)
    assert ma == pseudonymize_id(141234, KHOA)
    assert len(ma) == 64 and all(c in "0123456789abcdef" for c in ma)
    assert ma != pseudonymize_id(141234, b"khoa-khac-0123456789abcdef")
    assert ma != pseudonymize_id(141235, KHOA)


def test_khong_do_nguoc_duoc_bang_sha256_thuong():
    """Kẻ tấn công thử băm SHA-256 mọi mã bệnh nhân -> không khớp mã giả danh nào."""
    ma = pseudonymize_id(141234, KHOA)
    bang_tra = {hashlib.sha256(str(i).encode()).hexdigest() for i in range(141000, 142000)}
    assert ma not in bang_tra


def test_chuan_hoa_ma_truoc_khi_bam():
    assert pseudonymize_id(141234, KHOA) == pseudonymize_id(141234.0, KHOA) == pseudonymize_id(" 141234 ", KHOA)


def test_gia_danh_ca_cot():
    import pandas as pd
    cot = pseudonymize_series(pd.Series([1, 2, 1]), KHOA)
    assert cot[0] == cot[2] != cot[1]


def test_khoa_lay_tu_bien_moi_truong(monkeypatch):
    monkeypatch.setenv("PATIENT_HASH_SECRET", "x" * 20)
    assert get_secret() == b"x" * 20
    monkeypatch.setenv("PATIENT_HASH_SECRET", "ngan")
    with pytest.raises(ValueError):
        get_secret()


def test_tu_sinh_file_khoa_quyen_600(monkeypatch, tmp_path):
    duong_dan = tmp_path / "bi_mat" / "patient_hash.key"
    monkeypatch.delenv("PATIENT_HASH_SECRET")
    monkeypatch.setenv("PATIENT_HASH_KEY_FILE", str(duong_dan))
    khoa = get_secret()
    assert duong_dan.exists() and len(khoa) == 64
    assert stat.S_IMODE(os.stat(duong_dan).st_mode) == 0o600
    assert get_secret() == khoa                    # lần sau đọc lại đúng khóa cũ


def test_file_khoa_qua_ngan_bi_tu_choi(monkeypatch, tmp_path):
    duong_dan = tmp_path / "patient_hash.key"
    duong_dan.write_text("ngan")
    monkeypatch.delenv("PATIENT_HASH_SECRET")
    monkeypatch.setenv("PATIENT_HASH_KEY_FILE", str(duong_dan))
    with pytest.raises(ValueError):
        get_secret()


def test_cot_dinh_danh_lay_tu_hop_dong():
    assert {"patientunitstayid", "uniquepid", "patient_id"} <= sensitive_column_names()
    assert "age" not in sensitive_column_names()


def test_ai_ready_sach_qua_ca_hai_cong(db_path, ai_df):
    record_sample_lineage(db_path)
    assert check_gate(AI, ai_df, "gate1", db_path) == (True, [])
    assert check_gate(AI, ai_df, "gate2", db_path) == (True, [])


def test_chan_cot_dinh_danh_o_ai_ready(db_path, ai_df):
    record_sample_lineage(db_path)
    ro_ri = ai_df.assign(patient_id=range(len(ai_df)))
    dat, loi = check_gate(AI, ro_ri, "gate1", db_path)
    assert dat is False
    v = get_violations(db_path, severity="BREAKING")
    assert [x["error_type"] for x in v] == ["PRIVACY_SENSITIVE_COLUMN"]
    assert v[0]["affected_ai_table"] == AI and "patient_id" in v[0]["details"]


def test_chan_cot_ngoai_hop_dong_o_ai_ready(db_path, ai_df):
    record_sample_lineage(db_path)
    dat, _ = check_gate(AI, ai_df.assign(ethnicity="Asian"), "gate1", db_path)
    assert dat is False
    assert [x["error_type"] for x in get_violations(db_path, severity="BREAKING")] == ["PRIVACY_UNDECLARED_COLUMN"]


def test_tang_khac_khong_ap_luat_rieng_tu(db_path):
    std = standardize_patients({"A": make_hospital("A")}).assign(cot_them=1)
    hop_dong = next(h for h in load_contracts() if h["name"] == "standardized_patients")
    assert check_privacy("standardized_patients", std, hop_dong) == []
    assert check_gate("standardized_patients", std, "gate1", db_path)[0] is True


def test_ma_gia_danh_sai_dinh_dang_bi_chan(db_path, ai_df):
    """patient_hash kiểu cũ (SHA-256 cắt 16 ký tự) không khớp pattern 64 ký tự hex của hợp đồng."""
    cu = ai_df.assign(patient_hash=ai_df["patient_hash"].str[:16])
    dat, loi = check_gate(AI, cu, "gate2", db_path)
    assert dat is False and "CONFORMANCE" in loi[0] and "patient_hash" in loi[0]


def test_pattern_sai_trong_hop_dong_bi_bao_loi(tmp_path):
    (tmp_path / "hd.yaml").write_text(
        "name: x\napplies_to: [x]\nowner: o\nversion: '1'\ncolumns:\n  a:\n    type: string\n    pattern: '[a-'\n",
        encoding="utf-8")
    with pytest.raises(ValueError, match="pattern"):
        load_contracts(str(tmp_path))


def test_tang_sai_trong_hop_dong_bi_bao_loi(tmp_path):
    (tmp_path / "hd.yaml").write_text(
        "name: x\napplies_to: [x]\nowner: o\nversion: 1\ntier: gold\ncolumns:\n  a:\n    type: string\n",
        encoding="utf-8")
    with pytest.raises(ValueError, match="tầng"):
        load_contracts(str(tmp_path))
