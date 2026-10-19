"""
Kiểm thử trên DỮ LIỆU THẬT eICU trong data/02_staging/hospital_A|B|C.
Tự bỏ qua (skip) nếu máy chưa có dữ liệu (data/ không được commit).
"""

import os

import pandas as pd
import pytest

from governance.gates import check_gate
from governance.lineage import record_sample_lineage
from governance.sample_data import build_ai_ready, standardize_patients
from governance.violation_engine import get_violations

STAGING = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "02_staging"))
CO_DU_LIEU = all(os.path.exists(os.path.join(STAGING, f"hospital_{v}", "patient.csv")) for v in "ABC")

pytestmark = pytest.mark.skipif(not CO_DU_LIEU, reason="Chưa có dữ liệu thật trong data/02_staging")


def _doc(vien):
    thu_muc = os.path.join(STAGING, f"hospital_{vien}")
    return {
        "patient": pd.read_csv(os.path.join(thu_muc, "patient.csv")),
        "lab": pd.read_csv(os.path.join(thu_muc, "lab.csv")),
        "vital": pd.read_csv(os.path.join(thu_muc, "vitalPeriodic.csv")),
    }


@pytest.fixture(scope="module")
def raw():
    return {v: _doc(v) for v in "ABC"}


def test_du_lieu_that_sach_deu_dat(db_path, raw):
    record_sample_lineage(db_path)
    for vien, bang in raw.items():
        for loai in ("patient", "lab", "vital"):
            ten = f"raw_{loai}_hospital_{vien}"
            for cong in ("gate1", "gate2"):
                dat, loi = check_gate(ten, bang[loai], cong, db_path)
                assert dat, (ten, cong, loi)

    std = standardize_patients(raw)
    assert check_gate("standardized_patients", std, "gate1", db_path)[0]
    assert check_gate("standardized_patients", std, "gate2", db_path)[0]
    ai = build_ai_ready(std)
    assert check_gate("ai_ready_icu_mortality", ai, "gate1", db_path)[0]
    assert check_gate("ai_ready_icu_mortality", ai, "gate2", db_path)[0]

    # Không có lỗi BREAKING; cảnh báo duy nhất mong đợi: 1 dòng nhịp tim < 20 ở viện C
    assert get_violations(db_path, severity="BREAKING") == []
    canh_bao = get_violations(db_path, severity="WARNING")
    assert [(v["error_type"], v["affected_table"]) for v in canh_bao] == [
        ("PLAUSIBILITY", "raw_vital_hospital_C")]


def test_du_lieu_that_vien_B_doi_ten_bi_chan(db_path, raw):
    record_sample_lineage(db_path)
    p = raw["B"]["patient"]
    assert check_gate("raw_patient_hospital_B", p, "gate1", db_path)[0]

    dat, loi = check_gate("raw_patient_hospital_B", p.rename(columns={"admissionweight": "weight"}),
                          "gate1", db_path)
    assert dat is False
    v = get_violations(db_path)[0]
    assert (v["error_type"], v["severity"], v["hospital"], v["affected_ai_table"]) == (
        "SCHEMA_COLUMN_RENAMED", "BREAKING", "Bệnh viện B", "ai_ready_icu_mortality")
