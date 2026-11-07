"""Kiểm thử phát hiện vi phạm do pipeline đổi code (governance/pipeline_drift.py), không cần Spark."""

import pytest

from catalog_access import get_audit_logs
from governance import pipeline_drift as pd_
from governance import provenance as pv
from governance.violation_engine import get_violations

BANG = "standardized_patients"


def _ho_so(so_dong=100, can_nang=80.0, trong=5.0):
    return {"so_dong": so_dong, "cot": {"patient_id": {"type": "int", "null_pct": 0.0, "mean": 5.0},
                                        "weight_kg": {"type": "float", "null_pct": trong, "mean": can_nang}}}


@pytest.fixture
def lo(tmp_path, db_path):
    """Hai lô raw (nội dung khác nhau) để làm đầu vào."""
    cac_lo = []
    for i in (1, 2):
        tep = tmp_path / f"raw_{i}.csv"
        tep.write_text(f"a\n{i}\n", encoding="utf-8")
        cac_lo.append(pv.record_batch("raw_patient_hospital_A", "Bệnh viện A", f"hospital_A/dot_{i}", str(tep), 1,
                                      "ACCEPTED", db_path=db_path))
    return cac_lo


@pytest.fixture
def phien_ban_truoc(tmp_path, db_path, lo):
    """Một phiên bản standardized do code hiện tại tạo từ lô 1, cân nặng trung bình 80."""
    tep = tmp_path / "std.csv"
    tep.write_text("a\n1\n", encoding="utf-8")
    pv.record_dataset_version(BANG, "standardized", str(tep), 100, [("batch", lo[0])], db_path=db_path,
                              ho_so=_ho_so())
    return [("batch", lo[0])]


@pytest.fixture
def code_moi(monkeypatch):
    """Giả lập phiên bản code mới (lỗi pipeline giả lập được tính vào phiên bản code)."""
    monkeypatch.setenv("PIPELINE_LOI_GIA_LAP", "can_nang_pound")
    return pv.get_code_version()


def test_so_sanh_dau_ra():
    lech = pd_.compare_outputs(_ho_so(), _ho_so(so_dong=90, can_nang=176.0, trong=30.0), cung_dau_vao=True)
    mo_ta = " | ".join(m for _, m in lech)
    assert "số dòng 100 -> 90" in mo_ta and "trống 5.0% -> 30.0%" in mo_ta and "80 -> 176 (+120%)" in mo_ta
    assert pd_.compare_outputs(_ho_so(), _ho_so(can_nang=84.0), cung_dau_vao=True) == []       # lệch 5% < 20%
    # Khác đầu vào: số dòng đổi là chuyện bình thường của dữ liệu
    assert pd_.compare_outputs(_ho_so(), _ho_so(so_dong=90), cung_dau_vao=False) == []
    cot_moi = pd_.compare_outputs(_ho_so(), {"so_dong": 100, "cot": {"patient_id": _ho_so()["cot"]["patient_id"]}},
                                  cung_dau_vao=True)
    assert cot_moi == [("weight_kg", "cột 'weight_kg' không còn trong đầu ra")]


def test_chua_co_phien_ban_truoc_thi_qua(db_path, lo):
    assert pd_.check_pipeline_drift(BANG, _ho_so(can_nang=500.0), [("batch", lo[0])], db_path) == (True, [])


def test_cung_code_thi_khong_so(db_path, phien_ban_truoc):
    assert pd_.check_pipeline_drift(BANG, _ho_so(can_nang=176.0), phien_ban_truoc, db_path)[0]
    assert get_violations(db_path) == []


def test_code_doi_cung_dau_vao_lech_thi_chan(db_path, phien_ban_truoc, code_moi):
    dat, lech = pd_.check_pipeline_drift(BANG, _ho_so(can_nang=176.0), phien_ban_truoc, db_path)
    assert not dat and lech == ["cột 'weight_kg' trung bình 80 -> 176 (+120%)"]
    v = get_violations(db_path)[0]
    assert (v["error_type"], v["severity"], v["affected_table"]) == ("PIPELINE_OUTPUT_DRIFT", "BREAKING", BANG)
    assert "[Cột: weight_kg]" in v["details"] and code_moi in v["details"] and "CÙNG đầu vào" in v["details"]


def test_code_doi_khac_dau_vao_chi_canh_bao(db_path, lo, phien_ban_truoc, code_moi):
    dat, _ = pd_.check_pipeline_drift(BANG, _ho_so(can_nang=176.0), [("batch", lo[1])], db_path)
    assert dat and get_violations(db_path)[0]["severity"] == "WARNING"


def test_code_doi_nhung_dau_ra_giong_thi_qua(db_path, phien_ban_truoc, code_moi):
    assert pd_.check_pipeline_drift(BANG, _ho_so(), phien_ban_truoc, db_path) == (True, [])


def test_chay_lai_dung_code_va_dau_vao_da_co_thi_qua(tmp_path, db_path, phien_ban_truoc, code_moi):
    """Code mới đã từng tạo phiên bản từ cùng đầu vào (ví dụ đã duyệt trước đó) -> chạy lại không bị chặn."""
    tep = tmp_path / "std_2.csv"
    tep.write_text("a\n2\n", encoding="utf-8")
    pv.record_dataset_version(BANG, "standardized", str(tep), 100, phien_ban_truoc, db_path=db_path,
                              ho_so=_ho_so(can_nang=176.0))
    assert pd_.check_pipeline_drift(BANG, _ho_so(can_nang=176.0), phien_ban_truoc, db_path)[0]


def test_duyet_phien_ban_code_thi_khong_chan(db_path, phien_ban_truoc, code_moi):
    pd_.approve_code_version("engineer_alice", code_moi, "cố ý đổi đơn vị", db_path)
    assert pd_.check_pipeline_drift(BANG, _ho_so(can_nang=176.0), phien_ban_truoc, db_path)[0]
    dong = get_audit_logs(db_path=db_path)[0]
    assert (dong["ai"], dong["lam_gi"], dong["ket_qua"]) == ("engineer_alice", f"approve_code_version:{code_moi}",
                                                             "SUCCESS")


def test_khong_co_quyen_ghi_thi_khong_duoc_duyet(db_path, code_moi):
    with pytest.raises(PermissionError):
        pd_.approve_code_version("analyst_dave", code_moi, db_path=db_path)
    assert get_audit_logs(db_path=db_path)[0]["ket_qua"] == "DENIED"


def test_phien_ban_code_doi_khi_bat_loi_gia_lap(monkeypatch):
    goc = pv.get_code_version()
    assert "+src." in goc and "+loi." not in goc
    monkeypatch.setenv("PIPELINE_LOI_GIA_LAP", "dao_nhan_tu_vong")
    assert pv.get_code_version() == goc + "+loi.dao_nhan_tu_vong"


def test_so_voi_phien_ban_cung_dau_vao_trong_lich_su(tmp_path, db_path, lo, phien_ban_truoc, code_moi, monkeypatch):
    """
    Phiên bản MỚI NHẤT có đầu vào khác (ví dụ lần chạy trước có một lô bị chặn), nhưng lịch sử có phiên bản do
    code cũ tạo từ đúng đầu vào này -> so với phiên bản đó và chặn, không chỉ cảnh báo.
    """
    monkeypatch.delenv("PIPELINE_LOI_GIA_LAP")                      # phiên bản thứ hai vẫn do code cũ tạo
    tep = tmp_path / "std_2.csv"
    tep.write_text("a\n2\n", encoding="utf-8")
    pv.record_dataset_version(BANG, "standardized", str(tep), 90, [("batch", lo[1])], db_path=db_path,
                              ho_so=_ho_so(so_dong=90, can_nang=82.0))
    monkeypatch.setenv("PIPELINE_LOI_GIA_LAP", "can_nang_pound")
    dat, lech = pd_.check_pipeline_drift(BANG, _ho_so(can_nang=176.0), phien_ban_truoc, db_path)
    assert not dat and lech == ["cột 'weight_kg' trung bình 80 -> 176 (+120%)"]
    assert get_violations(db_path)[0]["severity"] == "BREAKING"
