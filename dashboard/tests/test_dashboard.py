"""
Kiểm thử dashboard bằng streamlit.testing (chạy app không cần trình duyệt) trên DB tạm
đã có dữ liệu giả đi qua pipeline. Tự bỏ qua nếu máy chưa cài streamlit.
"""

import os

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from catalog_access import get_audit_logs  # noqa: E402
from pipeline import build_ai_ready, inject_errors, standardize  # noqa: E402

APP = os.path.join(os.path.dirname(__file__), "..", "app.py")
TRANG = ["1. 📚 Catalog (Danh bạ dữ liệu)",
         "2. 🔐 Truy cập dữ liệu (RBAC)",
         "3. 🔗 Lineage (Đồ thị nguồn gốc)",
         "4. 🧾 Provenance (Lần chạy & lô dữ liệu)",
         "5. ⚠️ Vi phạm (Violations & Chặn lỗi)",
         "6. 📋 Audit (Sổ nhật ký)"]


@pytest.fixture
def app(mt, monkeypatch):
    """Dashboard đọc DB tạm: 1 lần chạy sạch + 1 lần chạy có lỗi đổi tên cột ở viện B."""
    import catalog_access.init_db as init_db
    monkeypatch.setattr(init_db, "DEFAULT_DB_PATH", mt["db"])
    standardize.main(mt["db"], mt["raw"], mt["std"])
    build_ai_ready.main(mt["db"], mt["std"], mt["ai"])
    inject_errors.gay_loi("doi_ten", mt["raw"], mt["backup"])
    standardize.main(mt["db"], mt["raw"], mt["std"])

    at = AppTest.from_file(APP, default_timeout=120)
    at.run()
    assert not at.exception
    return at


def _mo_trang(at, trang, nguoi_dung="engineer_alice"):
    at.sidebar.selectbox[0].set_value(nguoi_dung)
    at.sidebar.radio[0].set_value(trang)
    at.run()
    assert not at.exception, at.exception
    return at


@pytest.mark.parametrize("trang", TRANG)
def test_moi_trang_hien_thi_khong_loi(app, trang):
    _mo_trang(app, trang)


def test_trang_thai_theo_lan_chay_gan_nhat(app):
    canh_bao = " ".join(e.value for e in app.sidebar.error)
    assert "LẦN CHẠY GẦN NHẤT CÓ LỖI BREAKING" in canh_bao


def test_nha_nghien_cuu_doc_raw_bi_tu_choi_va_ghi_audit(app, mt):
    at = _mo_trang(app, TRANG[1], "researcher_bob")
    lua_chon = next(o for o in at.selectbox[0].options if o.startswith("raw_patient_hospital_B"))
    at.selectbox[0].set_value(lua_chon)
    at.button[0].click()
    at.run()
    assert any("TỪ CHỐI TRUY CẬP" in e.value for e in at.error)
    dong = get_audit_logs(db_path=mt["db"])[0]
    assert (dong["ai"], dong["bang_nao"], dong["ket_qua"]) == ("researcher_bob", "raw_patient_hospital_B", "DENIED")


def test_chuyen_vien_phan_tich_bi_an_cot_dinh_danh(app):
    at = _mo_trang(app, TRANG[1], "analyst_dave")
    at.selectbox[0].set_value(next(o for o in at.selectbox[0].options if o.startswith("standardized_patients")))
    at.button[0].click()
    at.run()
    assert any("ĐƯỢC PHÉP" in s.value for s in at.success)
    assert any("patient_id" in w.value for w in at.warning)
    assert "patient_id" not in at.dataframe[-1].value.columns


def test_khach_khong_xem_duoc_so_audit(app):
    at = _mo_trang(app, TRANG[5], "guest_charlie")
    assert any("không được xem nội dung sổ audit" in w.value for w in at.warning)
    assert any("Chuỗi băm toàn vẹn" in s.value for s in at.success)


def test_vi_pham_hien_lo_gay_loi(app):
    at = _mo_trang(app, TRANG[4])
    noi_dung = " ".join(m.value for m in at.markdown)
    assert "SCHEMA_COLUMN_RENAMED" in noi_dung and "hospital_B/" in noi_dung


def test_provenance_kiem_tra_checksum(app):
    at = _mo_trang(app, TRANG[3])
    at.button[0].click()
    at.run()
    assert any("khớp checksum" in s.value for s in at.success)
