"""
Kiểm thử dashboard bằng streamlit.testing (chạy app không cần trình duyệt) trên DB tạm
đã có dữ liệu giả đi qua pipeline Spark. Tự bỏ qua nếu máy chưa cài streamlit hoặc chưa có Spark.
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


@pytest.fixture(scope="module")
def du_lieu(mt_dung_chung, spark):
    """DB tạm dùng chung cho cả file: 1 lần chạy sạch + 1 lần chạy có lỗi đổi tên cột ở viện B."""
    mt = mt_dung_chung
    standardize.main(mt["db"], mt["raw"], mt["std"], spark)
    build_ai_ready.main(mt["db"], mt["std"], mt["ai"], spark)
    inject_errors.gay_loi("doi_ten", mt["raw"], mt["backup"])
    standardize.main(mt["db"], mt["raw"], mt["std"], spark)
    return mt


@pytest.fixture
def mt(du_lieu):
    return du_lieu


@pytest.fixture
def app(du_lieu, monkeypatch):
    import catalog_access.init_db as init_db
    monkeypatch.setattr(init_db, "DEFAULT_DB_PATH", du_lieu["db"])
    at = AppTest.from_file(APP, default_timeout=120)
    at.run()
    assert not at.exception
    return at


def _mo_trang(at, trang, nguoi_dung="engineer_alice"):
    """Vào trang `trang` bằng phiên đã đăng nhập của `nguoi_dung` (form đăng nhập có test riêng bên dưới)."""
    at.session_state["nguoi_dung"] = nguoi_dung
    at.run()
    at.sidebar.radio[0].set_value(trang)
    at.run()
    assert not at.exception, at.exception
    return at


@pytest.mark.parametrize("trang", TRANG)
def test_moi_trang_hien_thi_khong_loi(app, trang):
    _mo_trang(app, trang)


def test_trang_thai_theo_lan_chay_gan_nhat(app):
    at = _mo_trang(app, TRANG[0])
    canh_bao = " ".join(e.value for e in at.sidebar.error)
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


# ---------------------------------------------------------------------------
# Đăng nhập bằng mật khẩu
# ---------------------------------------------------------------------------

def _dang_nhap(at, ten, mat_khau):
    at.text_input[0].input(ten)
    at.text_input[1].input(mat_khau)
    at.button[0].click()
    at.run()
    assert not at.exception, at.exception
    return at


def test_chua_dang_nhap_chi_thay_form(app):
    assert len(app.sidebar.radio) == 0 and len(app.text_input) == 2


def test_sai_mat_khau_bi_tu_choi_va_ghi_audit(app, mt):
    at = _dang_nhap(app, "researcher_bob", "mat-khau-sai")
    assert any("Sai tài khoản hoặc mật khẩu" in e.value for e in at.error)
    assert "nguoi_dung" not in at.session_state and len(at.sidebar.radio) == 0
    dong = get_audit_logs(db_path=mt["db"])[0]
    assert (dong["ai"], dong["lam_gi"], dong["ket_qua"]) == ("researcher_bob", "login", "DENIED")


def test_dung_mat_khau_thi_vao_duoc(app, mt):
    at = _dang_nhap(app, "analyst_dave", "dave@2026")
    assert at.session_state["nguoi_dung"] == "analyst_dave" and len(at.sidebar.radio) == 1
    assert any("analyst_dave" in m.value for m in at.sidebar.markdown)
    dong = get_audit_logs(db_path=mt["db"])[0]
    assert (dong["ai"], dong["lam_gi"], dong["ket_qua"]) == ("analyst_dave", "login", "ALLOWED")


# ---------------------------------------------------------------------------
# Vòng đời vi phạm và duyệt đổi tên cột trên trang Vi phạm
# ---------------------------------------------------------------------------

def test_vi_pham_co_trang_thai(app):
    at = _mo_trang(app, TRANG[4])
    assert any(m.label == "Vi phạm đang mở" and int(m.value) > 0 for m in at.metric)
    assert "Đang mở" in " ".join(m.value for m in at.markdown)


def test_ky_su_duyet_doi_ten_cot_tren_dashboard(app, mt):
    from governance.schema_registry import get_schema_approvals

    at = _mo_trang(app, TRANG[4])
    next(b for b in at.button if b.label == "Duyệt đổi tên cột").click()
    at.run()
    assert not at.exception, at.exception
    assert any("Đã duyệt" in s.value for s in at.success)
    duyet = get_schema_approvals(db_path=mt["db"])
    assert [(a["table_name"], a["source_column"], a["target_column"], a["approved_by"]) for a in duyet] == [
        ("raw_patient_hospital_B", "weight", "admissionweight", "engineer_alice")]


def test_chuyen_vien_phan_tich_khong_duyet_duoc(app):
    at = _mo_trang(app, TRANG[4], "analyst_dave")
    assert any("không duyệt được" in i.value for i in at.info)
    assert not any(b.label == "Duyệt đổi tên cột" for b in at.button)
