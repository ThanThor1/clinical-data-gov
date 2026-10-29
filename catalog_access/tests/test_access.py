"""Kiểm thử phân quyền RBAC: quyết định theo vai trò / tầng, ghi audit, AccessDenied, ẩn cột nhạy cảm."""

import pytest

from catalog_access import (
    AccessDenied,
    can_view_audit,
    check_access,
    evaluate_access,
    get_audit_logs,
    get_hidden_columns,
    register_dataset,
    require_access,
    resolve_role,
)

AI = "ai_ready_icu_mortality"


@pytest.mark.parametrize("nguoi_dung, bang, hanh_dong, ky_vong", [
    ("engineer_alice", "raw_patient_hospital_A", "read", True),
    ("engineer_alice", AI, "write", True),
    ("pipeline_service", "standardized_patients", "write", True),
    ("analyst_dave", "standardized_patients", "read", True),
    ("analyst_dave", AI, "read", True),
    ("analyst_dave", "raw_lab_hospital_C", "read", False),
    ("analyst_dave", "standardized_patients", "write", False),
    ("researcher_bob", AI, "read", True),
    ("researcher_bob", "raw_patient_hospital_B", "read", False),
    ("researcher_bob", "standardized_patients", "read", False),
    ("researcher_bob", AI, "write", False),
    ("guest_charlie", AI, "read", False),
    ("nhà nghiên cứu", "raw_patient_hospital_B", "read", False),
    ("kỹ sư dữ liệu", "raw_patient_hospital_B", "read", True),
    ("nguoi_khong_co_trong_danh_sach", AI, "read", False),
    ("engineer_alice", AI, "drop_table", False),
])
def test_ma_tran_quyen(db_path, nguoi_dung, bang, hanh_dong, ky_vong):
    assert check_access(nguoi_dung, bang, hanh_dong, db_path=db_path) is ky_vong


def test_moi_lan_kiem_tra_deu_ghi_audit(db_path):
    check_access("researcher_bob", "raw_patient_hospital_B", db_path=db_path)
    check_access("researcher_bob", AI, db_path=db_path)
    dong = get_audit_logs(db_path=db_path)
    assert [(d["ai"], d["lam_gi"], d["bang_nao"], d["ket_qua"]) for d in dong] == [
        ("researcher_bob", "read:ai_ready", AI, "ALLOWED"),
        ("researcher_bob", "read:raw", "raw_patient_hospital_B", "DENIED"),
    ]


def test_evaluate_va_auto_audit_false_khong_ghi_audit(db_path):
    kq = evaluate_access("researcher_bob", "raw_patient_hospital_B", db_path=db_path)
    assert kq["allowed"] is False and kq["role"] == "researcher" and kq["tier"] == "raw"
    assert check_access("engineer_alice", AI, db_path=db_path, auto_audit=False) is True
    assert get_audit_logs(db_path=db_path) == []


def test_require_access_nem_loi_va_ghi_DENIED(db_path):
    with pytest.raises(AccessDenied) as loi:
        require_access("researcher_bob", "raw_patient_hospital_B", db_path=db_path)
    assert loi.value.nguoi_dung == "researcher_bob" and loi.value.action == "read"
    assert "tầng [raw]" in loi.value.reason
    assert isinstance(loi.value, PermissionError)
    assert get_audit_logs(db_path=db_path)[0]["ket_qua"] == "DENIED"


def test_require_access_cho_phep_tra_ve_quyet_dinh(db_path):
    kq = require_access("pipeline_service", "standardized_labs", "write", db_path=db_path)
    assert kq["allowed"] and kq["role"] == "data_engineer" and kq["tier"] == "standardized"
    assert get_audit_logs(db_path=db_path)[0]["ket_qua"] == "ALLOWED"


def test_tang_lay_tu_catalog_truoc_ten_bang(db_path):
    """Bảng tên lạ nhưng đăng ký tier ai_ready -> nhà nghiên cứu đọc được; chưa đăng ký -> coi là raw."""
    register_dataset("mo_hinh_sepsis", "Toàn hệ thống", "ai_ready", "AI Lab", db_path=db_path)
    assert check_access("researcher_bob", "mo_hinh_sepsis", db_path=db_path) is True
    assert evaluate_access("researcher_bob", "bang_chua_dang_ky", db_path=db_path)["tier"] == "raw"
    assert check_access("researcher_bob", "bang_chua_dang_ky", db_path=db_path) is False


def test_xem_catalog(db_path):
    assert check_access("guest_charlie", "", "view_catalog", db_path=db_path) is True
    assert check_access("researcher_bob", "", "view_catalog", db_path=db_path) is True


def test_an_cot_nhay_cam_theo_vai_tro():
    nhay_cam = ["patient_id", "uniquepid", ""]
    assert get_hidden_columns("analyst_dave", nhay_cam) == ["patient_id", "uniquepid"]
    assert get_hidden_columns("researcher_bob", nhay_cam) == ["patient_id", "uniquepid"]
    assert get_hidden_columns("engineer_alice", nhay_cam) == []
    assert get_hidden_columns("pipeline_service", nhay_cam) == []


def test_quyen_xem_audit():
    assert can_view_audit("engineer_alice") is True
    assert can_view_audit("analyst_dave") is False
    assert can_view_audit("researcher_bob") is False
    assert can_view_audit("nguoi_la") is False


@pytest.mark.parametrize("dinh_danh, vai_tro", [
    ("engineer_alice", "data_engineer"), ("pipeline_service", "data_engineer"),
    ("analyst_dave", "analyst"), ("Chuyên viên phân tích", "analyst"),
    ("researcher_bob", "researcher"), ("  Nhà Nghiên Cứu ", "researcher"),
    ("guest_charlie", "guest"), ("ai_do", "guest"),
])
def test_xac_dinh_vai_tro(dinh_danh, vai_tro):
    assert resolve_role(dinh_danh) == vai_tro
