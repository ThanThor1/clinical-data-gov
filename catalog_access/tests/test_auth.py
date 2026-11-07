"""Kiểm thử đăng nhập dashboard: băm PBKDF2, xác thực theo policies.yaml, ghi audit login."""

from catalog_access import get_audit_logs
from catalog_access.auth import authenticate, hash_password, verify_password


def test_bam_va_kiem_tra_mat_khau():
    bam = hash_password("mat-khau-thu", so_vong=1_000)
    assert bam.startswith("pbkdf2_sha256$1000$") and "mat-khau-thu" not in bam
    assert verify_password("mat-khau-thu", bam)
    assert not verify_password("mat-khau-sai", bam)


def test_cung_mat_khau_khac_muoi_thi_khac_bam():
    assert hash_password("abc", so_vong=1_000) != hash_password("abc", so_vong=1_000)


def test_chuoi_bam_sai_dang_thi_tu_choi():
    assert not verify_password("abc", "khong-phai-bam")
    assert not verify_password("abc", "md5$1$00$00")


def test_dang_nhap_dung_mat_khau_demo_va_ghi_audit(db_path):
    assert authenticate(" Engineer_Alice ", "alice@2026", db_path=db_path)
    dong = get_audit_logs(db_path=db_path)[0]
    assert (dong["ai"], dong["lam_gi"], dong["ket_qua"]) == ("engineer_alice", "login", "ALLOWED")


def test_sai_mat_khau_bi_tu_choi_va_ghi_audit(db_path):
    assert not authenticate("researcher_bob", "alice@2026", db_path=db_path)
    dong = get_audit_logs(db_path=db_path)[0]
    assert (dong["ai"], dong["lam_gi"], dong["ket_qua"]) == ("researcher_bob", "login", "DENIED")


def test_tai_khoan_khong_co_mat_khau_hoac_khong_ton_tai(db_path):
    assert not authenticate("pipeline_service", "", db_path=db_path)        # tài khoản dịch vụ
    assert not authenticate("data_engineer", "alice@2026", db_path=db_path)  # tên vai trò, không phải tài khoản
    assert not authenticate("", "", db_path=db_path)
    assert [d["ket_qua"] for d in get_audit_logs(db_path=db_path)] == ["DENIED"] * 3
