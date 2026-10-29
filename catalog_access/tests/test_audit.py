"""Kiểm thử sổ audit: 3 cách gọi log_event, ngữ cảnh run_id, chuỗi băm chống sửa."""

import sqlite3
import threading

import pytest

from catalog_access import audit_context, get_audit_context, get_audit_logs, log_event, verify_audit_chain
from catalog_access.audit import HASH_GOC, compute_row_hash


def _sua(db, cau_lenh, tham_so=()):
    conn = sqlite3.connect(db)
    conn.execute(cau_lenh, tham_so)
    conn.commit()
    conn.close()


def _ghi(db, so_dong):
    for i in range(so_dong):
        log_event(f"nguoi_{i}", "read", "raw_patient_hospital_A", "ALLOWED", db_path=db)


def test_ba_cach_goi_log_event(db_path):
    log_event("engineer_alice", "read", "raw_patient_hospital_A", "ALLOWED", db_path=db_path)
    log_event("2026-10-16 22:10:00", "nhà nghiên cứu", "read", "raw_patient_hospital_B", "DENIED", db_path=db_path)
    log_event(ai="pipeline_worker", lam_gi="check_gate", bang_nao="raw_patient_hospital_B", ket_qua="BLOCKED",
              db_path=db_path)

    dong = list(reversed(get_audit_logs(db_path=db_path)))
    assert [(d["ai"], d["lam_gi"], d["bang_nao"], d["ket_qua"]) for d in dong] == [
        ("engineer_alice", "read", "raw_patient_hospital_A", "ALLOWED"),
        ("nhà nghiên cứu", "read", "raw_patient_hospital_B", "DENIED"),
        ("pipeline_worker", "check_gate", "raw_patient_hospital_B", "BLOCKED"),
    ]
    assert dong[1]["thoi_gian"] == "2026-10-16 22:10:00"
    # Tên cột tiếng Anh vẫn có cho code cũ
    assert (dong[2]["actor"], dong[2]["action"], dong[2]["target_table"], dong[2]["result"]) == (
        "pipeline_worker", "check_gate", "raw_patient_hospital_B", "BLOCKED")


def test_ket_qua_la_duoc_chuan_hoa(db_path):
    log_event("u", "read", "t", "ok", db_path=db_path)
    log_event("u", "read", "t", "gia_tri_la", db_path=db_path)
    assert [d["ket_qua"] for d in get_audit_logs(db_path=db_path)] == ["DENIED", "ALLOWED"]


def test_loc_theo_nguoi_dung_ket_qua_va_lan_chay(db_path):
    log_event("researcher_bob", "read", "raw_x", "DENIED", db_path=db_path)
    log_event("engineer_alice", "read", "raw_x", "ALLOWED", run_id="run_1", db_path=db_path)
    assert [d["ai"] for d in get_audit_logs(ai="bob", db_path=db_path)] == ["researcher_bob"]
    assert [d["ai"] for d in get_audit_logs(result="allowed", db_path=db_path)] == ["engineer_alice"]
    assert [d["ai"] for d in get_audit_logs(run_id="run_1", db_path=db_path)] == ["engineer_alice"]
    df = get_audit_logs(db_path=db_path, as_df=True)
    assert len(df) == 2 and {"thoi_gian", "ai", "lam_gi", "bang_nao", "ket_qua", "run_id"} <= set(df.columns)


def test_ngu_canh_tu_gan_run_id(db_path):
    assert get_audit_context() == {"run_id": "", "batch": ""}
    with audit_context(run_id="run_x", batch="hospital_A/dot_1"):
        log_event("u", "read", "t", "ALLOWED", db_path=db_path)
        with audit_context(batch="hospital_A/dot_2") as ngu_canh:
            assert ngu_canh == {"run_id": "run_x", "batch": "hospital_A/dot_2"}
        assert get_audit_context()["batch"] == "hospital_A/dot_1"
    log_event("u", "read", "t", "ALLOWED", db_path=db_path)
    assert get_audit_context() == {"run_id": "", "batch": ""}
    assert [d["run_id"] for d in get_audit_logs(db_path=db_path)] == ["", "run_x"]


def test_chuoi_bam_toan_ven(db_path):
    assert verify_audit_chain(db_path)["ok"]          # sổ rỗng vẫn toàn vẹn
    _ghi(db_path, 5)
    kq = verify_audit_chain(db_path)
    assert kq["ok"] and kq["so_dong"] == 5 and len(kq["hash_cuoi"]) == 64 and kq["dong_loi"] is None

    conn = sqlite3.connect(db_path)
    dong = conn.execute("SELECT id, prev_hash, row_hash FROM audit_log ORDER BY id").fetchall()
    conn.close()
    assert dong[0][1] == HASH_GOC
    assert all(dong[i][1] == dong[i - 1][2] for i in range(1, len(dong)))


def test_bam_doi_khi_bat_ky_truong_nao_doi():
    goc = ("p" * 64, 1, "2026-10-29 08:00:00", "u", "read", "t", "ALLOWED", "")
    khoa = b"khoa-kiem-thu-0123456789"
    bam_goc = compute_row_hash(*goc, khoa=khoa)
    for i in range(len(goc)):
        doi = list(goc)
        doi[i] = 2 if i == 1 else str(doi[i]) + "x"
        assert compute_row_hash(*doi, khoa=khoa) != bam_goc, i
    assert compute_row_hash(*goc, khoa=b"khoa-khac-0123456789") != bam_goc


def test_phat_hien_sua_noi_dung(db_path):
    _ghi(db_path, 5)
    _sua(db_path, "UPDATE audit_log SET ket_qua = 'DENIED' WHERE id = 3")
    kq = verify_audit_chain(db_path)
    assert kq["ok"] is False and kq["dong_loi"] == 3 and "bị sửa" in kq["ly_do"]


def test_phat_hien_xoa_dong_o_giua(db_path):
    _ghi(db_path, 5)
    _sua(db_path, "DELETE FROM audit_log WHERE id = 3")
    kq = verify_audit_chain(db_path)
    assert kq["ok"] is False and kq["dong_loi"] == 4


def test_phat_hien_xoa_dong_cuoi_va_dau_vet_khong_mat_khi_ghi_tiep(db_path):
    _ghi(db_path, 6)
    _sua(db_path, "DELETE FROM audit_log WHERE id >= 5")
    kq = verify_audit_chain(db_path)
    assert kq["ok"] is False and "Mất 2 dòng cuối" in kq["ly_do"]

    _ghi(db_path, 4)            # ghi tiếp cũng không lấp được dấu vết
    kq = verify_audit_chain(db_path)
    assert kq["ok"] is False and kq["dong_loi"] == 5


def test_phat_hien_dong_gia_khi_khong_co_khoa(db_path):
    _ghi(db_path, 3)
    conn = sqlite3.connect(db_path)
    _, cuoi = conn.execute("SELECT id, row_hash FROM audit_log ORDER BY id DESC LIMIT 1").fetchone()
    gia = ("2026-10-29 03:00:00", "researcher_bob", "read:raw", "raw_patient_hospital_A", "ALLOWED", "")
    conn.execute("INSERT INTO audit_log (id, thoi_gian, ai, lam_gi, bang_nao, ket_qua, run_id, prev_hash, row_hash) "
                 "VALUES (4, ?, ?, ?, ?, ?, ?, ?, ?)",
                 (*gia, cuoi, compute_row_hash(cuoi, 4, *gia, khoa=b"khoa-doan-mo-0123456789")))
    conn.commit()
    conn.close()
    kq = verify_audit_chain(db_path)
    assert kq["ok"] is False and kq["dong_loi"] == 4


def test_doi_khoa_audit_thi_so_cu_khong_xac_minh_duoc(db_path, monkeypatch):
    _ghi(db_path, 2)
    monkeypatch.setenv("AUDIT_LOG_SECRET", "mot-khoa-hoan-toan-khac-0123")
    assert verify_audit_chain(db_path)["ok"] is False


def test_nhieu_luong_ghi_cung_luc_van_giu_chuoi(db_path):
    """Mỗi lần ghi giữ khóa BEGIN IMMEDIATE -> không có hai dòng cùng nối vào một mắt xích."""
    loi = []

    def _luong(stt):
        try:
            for i in range(20):
                log_event(f"luong_{stt}", "read", f"bang_{i}", "ALLOWED", db_path=db_path)
        except Exception as e:      # pragma: no cover - chỉ để báo lỗi rõ ràng nếu có
            loi.append(e)

    cac_luong = [threading.Thread(target=_luong, args=(i,)) for i in range(4)]
    for t in cac_luong:
        t.start()
    for t in cac_luong:
        t.join()
    assert loi == []
    kq = verify_audit_chain(db_path)
    assert kq["ok"] and kq["so_dong"] == 80


@pytest.mark.parametrize("so_khoa", ["ngan", ""])
def test_khoa_audit_qua_ngan_bi_tu_choi(db_path, monkeypatch, so_khoa):
    monkeypatch.setenv("AUDIT_LOG_SECRET", so_khoa)
    with pytest.raises(ValueError):
        log_event("u", "read", "t", "ALLOWED", db_path=db_path)
