"""
Kiểm thử cổng kiểm tra trên dữ liệu giả mô phỏng eICU.
Bắt buộc: dữ liệu sạch thì đạt + đủ 5 lỗi của inject_errors:
  đổi tên cột, đổi kiểu cột, xóa cột, nhịp tim = 500, xóa trắng 50% cân nặng.
"""

import numpy as np
import pytest

from catalog_access import get_audit_logs
from governance.gates import check_gate, check_gate_detailed
from governance.lineage import record_sample_lineage
from governance.sample_data import build_ai_ready, make_hospital, make_patient, make_vital, standardize_patients
from governance.schema_registry import get_schema_history
from governance.violation_engine import get_violations

AI = "ai_ready_icu_mortality"
BN_B = "raw_patient_hospital_B"


@pytest.fixture
def db(db_path):
    """DB tạm đã có lineage mẫu raw -> standardized -> ai_ready."""
    record_sample_lineage(db_path)
    return db_path


def _qua_2_cong(ten_bang, df, db):
    """Chạy gate1 rồi gate2 như pipeline thật; trả về (đạt?, danh sách lỗi)."""
    dat1, loi1 = check_gate(ten_bang, df, "gate1", db)
    dat2, loi2 = check_gate(ten_bang, df, "gate2", db)
    return dat1 and dat2, loi1 + loi2


def _loai_loi(db, muc_do="BREAKING"):
    return [v["error_type"] for v in get_violations(db, severity=muc_do)]


# ---------------------------------------------------------------------------
# Dữ liệu sạch
# ---------------------------------------------------------------------------

def test_du_lieu_sach_ca_3_vien_deu_dat(db):
    raw = {v: make_hospital(v) for v in "ABC"}
    for vien, bang in raw.items():
        assert _qua_2_cong(f"raw_patient_hospital_{vien}", bang["patient"], db) == (True, [])
        assert _qua_2_cong(f"raw_lab_hospital_{vien}", bang["lab"], db) == (True, [])
        assert _qua_2_cong(f"raw_vital_hospital_{vien}", bang["vital"], db) == (True, [])

    std = standardize_patients(raw)
    assert _qua_2_cong("standardized_patients", std, db) == (True, [])
    assert _qua_2_cong(AI, build_ai_ready(std), db) == (True, [])

    assert get_violations(db) == []
    ket_qua_cong = {a["ket_qua"] for a in get_audit_logs(db_path=db) if a["ai"] == "governance_gate"}
    assert ket_qua_cong == {"SUCCESS"}


def test_gui_lai_nhieu_dot_van_dat(db):
    """3 đợt snapshot giống schema -> không báo lỗi, không sinh version trùng."""
    for _ in range(3):
        assert check_gate(BN_B, make_patient("B"), "gate1", db) == (True, [])
    assert len(get_schema_history(BN_B, db)) == 1


# ---------------------------------------------------------------------------
# Lỗi 1: đổi tên cột admissionweight -> weight (kịch bản demo chính)
# ---------------------------------------------------------------------------

def test_loi_doi_ten_cot_bi_chan(db):
    df = make_patient("B")
    check_gate(BN_B, df, "gate1", db)                         # đợt sạch -> schema v1

    dat, loi = check_gate(BN_B, df.rename(columns={"admissionweight": "weight"}), "gate1", db)
    assert dat is False
    assert len(loi) == 1 and "SCHEMA_COLUMN_RENAMED" in loi[0] and "weight" in loi[0]

    v = get_violations(db)[0]
    assert (v["error_type"], v["severity"], v["hospital"]) == ("SCHEMA_COLUMN_RENAMED", "BREAKING", "Bệnh viện B")
    assert v["affected_ai_table"] == AI and v["action"] == "đã chặn"

    audit = get_audit_logs(db_path=db)
    assert any(a["ai"] == "governance_gate" and a["bang_nao"] == BN_B and a["ket_qua"] == "BLOCKED" for a in audit)
    assert any(a["ai"] == "violation_engine" and a["ket_qua"] == "BLOCKED" for a in audit)

    # Phiên bản schema lỗi được đánh dấu gây hỏng
    lich_su = get_schema_history(BN_B, db)
    assert [h["is_breaking"] for h in lich_su] == [0, 1]


def test_doi_ten_ngay_dot_dau_van_bi_chan(db):
    """Chưa có schema trước để so -> hợp đồng bắt được là thiếu cột bắt buộc."""
    dat, loi = check_gate(BN_B, make_patient("B").rename(columns={"admissionweight": "weight"}), "gate1", db)
    assert dat is False
    assert _loai_loi(db) == ["MISSING_REQUIRED_COLUMN"]
    assert get_violations(db)[0]["affected_ai_table"] == AI


def test_vien_B_sua_loi_thi_lai_dat(db):
    df = make_patient("B")
    check_gate(BN_B, df, "gate1", db)
    check_gate(BN_B, df.rename(columns={"admissionweight": "weight"}), "gate1", db)
    assert check_gate(BN_B, df, "gate1", db) == (True, [])


# ---------------------------------------------------------------------------
# Lỗi 2: đổi kiểu cột
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("doi_kieu", [
    lambda s: s.astype(str),                       # số -> chuỗi
    lambda s: s.map(lambda x: f"{x} kg"),          # thêm đơn vị vào số
])
def test_loi_doi_kieu_cot_bi_chan(db, doi_kieu):
    df = make_patient("A")
    check_gate("raw_patient_hospital_A", df, "gate1", db)
    loi_df = df.assign(admissionweight=doi_kieu(df["admissionweight"]))

    dat, loi = check_gate("raw_patient_hospital_A", loi_df, "gate1", db)
    assert dat is False
    assert _loai_loi(db) == ["SCHEMA_TYPE_CHANGED"]
    assert "hợp đồng yêu cầu float" in loi[0]


# ---------------------------------------------------------------------------
# Lỗi 3: xóa cột
# ---------------------------------------------------------------------------

def test_loi_xoa_cot_bi_chan(db):
    df = make_patient("C")
    check_gate("raw_patient_hospital_C", df, "gate1", db)
    dat, _ = check_gate("raw_patient_hospital_C", df.drop(columns=["admissionweight", "ethnicity"]), "gate1", db)
    assert dat is False
    # Cột bắt buộc -> BREAKING; cột ngoài hợp đồng (ethnicity) -> chỉ WARNING
    assert _loai_loi(db) == ["SCHEMA_COLUMN_REMOVED"]
    canh_bao = get_violations(db, severity="WARNING")
    assert len(canh_bao) == 1 and "ethnicity" in canh_bao[0]["details"]


# ---------------------------------------------------------------------------
# Lỗi 4: nhịp tim = 500
# ---------------------------------------------------------------------------

def test_loi_nhip_tim_500_nhieu_dong_bi_chan(db):
    df = make_vital("A")
    df.loc[df.sample(frac=0.10, random_state=1).index, "heartrate"] = 500

    dat, loi = check_gate("raw_vital_hospital_A", df, "gate2", db)
    assert dat is False
    v = get_violations(db)[0]
    assert (v["error_type"], v["severity"]) == ("PLAUSIBILITY", "BREAKING")
    assert v["affected_ai_table"] == AI           # heartrate -> heartrate_mean -> AI


def test_nhip_tim_500_mot_dong_chi_canh_bao(db):
    """Luật 1%: 1/400 dòng = 0,25% -> WARNING, vẫn cho qua nhưng ghi lại."""
    df = make_vital("C")
    df.loc[0, "heartrate"] = 500

    dat, loi = check_gate("raw_vital_hospital_C", df, "gate2", db)
    assert dat is True
    assert len(loi) == 1 and loi[0].startswith("[WARNING] PLAUSIBILITY")
    assert get_violations(db)[0]["action"] == "cảnh báo"


# ---------------------------------------------------------------------------
# Lỗi 5: xóa trắng 50% cân nặng
# ---------------------------------------------------------------------------

def test_loi_xoa_trang_50_phan_tram_can_nang_bi_chan(db):
    df = make_patient("A")
    df.loc[df.index[: len(df) // 2], "admissionweight"] = np.nan   # ít nhất 50% trống

    dat, loi = check_gate("raw_patient_hospital_A", df, "gate2", db)
    assert dat is False
    assert _loai_loi(db) == ["COMPLETENESS"]
    assert "vượt ngưỡng 40%" in loi[0]


def test_dot_sach_thieu_30_8_phan_tram_can_nang_van_dat(db):
    """Viện A đợt 3 thật: 13 bệnh nhân, 4 ô cân nặng trống (30,8%) -> không bị chặn oan."""
    df = make_patient("A").head(13).copy()
    df["admissionweight"] = [70.0] * 9 + [np.nan] * 4
    assert check_gate("raw_patient_hospital_A", df, "gate2", db) == (True, [])


# ---------------------------------------------------------------------------
# Các trường hợp khác
# ---------------------------------------------------------------------------

def test_loi_o_raw_lab_van_ghi_vi_pham_nhung_khong_anh_huong_ai(db):
    """raw_lab không chảy vào AI-ready -> vẫn chặn và ghi violation, affected_ai_table để trống."""
    bang = "raw_lab_hospital_A"
    df = make_hospital("A")["lab"]
    check_gate(bang, df, "gate1", db)
    dat, _ = check_gate(bang, df.drop(columns=["labresult"]), "gate1", db)
    assert dat is False
    v = get_violations(db)[0]
    assert (v["error_type"], v["severity"], v["affected_table"]) == ("SCHEMA_COLUMN_REMOVED", "BREAKING", bang)
    assert v["affected_ai_table"] == ""
    audit = get_audit_logs(db_path=db)
    assert any(a["ai"] == "governance_gate" and a["bang_nao"] == bang and a["ket_qua"] == "BLOCKED" for a in audit)

def test_gia_tri_gioi_tinh_sai(db):
    df = make_patient("A")
    df.loc[df.index[:8], "gender"] = "X"
    dat, _ = check_gate("raw_patient_hospital_A", df, "gate2", db)
    assert dat is False and _loai_loi(db) == ["CONFORMANCE"]


def test_standardized_gioi_tinh_phai_la_M_F_U(db):
    std = standardize_patients({"A": make_hospital("A")})
    std["gender"] = std["gender"].map({"M": "Male", "F": "Female", "U": "U"})
    dat, _ = check_gate("standardized_patients", std, "gate2", db)
    assert dat is False


def test_bang_rong_cua_vien_B_dat(db):
    assert _qua_2_cong("raw_vital_hospital_B", make_vital("B"), db) == (True, [])


def test_khong_co_hop_dong_thi_dat_kem_canh_bao(db):
    dat, loi = check_gate("bang_la", make_patient("A"), "gate1", db)
    assert dat is True and "NO_CONTRACT" in loi[0]
    dat, loi = check_gate("bang_la", make_patient("A"), "gate2", db)
    assert dat is True and "NO_CONTRACT" in loi[0]
    # Chỉ ghi 1 dòng violations (ở gate1), không trùng
    assert len(get_violations(db)) == 1


def test_detailed_tra_ve_dict_day_du(db):
    df = make_patient("A")
    df.loc[df.index[: len(df) // 2], "admissionweight"] = np.nan
    van_de = check_gate_detailed("raw_patient_hospital_A", df, "gate2", db)
    assert {"rule", "column", "severity", "message", "observed", "expected"} <= set(van_de[0])
    assert van_de[0]["rule"] == "completeness.max_null_pct"
    assert van_de[0]["affected_ai_tables"] == [AI]


def test_chay_thu_khong_ghi_db(db):
    df = make_patient("B").rename(columns={"admissionweight": "weight"})
    van_de = check_gate_detailed(BN_B, df, "gate1", db, ghi_nhan=False)
    assert van_de[0]["severity"] == "BREAKING"
    assert get_violations(db) == [] and get_schema_history(BN_B, db) == []
    assert get_audit_logs(db_path=db) == []


def test_cong_sai_bao_loi(db):
    with pytest.raises(ValueError):
        check_gate(BN_B, make_patient("B"), "gate3", db)


def test_goi_3_tham_so_nhu_CLAUDE_md(db, monkeypatch):
    """Người 1 gọi check_gate(ten_bang, du_lieu, cong) không truyền db_path."""
    import catalog_access.init_db as init_db
    monkeypatch.setattr(init_db, "DEFAULT_DB_PATH", db)
    assert check_gate(BN_B, make_patient("B"), "gate1") == (True, [])
    assert len(get_schema_history(BN_B, db)) == 1
