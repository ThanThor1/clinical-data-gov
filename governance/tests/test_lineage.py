"""Kiểm thử lineage: ghi không trùng, dựng đồ thị, tìm bảng AI-ready bị ảnh hưởng."""

from catalog_access import register_dataset
from governance.lineage import (
    build_graph,
    get_affected_ai_tables,
    get_downstream_tables,
    get_impact_paths,
    get_lineage_edges,
    record_lineage,
    record_sample_lineage,
)

AI = "ai_ready_icu_mortality"


def test_ghi_lineage_khong_trung(db_path):
    for _ in range(3):
        record_lineage("raw_patient_hospital_B", "standardized_patients",
                       {"admissionweight": "weight_kg", "patientunitstayid": "patient_id"},
                       db_path=db_path)
    canh = get_lineage_edges(db_path)
    assert len(canh) == 2
    assert canh[0]["transformation"] == "direct"


def test_goi_dung_3_tham_so_nhu_CLAUDE_md(db_path, monkeypatch):
    """Người 1 gọi record_lineage(nguon, dich, anh_xa_cot) không truyền db_path."""
    import catalog_access.init_db as init_db
    monkeypatch.setattr(init_db, "DEFAULT_DB_PATH", db_path)
    record_lineage("raw_patient_hospital_A", "standardized_patients", {"age": "age"})
    assert len(get_lineage_edges(db_path)) == 1


def test_do_thi_co_node_cot_va_node_bang(db_path):
    record_sample_lineage(db_path)
    g = build_graph(db_path)
    assert g.nodes["raw_patient_hospital_B.admissionweight"]["kind"] == "column"
    assert g.nodes["raw_patient_hospital_B"]["kind"] == "table"
    assert g.has_edge("raw_patient_hospital_B", "standardized_patients")
    assert g.has_edge("standardized_patients.weight_kg", f"{AI}.weight_kg")


def test_bang_raw_patient_va_vital_noi_toi_ai_ready(db_path):
    record_sample_lineage(db_path)
    for vien in "ABC":
        for loai in ("patient", "vital"):
            bang = f"raw_{loai}_hospital_{vien}"
            assert get_affected_ai_tables(bang, db_path=db_path) == [AI], bang
        assert get_downstream_tables(f"raw_patient_hospital_{vien}", db_path=db_path) == [
            AI, "standardized_patients"]


def test_bang_raw_lab_chi_toi_standardized_labs(db_path):
    """AI-ready chưa có cột lab -> raw_lab chỉ chảy tới standardized_labs, không tới AI-ready."""
    record_sample_lineage(db_path)
    assert not any(c["source_table"] == "standardized_labs" for c in get_lineage_edges(db_path))
    for vien in "ABC":
        bang = f"raw_lab_hospital_{vien}"
        assert get_downstream_tables(bang, db_path=db_path) == ["standardized_labs"]
        assert get_affected_ai_tables(bang, db_path=db_path) == []
        assert get_affected_ai_tables(bang, "labresult", db_path) == []


def test_du_duong_di_muc_cot_cho_ca_3_vien(db_path):
    """Các cột mà inject_errors làm hỏng phải nối được tới AI-ready ở cả 3 viện."""
    record_sample_lineage(db_path)
    for vien in "ABC":
        for bang, cot, cot_ai in [
            (f"raw_patient_hospital_{vien}", "admissionweight", "weight_kg"),
            (f"raw_patient_hospital_{vien}", "gender", "gender"),
            (f"raw_patient_hospital_{vien}", "age", "age"),
            (f"raw_patient_hospital_{vien}", "patientunitstayid", "patient_hash"),
            (f"raw_vital_hospital_{vien}", "heartrate", "heartrate_mean"),
        ]:
            assert get_affected_ai_tables(bang, cot, db_path) == [AI], (bang, cot)
            assert any(p.endswith(f"{AI}.{cot_ai}") for p in get_impact_paths(bang, cot, db_path)), (bang, cot)


def test_tra_theo_cot(db_path):
    record_sample_lineage(db_path)
    assert get_affected_ai_tables("raw_patient_hospital_B", "admissionweight", db_path) == [AI]
    assert get_affected_ai_tables("raw_vital_hospital_A", "heartrate", db_path) == [AI]
    duong = get_impact_paths("raw_patient_hospital_B", "admissionweight", db_path)
    assert duong == ["raw_patient_hospital_B.admissionweight -> standardized_patients.weight_kg"
                     f" -> {AI}.weight_kg"]


def test_cot_chua_co_lineage_thi_tra_theo_bang(db_path):
    record_sample_lineage(db_path)
    # ethnicity không có trong lineage -> tra theo bảng (thận trọng)
    assert get_affected_ai_tables("raw_patient_hospital_A", "ethnicity", db_path) == [AI]


def test_cot_khong_chay_toi_ai_thi_khong_anh_huong(db_path):
    record_lineage("raw_x", "standardized_x", {"a": "a"}, db_path=db_path)
    record_lineage("standardized_x", "ai_ready_y", {"b": "b"}, db_path=db_path)
    # Cột a chỉ đi tới standardized_x.a, không tới ai_ready_y
    assert get_affected_ai_tables("raw_x", "a", db_path) == []
    # Tra theo bảng thì có ảnh hưởng
    assert get_affected_ai_tables("raw_x", db_path=db_path) == ["ai_ready_y"]


def test_nhan_dien_tang_qua_bang_datasets(db_path):
    """Bảng không có tiền tố ai_ready_ nhưng đăng ký tier ai_ready vẫn được nhận ra."""
    register_dataset("mo_hinh_sepsis", "Toàn hệ thống", "ai_ready", "AI Lab", db_path=db_path)
    record_lineage("standardized_patients", "mo_hinh_sepsis", {"age": "age"}, db_path=db_path)
    record_lineage("raw_patient_hospital_A", "standardized_patients", {"age": "age"}, db_path=db_path)
    assert get_affected_ai_tables("raw_patient_hospital_A", "age", db_path) == ["mo_hinh_sepsis"]


def test_bang_ai_ready_tu_anh_huong_chinh_no(db_path):
    assert get_affected_ai_tables(AI, db_path=db_path) == [AI]


def test_bang_khong_co_lineage(db_path):
    assert get_downstream_tables("bang_la", db_path=db_path) == []
    assert get_affected_ai_tables("bang_la", db_path=db_path) == []
