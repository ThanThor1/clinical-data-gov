"""
Kiểm thử pipeline Spark đầu-cuối trên dữ liệu GIẢ (governance/sample_data.py), không cần dữ liệu thật:
staging -> raw (3 đợt mỗi viện) -> standardized (Delta) -> AI-ready (Delta), kèm lỗi giả lập của inject_errors.
Mọi thứ nằm trong thư mục tạm và metadata.db tạm (fixture `mt`, `spark` ở conftest.py gốc repo).
Máy chưa có Spark / Java thì các test cần Spark tự bỏ qua.
"""

import os

import pandas as pd
import pytest

from catalog_access import AccessDenied, get_audit_logs, get_dataset, read_table
from governance import provenance as pv
from governance.violation_engine import get_violations
from pipeline import build_ai_ready, inject_errors, standardize

AI = "ai_ready_icu_mortality"


def _chay(mt, spark):
    kq = standardize.main(mt["db"], mt["raw"], mt["std"], spark)
    ai_df = build_ai_ready.main(mt["db"], mt["std"], mt["ai"], spark)
    return kq, ai_df


def _dot_3(mt, vien):
    return pd.read_csv(f"{inject_errors.get_batch_3_dir(vien, mt['raw'])}/patient.csv")


def _trang_thai(mt, run_id):
    return next(r["status"] for r in pv.get_runs(db_path=mt["db"]) if r["run_id"] == run_id)


# ---------------------------------------------------------------------------
# Tầng raw (không cần Spark)
# ---------------------------------------------------------------------------

def test_dang_ky_bang_raw_cho_moi_vien(mt):
    for vien in "ABC":
        ds = get_dataset(f"raw_patient_hospital_{vien}", mt["db"])
        assert ds["tier"] == "raw" and ds["file_path"].endswith("*/patient.csv")
        assert ds["contract"] == "raw_patient v1.1" and "patientunitstayid" in ds["sensitive_columns"]
    assert get_dataset("raw_patient_hospital_A", mt["db"])["row_count"] == 40
    assert get_dataset("raw_vital_hospital_B", mt["db"])["row_count"] == 0


def test_xep_hang_vien_theo_so_benh_nhan(mt):
    assert inject_errors.xep_hang_vien(mt["raw"]) == ["A", "B", "C"]


# ---------------------------------------------------------------------------
# Dữ liệu sạch
# ---------------------------------------------------------------------------

def test_du_lieu_sach_di_qua_3_tang(mt, spark):
    kq, ai_df = _chay(mt, spark)
    assert (kq["so_dot"], kq["so_dot_dat"], kq["bi_chan"], kq["so_dong"]) == (9, 9, [], 95)
    assert kq["trang_thai_lo"] == {"lab:ACCEPTED": 9, "patient:ACCEPTED": 9, "vital:ACCEPTED": 9}
    assert get_violations(mt["db"], severity="BREAKING") == []

    assert list(ai_df.columns) == build_ai_ready.COT_AI and len(ai_df) == 95
    assert ai_df["patient_hash"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert ai_df["creatinine_max"].notna().any() and ai_df["glucose_mean"].notna().any()

    for bang, tang, thu_muc in (("standardized_patients", "standardized", mt["std"]),
                                ("standardized_labs", "standardized", mt["std"]), (AI, "ai_ready", mt["ai"])):
        ds = get_dataset(bang, mt["db"])
        assert ds["tier"] == tang and ds["row_count"] > 0 and ds["file_path"].endswith(bang)
        assert os.path.isdir(os.path.join(thu_muc, bang, "_delta_log"))          # bảng Delta Lake
    std = read_table("engineer_alice", "standardized_patients", db_path=mt["db"])
    assert std["source_batch"].str.match(r"hospital_[ABC]/").all() and std["source_batch"].nunique() == 9
    assert {r["status"] for r in pv.get_runs(db_path=mt["db"])} == {"SUCCESS"}


def test_chay_lai_dung_lai_ho_so_cu(mt, spark):
    """Xử lý tăng dần: file không đổi thì không phải tính lại hồ sơ bằng Spark."""
    kq_1, _ = _chay(mt, spark)
    kq_2, ai_2 = _chay(mt, spark)
    assert (kq_1["ho_so_moi_tinh"], kq_2["ho_so_moi_tinh"]) == (27, 0)
    assert kq_2["bi_chan"] == [] and len(ai_2) == 95

    inject_errors.gay_loi("doi_ten", mt["raw"], mt["backup"])
    kq_3, _ = _chay(mt, spark)
    assert kq_3["ho_so_moi_tinh"] == 1                                         # chỉ file vừa bị sửa


def test_provenance_truy_nguoc_ve_27_lo(mt, spark):
    _chay(mt, spark)
    phien_ban = pv.get_latest_version(AI, mt["db"])
    assert phien_ban["table_version"] == 0
    lo = pv.source_batches(phien_ban["id"], mt["db"])
    # 3 viện x 3 đợt x 3 loại bảng (vital viện B rỗng vẫn được ghi lại)
    assert len(lo) == 27 and {b["status"] for b in lo} == {"ACCEPTED"}
    assert {b["table_name"] for b in lo if b["row_count"] == 0} == {"raw_vital_hospital_B"}
    assert pv.verify_version(phien_ban["id"], mt["db"]) == {"ok": True, "da_kiem": 30, "van_de": []}


def test_time_travel_kiem_tra_lai_phien_ban_cu(mt, spark):
    _chay(mt, spark)
    cu = pv.get_latest_version(AI, mt["db"])
    inject_errors.gay_loi("doi_ten", mt["raw"], mt["backup"])
    _chay(mt, spark)
    moi = pv.get_latest_version(AI, mt["db"])
    assert (cu["table_version"], moi["table_version"]) == (0, 1) and cu["checksum"] != moi["checksum"]
    assert pv.verify_version(moi["id"], mt["db"])["ok"]

    # Phiên bản cũ: các bảng Delta đọc lại được (time travel), nhưng file raw đợt 3 viện B mà nó từng dùng
    # vừa bị inject_errors sửa trực tiếp -> provenance chỉ ra đúng file đó
    kiem_tra = pv.verify_version(cu["id"], mt["db"])
    assert not kiem_tra["ok"] and len(kiem_tra["van_de"]) == 1
    assert "raw_patient_hospital_B" in kiem_tra["van_de"][0] and "20-00-00" in kiem_tra["van_de"][0]

    # Bệnh viện khôi phục file -> phiên bản 0 của bảng AI-ready lại kiểm tra khớp hoàn toàn
    inject_errors.sua_lai("B", mt["raw"], mt["backup"])
    assert pv.verify_version(cu["id"], mt["db"]) == {"ok": True, "da_kiem": 30, "van_de": []}


def test_pipeline_doc_ghi_qua_kiem_soat_quyen(mt, spark):
    _chay(mt, spark)
    audit = get_audit_logs(ai="pipeline_service", limit=10_000, db_path=mt["db"])
    assert {"read:raw", "write:standardized", "read:standardized", "write:ai_ready"} <= {a["lam_gi"] for a in audit}
    assert {a["ket_qua"] for a in audit if a["lam_gi"].startswith(("read:", "write:"))} == {"ALLOWED"}


def test_tai_khoan_khong_co_quyen_thi_pipeline_dung(mt, spark, monkeypatch):
    monkeypatch.setattr(standardize, "ACTOR", "researcher_bob")
    with pytest.raises(AccessDenied):
        standardize.main(mt["db"], mt["raw"], mt["std"], spark)
    assert pv.get_runs(db_path=mt["db"])[0]["status"] == "FAILED"
    assert get_audit_logs(ket_qua="DENIED", db_path=mt["db"])[0]["ai"] == "researcher_bob"
    assert not os.path.exists(os.path.join(mt["std"], "standardized_patients"))


def test_sau_khi_chay_nguoi_dung_doc_theo_quyen(mt, spark):
    _chay(mt, spark)
    assert len(read_table("researcher_bob", AI, db_path=mt["db"])) == 95
    assert "patient_id" not in read_table("analyst_dave", "standardized_patients", db_path=mt["db"]).columns
    with pytest.raises(AccessDenied):
        read_table("researcher_bob", "standardized_patients", db_path=mt["db"])


# ---------------------------------------------------------------------------
# Lỗi giả lập (inject_errors) trên dữ liệu giả
# ---------------------------------------------------------------------------

def test_doi_ten_cot_vien_B_bi_chan_dung_lo(mt, spark):
    _chay(mt, spark)
    so_bn_dot_3 = len(_dot_3(mt, "B"))
    assert inject_errors.gay_loi("doi_ten", mt["raw"], mt["backup"]) == "B"
    kq, ai_df = _chay(mt, spark)

    assert len(kq["bi_chan"]) == 1 and kq["bi_chan"][0].startswith("raw_patient_hospital_B / ")
    assert len(ai_df) == 95 - so_bn_dot_3
    run_id = kq["run_id"]
    v = get_violations(mt["db"], severity="BREAKING", run_id=run_id)
    assert [(x["error_type"], x["affected_ai_table"]) for x in v] == [("SCHEMA_COLUMN_RENAMED", AI)]
    assert v[0]["batch"].startswith("hospital_B/") and v[0]["batch"].endswith("20-00-00")
    lo_b = {(b["table_name"], b["status"]) for b in pv.get_batches(run_id, mt["db"])
            if b["batch"] == v[0]["batch"]}
    assert lo_b == {("raw_patient_hospital_B", "BLOCKED"), ("raw_vital_hospital_B", "SKIPPED"),
                    ("raw_lab_hospital_B", "SKIPPED")}
    # Lần chạy chuẩn hóa có lô bị chặn nhưng vẫn ra được bảng -> PARTIAL
    assert _trang_thai(mt, run_id) == "PARTIAL"

    inject_errors.sua_lai("B", mt["raw"], mt["backup"])
    kq, ai_df = _chay(mt, spark)
    assert kq["bi_chan"] == [] and len(ai_df) == 95


def test_nhip_tim_500_chan_lo_vital_giu_benh_nhan(mt, spark):
    _chay(mt, spark)
    inject_errors.gay_loi("nhip_tim_500", mt["raw"], mt["backup"])
    kq, ai_df = _chay(mt, spark)
    assert len(kq["bi_chan"]) == 1 and kq["bi_chan"][0].startswith("raw_vital_hospital_A / ")
    assert len(ai_df) == 95 and ai_df["heartrate_mean"].max() <= 300
    v = get_violations(mt["db"], severity="BREAKING", run_id=kq["run_id"])
    assert [x["error_type"] for x in v] == ["PLAUSIBILITY"]


@pytest.mark.parametrize("loi, bang, loai_vi_pham", [
    ("doi_kieu", "raw_patient_hospital_A", "SCHEMA_TYPE_CHANGED"),
    ("xoa_cot", "raw_patient_hospital_C", "SCHEMA_COLUMN_REMOVED"),
    ("thieu_can_nang", "raw_patient_hospital_B", "COMPLETENESS"),
])
def test_cac_loi_con_lai_deu_bi_chan(mt, spark, loi, bang, loai_vi_pham):
    _chay(mt, spark)
    vien = inject_errors.gay_loi(loi, mt["raw"], mt["backup"])
    so_bn_dot_3 = len(_dot_3(mt, vien))
    kq, ai_df = _chay(mt, spark)
    v = get_violations(mt["db"], severity="BREAKING", run_id=kq["run_id"])
    assert [(x["error_type"], x["affected_table"]) for x in v] == [(loai_vi_pham, bang)]
    assert len(ai_df) == 95 - so_bn_dot_3


def test_bang_standardized_ghi_ngoai_pipeline_bi_chan(mt, spark):
    from pyspark.sql import functions as F

    _chay(mt, spark)
    duong_dan = os.path.join(mt["std"], "standardized_patients")
    goc = spark.read.format("delta").load(duong_dan)
    (goc.withColumn("weight_kg", F.when(F.col("patient_id") == 140000, F.lit(999.0)).otherwise(F.col("weight_kg")))
     .write.format("delta").mode("overwrite").save(duong_dan))
    assert build_ai_ready.main(mt["db"], mt["std"], mt["ai"], spark) is None
    v = get_violations(mt["db"], severity="BREAKING")[0]
    assert (v["error_type"], v["affected_table"]) == ("PROVENANCE_UNVERIFIED", "standardized_patients")
    assert pv.get_runs(db_path=mt["db"])[0]["status"] == "BLOCKED"


# ---------------------------------------------------------------------------
# Đọc CSV nhiều file và các phép chuẩn hóa (Spark)
# ---------------------------------------------------------------------------

def _file_gui(thu_muc, vien, dot, df):
    duong_dan = thu_muc / f"hospital_{vien}" / dot / "patient.csv"
    duong_dan.parent.mkdir(parents=True)
    df.to_csv(duong_dan, index=False)
    return standardize.FileGui(vien, dot, "patient", str(duong_dan), tuple(df.columns), "")


def test_doc_nhieu_file_khong_lech_cot_khi_mot_vien_xoa_cot(tmp_path, spark):
    """Spark khớp cột theo vị trí khi đọc lẫn nhiều file CSV; doc_csv_spark gom theo tiêu đề nên không lệch."""
    du = pd.DataFrame({"patientunitstayid": [1], "gender": ["Male"], "age": ["45"]})
    thieu = pd.DataFrame({"patientunitstayid": [2], "age": ["50"]})            # viện 2 xóa cột gender
    cac_file = [_file_gui(tmp_path, "1", "dot_1", du), _file_gui(tmp_path, "2", "dot_1", thieu)]

    dong = {r["patientunitstayid"]: r for r in standardize.doc_csv_spark(spark, cac_file).collect()}
    assert (dong["1"]["gender"], dong["1"]["age"]) == ("Male", "45")
    assert (dong["2"]["gender"], dong["2"]["age"]) == (None, "50")

    # Đọc ngây thơ một lần cả hai file: Spark lấy tiêu đề của một file rồi khớp theo vị trí -> lệch cột
    ngay_tho = spark.read.option("header", True).csv([f.duong_dan for f in cac_file]).collect()
    assert {(r["patientunitstayid"], r.asDict().get("age")) for r in ngay_tho} != {("1", "45"), ("2", "50")}


def test_chuan_hoa_benh_nhan(tmp_path, spark):
    raw = pd.DataFrame({"patientunitstayid": [1, 2, 3, 4], "age": ["> 89", "45", None, "60"],
                        "gender": ["Male", "Female", "Other", None],
                        "admissionweight": [70.5, None, 0.5, 80.0],
                        "unitdischargestatus": ["Alive", "Expired", "Alive", None]})
    f = _file_gui(tmp_path, "146", "2026-10-29_08-00-00", raw)
    from governance.contracts import find_contract
    df = standardize.chuan_hoa_benh_nhan(standardize.doc_csv_spark(spark, [f]), find_contract("raw_patient_hospital_x"))
    dong = {r["patient_id"]: r.asDict() for r in df.collect()}
    assert [dong[i]["age"] for i in (1, 2, 3)] == [90, 45, None]
    assert [dong[i]["gender"] for i in (1, 2, 3, 4)] == ["M", "F", "U", "U"]
    assert (dong[1]["weight_kg"], dong[3]["weight_kg"]) == (70.5, None)        # 0.5 kg ngoài [20, 400] -> trống
    assert {(d["source_hospital"], d["source_batch"]) for d in dong.values()} == {
        ("146", "hospital_146/2026-10-29_08-00-00")}


def test_dac_trung_xet_nghiem(spark):
    lab = spark.createDataFrame(
        [(1, "creatinine", 1.2), (1, "creatinine", 2.5), (1, "glucose", 100.0), (2, "glucose", 150.0),
         (3, "sodium", 140.0)], "patient_id long, lab_name string, lab_value double")
    dt = {r["patient_id"]: r for r in build_ai_ready.calc_lab_features(lab).collect()}
    assert (dt[1]["creatinine_max"], dt[1]["glucose_mean"]) == (2.5, 100.0)
    assert (dt[2]["creatinine_max"], dt[2]["glucose_mean"]) == (None, 150.0)
    assert 3 not in dt                                         # chỉ có sodium -> không có đặc trưng


def test_build_ai_ready_khong_co_lab_va_khong_lo_patient_id(spark):
    from governance.privacy import pseudonymize_id
    std = spark.createDataFrame(
        [(1, 40, "M", 70.0, 80.0, "Alive"), (2, 90, "F", None, None, "Expired"), (3, None, "U", None, None, None)],
        "patient_id long, age int, gender string, weight_kg double, heartrate_mean double, unitdischargestatus string")
    khoa = b"khoa-kiem-thu-0123456789"
    ai = build_ai_ready.build_ai_ready(std, None, secret=khoa)
    assert ai.columns == build_ai_ready.COT_AI
    dong = ai.collect()
    assert [r["icu_mortality"] for r in dong] == [0, 1, 0]
    assert all(r["creatinine_max"] is None for r in dong)
    assert [r["patient_hash"] for r in dong] == [pseudonymize_id(i, khoa) for i in (1, 2, 3)]


# ---------------------------------------------------------------------------
# Lỗi do PIPELINE đổi code: dữ liệu vẫn đúng hợp đồng nhưng sai nghĩa
# ---------------------------------------------------------------------------

def _ai_tren_dia(mt):
    from deltalake import DeltaTable
    return DeltaTable(os.path.join(mt["ai"], AI)).to_pandas().sort_values("patient_hash").reset_index(drop=True)


@pytest.mark.parametrize("loi, bang, cot", [("can_nang_pound", "standardized_patients", "weight_kg"),
                                            ("dao_nhan_tu_vong", AI, "icu_mortality")])
def test_loi_pipeline_qua_hop_dong_nhung_bi_chan(mt, spark, loi, bang, cot):
    _chay(mt, spark)
    sach = _ai_tren_dia(mt)
    moc = max([v["id"] for v in get_violations(mt["db"])], default=0)
    with inject_errors.gay_loi_pipeline(loi):
        _chay(mt, spark)
    breaking = get_violations(mt["db"], severity="BREAKING", since_id=moc)
    # Hợp đồng không bắt được (giá trị vẫn đúng luật) - chỉ bộ phát hiện lệch đầu ra chặn
    assert {(v["error_type"], v["affected_table"]) for v in breaking} == {("PIPELINE_OUTPUT_DRIFT", bang)}
    assert any(f"[Cột: {cot}]" in v["details"] and f"+loi.{loi}" in v["details"] for v in breaking)
    assert AI in breaking[0]["affected_ai_table"].split(",")
    pd.testing.assert_frame_equal(_ai_tren_dia(mt), sach)          # tầng AI-ready không bị nhiễm bẩn


def test_tat_loi_pipeline_thi_chay_lai_binh_thuong_va_dong_vi_pham(mt, spark):
    _chay(mt, spark)
    with inject_errors.gay_loi_pipeline("can_nang_pound"):
        kq_loi, _ = _chay(mt, spark)
    kq, ai_df = _chay(mt, spark)
    assert "standardized_patients" in kq["phien_ban"] and len(ai_df) == 95
    v = next(x for x in get_violations(mt["db"]) if x["error_type"] == "PIPELINE_OUTPUT_DRIFT")
    assert (v["run_id"], v["status"]) == (kq_loi["run_id"], "RESOLVED")


# ---------------------------------------------------------------------------
# Duyệt thay đổi schema và nguồn không đồng nhất
# ---------------------------------------------------------------------------

def test_duyet_doi_ten_cot_thi_lo_qua_cong_va_dong_vi_pham(mt, spark):
    from governance.lineage import get_impact_paths
    from governance.schema_registry import approve_rename

    _chay(mt, spark)
    sach = _ai_tren_dia(mt)
    vien = inject_errors.gay_loi("doi_ten", mt["raw"], mt["backup"])
    bang = f"raw_patient_hospital_{vien}"
    kq_loi, _ = _chay(mt, spark)
    assert kq_loi["bi_chan"] == [f"{bang} / {inject_errors.get_batch_3_dir(vien, mt['raw']).split('/')[-1]}"]

    ma = approve_rename("engineer_alice", bang, "weight", "admissionweight", "viện đổi tên cột cân nặng", mt["db"])
    kq, ai_df = _chay(mt, spark)
    assert kq["bi_chan"] == [] and len(ai_df) == 95
    pd.testing.assert_frame_equal(_ai_tren_dia(mt), sach)          # cân nặng đọc từ cột 'weight' như cũ
    v = next(x for x in get_violations(mt["db"]) if x["error_type"] == "SCHEMA_COLUMN_RENAMED")
    assert (v["run_id"], v["status"]) == (kq_loi["run_id"], "RESOLVED")
    lo = [b for b in pv.get_batches(kq["run_id"], mt["db"]) if b["mapping"]]
    assert [(b["table_name"], b["status"], b["mapping"]) for b in lo] == [
        (bang, "ACCEPTED", f"weight -> admissionweight (duyệt #{ma})")]
    assert any("standardized_patients.weight_kg" in d for d in get_impact_paths(bang, "weight", mt["db"]))


def test_nguon_khong_dong_nhat_len_toi_ai_ready(mt, spark, tmp_path):
    from governance.sample_data import make_eav_source
    from governance.sources import source_of_hospital
    from pipeline import ingest_raw, source_adapters

    landing = tmp_path / "landing_c2012"
    landing.mkdir()
    ky_vong = make_eav_source(landing)
    source_adapters.chuyen_eav_long(source_of_hospital("c2012"), str(landing), str(tmp_path / "staging"))
    ingest_raw.main(mt["db"], str(tmp_path / "staging"), mt["raw"])
    kq, ai_df = _chay(mt, spark)

    assert kq["bi_chan"] == [] and len(ai_df) == 95 + len(ky_vong)
    ds = get_dataset("raw_patient_hospital_c2012", mt["db"])
    assert (ds["source_system"], ds["source_format"]) == ("challenge2012", "eav_long")
    assert get_dataset("raw_patient_hospital_A", mt["db"])["source_system"] == "eicu_demo"
    canh_bao = [v for v in get_violations(mt["db"], run_id=kq["run_id"]) if v["error_type"] == "SEMANTIC_MISMATCH"]
    assert [v["affected_table"] for v in canh_bao] == ["raw_patient_hospital_c2012"]
    assert 77.5 in set(ai_df["weight_kg"]) and int(ai_df["icu_mortality"].sum()) >= len(ky_vong) // 2
