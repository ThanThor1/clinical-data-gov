"""
Chuẩn hóa: tầng RAW -> tầng STANDARDIZED.

Với mỗi bệnh viện, mỗi đợt gửi (thư mục snapshot), mỗi loại dữ liệu (patient | lab | vital):
  1. Đọc file qua catalog_access.read_table bằng tài khoản pipeline_service
     (kiểm tra quyền + ghi audit; tài khoản không có quyền thì dừng ngay).
  2. Cho qua gate1 (schema + hợp đồng) rồi gate2 (chất lượng Kahn).
  3. Ghi provenance của lô: file nguồn, checksum SHA-256, số dòng, ACCEPTED / BLOCKED / SKIPPED.
Sau đó gộp các lô đã qua cổng, kiểm tra cổng ở tầng standardized, ghi file qua write_table
và ghi phiên bản dữ liệu (dataset_versions) kèm danh sách lô đầu vào.

Chạy: python -m pipeline.standardize   (từ gốc repo)
"""

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog_access import read_table, register_dataset, write_table
from catalog_access.data_access import to_catalog_path
from governance.contracts import catalog_metadata, contract_label, find_contract
from governance.gates import check_gate
from governance.lineage import record_sample_lineage
from governance.provenance import batch_context, pipeline_run, record_batch, record_dataset_version
from governance.schema_registry import get_latest_schema
from pipeline.ingest_raw import FILE_THEO_LOAI, HOSPITALS

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
STD_DIR = os.path.join(DATA_DIR, "standardized")

# Tài khoản dịch vụ của pipeline (vai trò data_engineer trong catalog_access/policies.yaml)
ACTOR = "pipeline_service"


def _file_raw(raw_dir, hospital, batch_name, data_type):
    return os.path.join(raw_dir, f"hospital_{hospital}", batch_name, FILE_THEO_LOAI[data_type])


def load_and_check_batch(hospital, batch_name, data_type, raw_dir=RAW_DIR, db_path=None):
    """
    Đọc file raw của 1 đợt, 1 loại dữ liệu (patient | lab | vital), cho qua gate1 rồi gate2
    và ghi provenance của lô. Trả về (df nếu đạt / None nếu bị chặn hoặc không có file, id lô).
    """
    file_path = _file_raw(raw_dir, hospital, batch_name, data_type)
    if not os.path.exists(file_path):
        print(f"[BỎ QUA] Không có file {file_path}")
        return None, None
    table_name = f"raw_{data_type}_hospital_{hospital}"
    df = read_table(ACTOR, table_name, path=file_path, db_path=db_path)

    dat = True
    for gate in ("gate1", "gate2"):
        dat, loi = check_gate(table_name, df, gate, db_path)
        if not dat:
            print(f"[CHẶN] {table_name} ở {gate}:")
            print("\n".join(loi))
            break

    ma_lo = record_batch(
        table_name, f"Bệnh viện {hospital}", f"hospital_{hospital}/{batch_name}", file_path, len(df),
        "ACCEPTED" if dat else "BLOCKED",
        schema_version=get_latest_schema(table_name, db_path)[0],
        contract=contract_label(find_contract(table_name)),
        db_path=db_path,
    )
    return (df if dat else None), ma_lo


def record_skipped_batch(hospital, batch_name, data_type, raw_dir=RAW_DIR, db_path=None):
    """Bảng patient của đợt bị chặn -> lab / vital cùng đợt không được xử lý, vẫn ghi lại là SKIPPED."""
    file_path = _file_raw(raw_dir, hospital, batch_name, data_type)
    if not os.path.exists(file_path):
        return None
    table_name = f"raw_{data_type}_hospital_{hospital}"
    so_dong = len(pd.read_csv(file_path, usecols=[0]))
    return record_batch(table_name, f"Bệnh viện {hospital}", f"hospital_{hospital}/{batch_name}",
                        file_path, so_dong, "SKIPPED", contract=contract_label(find_contract(table_name)),
                        db_path=db_path)


def clean_patient(df, hospital, batch=""):
    df = df.rename(
        columns={
            "patientunitstayid": "patient_id",
            "admissionweight": "weight_kg"
        }
    )

    # age: "> 89" -> 90, ép số, giữ trống
    df["age"] = df["age"].astype(str).str.replace("> 89", "90", regex=False)
    df["age"] = pd.to_numeric(df['age'], errors="coerce")

    # gender
    df['gender'] = df['gender'].map({'Male': 'M', 'Female': 'F'}).fillna('U')
    df['source_hospital'] = hospital
    # Mỗi dòng ghi lại đợt gửi của nó -> tra được file nguồn trong batch_ingestions
    df['source_batch'] = batch
    return df[['patient_id', 'age', 'gender', "weight_kg", "unitdischargestatus",
               "source_hospital", "source_batch"]]


def clean_lab(df, hospital, batch=""):
    df = df.rename(columns={
        "patientunitstayid": "patient_id",
        "labname": "lab_name",
        "labresult": "lab_value",
    })
    df["source_hospital"] = hospital
    df["source_batch"] = batch
    return df[["patient_id", "lab_name", "lab_value", "source_hospital", "source_batch"]]


def calc_heartrate_mean(df_vital):
    if df_vital is None or df_vital.empty:
        return pd.DataFrame(columns=["patient_id", "heartrate_mean"])
    df = df_vital.rename(columns={
        "patientunitstayid": "patient_id"
    })

    hr = df.groupby("patient_id")["heartrate"].mean().reset_index()
    return hr.rename(columns={"heartrate": "heartrate_mean"})


def register_standardized(table_name, file_path, row_count, db_path=None):
    """Đăng ký bảng tầng standardized vào catalog (cột nhạy cảm, mô tả, hợp đồng lấy từ hợp đồng dữ liệu)."""
    register_dataset(
        table_name,
        hospital="Toàn hệ thống",
        tier="standardized",
        owner="Nhóm Kỹ thuật dữ liệu",
        registered_by=ACTOR,
        db_path=db_path,
        file_path=to_catalog_path(file_path),
        row_count=row_count,
        **catalog_metadata(table_name),
    )


def check_both_gates(table_name, df, db_path=None):
    """Chạy gate1 rồi gate2 cho bảng tầng standardized. Trả về True nếu qua cả hai."""
    for gate in ("gate1", "gate2"):
        dat, loi = check_gate(table_name, df, gate, db_path)
        if not dat:
            print(f"[CHẶN] {table_name} ở {gate}:")
            print("\n".join(loi))
            return False
    return True


def _ghi_bang_standardized(table_name, df, lo_dau_vao, std_dir, db_path):
    """Ghi file (qua write_table), ghi phiên bản provenance, đăng ký catalog. Trả về id phiên bản."""
    duong_dan = os.path.join(std_dir, f"{table_name}.csv")
    write_table(ACTOR, table_name, df, path=duong_dan, db_path=db_path)
    ma_phien_ban = record_dataset_version(table_name, "standardized", duong_dan, len(df),
                                          [("batch", m) for m in lo_dau_vao], db_path=db_path)
    register_standardized(table_name, duong_dan, len(df), db_path)
    return ma_phien_ban


def _chuan_hoa(db_path, raw_dir, std_dir):
    # Ghi lineage đầy đủ raw (3 viện) -> standardized -> ai_ready TRƯỚC mọi check_gate,
    # để khi có vi phạm, violation_engine tìm được bảng AI-ready bị ảnh hưởng.
    record_sample_lineage(db_path)

    all_patients, all_labs = [], []
    # id các lô đã qua cổng -> đầu vào của phiên bản standardized (provenance)
    lo_patients, lo_labs = [], []
    ket_qua = {"so_dot": 0, "so_dot_dat": 0, "bi_chan": [], "so_dong": None, "phien_ban": {}}

    for hospital in HOSPITALS:
        hospital_dir = os.path.join(raw_dir, f"hospital_{hospital}")
        if not os.path.isdir(hospital_dir):
            continue
        # Sort theo tên = sort theo thời gian gửi
        batches = sorted(os.listdir(hospital_dir))

        for batch in batches:
            print(f"\n--- Viện {hospital} | Đợt {batch} ---")
            ket_qua["so_dot"] += 1
            dot_dat = True
            nhan_lo = f"hospital_{hospital}/{batch}"

            with batch_context(nhan_lo):
                # Patient
                df_p, ma_p = load_and_check_batch(hospital, batch, "patient", raw_dir, db_path)
                if df_p is None:
                    ket_qua["bi_chan"].append(f"raw_patient_hospital_{hospital} / {batch}")
                    for loai in ("vital", "lab"):
                        record_skipped_batch(hospital, batch, loai, raw_dir, db_path)
                    continue
                df_p = clean_patient(df_p, hospital, nhan_lo)
                lo_patients.append(ma_p)

                # Vital -> heartrate_mean
                df_v, ma_v = load_and_check_batch(hospital, batch, "vital", raw_dir, db_path)
                if df_v is not None:
                    hr = calc_heartrate_mean(df_v)
                    df_p = df_p.merge(hr, on="patient_id", how="left")
                    lo_patients.append(ma_v)
                else:
                    dot_dat = False
                    ket_qua["bi_chan"].append(f"raw_vital_hospital_{hospital} / {batch}")
                if "heartrate_mean" not in df_p.columns:
                    df_p["heartrate_mean"] = float("nan")
                # Đợt không có vital (viện B) -> merge với bảng rỗng cho cột kiểu object -> ép về float
                df_p["heartrate_mean"] = pd.to_numeric(df_p["heartrate_mean"], errors="coerce").astype(float)

                # Lab
                df_l, ma_l = load_and_check_batch(hospital, batch, "lab", raw_dir, db_path)
                if df_l is not None:
                    all_labs.append(clean_lab(df_l, hospital, nhan_lo))
                    lo_labs.append(ma_l)
                else:
                    dot_dat = False
                    ket_qua["bi_chan"].append(f"raw_lab_hospital_{hospital} / {batch}")

            all_patients.append(df_p)
            ket_qua["so_dot_dat"] += int(dot_dat)

    if not all_patients:
        print("Không có dữ liệu nào qua cổng!")
        return ket_qua

    final_p = pd.concat(all_patients, ignore_index=True)
    final_l = pd.concat(all_labs, ignore_index=True) if all_labs else pd.DataFrame()

    # Tầng standardized cũng phải qua cả gate1 (schema) và gate2 (chất lượng)
    if not check_both_gates("standardized_patients", final_p, db_path):
        print("STANDARDIZED_PATIENTS BỊ CHẶN, không ghi file.")
        return ket_qua
    if not final_l.empty and not check_both_gates("standardized_labs", final_l, db_path):
        print("STANDARDIZED_LABS BỊ CHẶN, không ghi file.")
        final_l = pd.DataFrame()

    # Ghi file, ghi provenance và đăng ký catalog (chỉ bảng đã qua cổng)
    ket_qua["phien_ban"]["standardized_patients"] = _ghi_bang_standardized(
        "standardized_patients", final_p, lo_patients, std_dir, db_path)
    if not final_l.empty:
        ket_qua["phien_ban"]["standardized_labs"] = _ghi_bang_standardized(
            "standardized_labs", final_l, lo_labs, std_dir, db_path)
    ket_qua["so_dong"] = len(final_p)
    print(f"\nĐã lưu standardized! ({len(final_p)} bệnh nhân)")
    return ket_qua


def main(db_path=None, raw_dir=RAW_DIR, std_dir=STD_DIR):
    """
    Chuẩn hóa mọi đợt raw đã qua cổng. Trả về dict tóm tắt:
    so_dot, so_dot_dat, bi_chan (danh sách 'bảng / đợt'), so_dong (số dòng standardized_patients
    đã lưu, None nếu bị chặn), phien_ban ({bảng: id phiên bản đã ghi}), run_id.
    """
    os.makedirs(std_dir, exist_ok=True)
    with pipeline_run(ACTOR, "chuẩn hóa raw -> standardized", db_path) as run_id:
        ket_qua = _chuan_hoa(db_path, raw_dir, std_dir)
        ket_qua["run_id"] = run_id
    return ket_qua


if __name__ == "__main__":
    main()
