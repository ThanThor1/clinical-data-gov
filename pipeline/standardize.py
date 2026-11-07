"""
Chuẩn hóa: tầng RAW -> tầng STANDARDIZED, chạy bằng Apache Spark.

Dữ liệu thật (eICU demo): 186 bệnh viện x 3 đợt x 3 loại = 1.674 file CSV gửi lên, khoảng 2 triệu dòng.

  1. Tìm mọi file gửi lên, đọc dòng tiêu đề, gắn đổi tên cột đã được kỹ sư dữ liệu duyệt (schema_approvals);
     ghi lineage (cả tham số gốc của nguồn khác chuẩn, sources/*.yaml) và cảnh báo nguồn khác định nghĩa;
     kiểm tra quyền đọc từng bảng raw bằng tài khoản pipeline_service (ghi audit; không có quyền thì dừng).
  2. Lập hồ sơ chất lượng cho TẤT CẢ các lô bằng một job Spark mỗi loại dữ liệu (groupBy theo file:
     số ô trống, sai kiểu, ngoài khoảng, ... của từng cột) - xem governance/profiling.py.
     Xử lý tăng dần: hồ sơ được lưu theo checksum file, lần chạy sau chỉ tính cho file mới / bị sửa.
     Lưu ý: Spark đọc nhiều file CSV một lúc thì khớp cột theo VỊ TRÍ, không theo tên. Vì vậy file được
     gom theo dòng tiêu đề và đọc từng nhóm: file bị đổi tên / xóa / thêm cột nằm riêng một nhóm,
     không làm lệch cột của các viện khác.
  3. Cổng gate1 + gate2 chạy trên hồ sơ của từng lô, theo thứ tự đợt gửi của từng viện; ghi provenance
     của lô (file, SHA-256, số dòng, ACCEPTED / BLOCKED / SKIPPED).
  4. Spark đọc lại CHỈ các lô đã qua cổng và chuẩn hóa: đổi tên cột, '> 89' -> 90, Male/Female -> M/F/U,
     giá trị ngoài khoảng hợp lý của hợp đồng -> để trống (đã cảnh báo ở gate2), nhịp tim trung bình
     mỗi bệnh nhân.
  5. Cổng kiểm tra tầng standardized (gate1, gate2, rồi lệch đầu ra khi code pipeline đổi - xem
     governance/pipeline_drift.py), ghi 2 bảng Delta Lake qua write_table_spark, ghi phiên bản
     (dataset_versions, kèm số phiên bản Delta, phiên bản code, hồ sơ đầu ra) cùng danh sách lô đầu vào.

Chạy: python -m pipeline.standardize   (từ gốc repo)
"""

import csv
import dataclasses
import math
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog_access import register_dataset, require_access
from catalog_access.data_access import to_catalog_path, write_table_spark
from governance.contracts import catalog_metadata, contract_label, find_contract
from governance.gates import check_gate
from governance.lineage import record_rename_lineage, record_sample_lineage
from governance.pipeline_drift import check_pipeline_drift
from governance.profiling import empty_profile, load_cached_profiles, profile_spark, save_profiles
from governance.provenance import batch_context, file_checksum, pipeline_run, record_batch, record_dataset_version
from governance.schema_registry import get_latest_schema, get_schema_approvals
from governance.sources import check_source_semantics, record_source_lineage
from pipeline.ingest_raw import FILE_THEO_LOAI
from pipeline.inject_errors import loi_pipeline
from pipeline.spark_session import get_spark
from pipeline.split_snapshots import danh_sach_vien

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
STD_DIR = os.path.join(DATA_DIR, "standardized")

# Tài khoản dịch vụ của pipeline (vai trò data_engineer trong catalog_access/policies.yaml)
ACTOR = "pipeline_service"

# Đường dẫn file gửi lên -> mã viện (nhóm 1) và đợt gửi (nhóm 2)
MAU_FILE = r"hospital_([^/]+)/([^/]+)/[^/]+$"

# Mỗi file Parquet của bảng standardized chứa khoảng chừng này dòng (tránh nhiều file nhỏ)
SO_DONG_MOI_FILE = 1_000_000


@dataclass(frozen=True)
class FileGui:
    """Một file bệnh viện gửi lên trong một đợt."""
    vien: str
    dot: str
    loai: str
    duong_dan: str
    tieu_de: tuple
    checksum: str
    # Đổi tên cột đã duyệt áp cho file này: ((tên bệnh viện gửi, tên theo hợp đồng, id lần duyệt), ...)
    doi_ten: tuple = ()

    @property
    def bang(self):
        return f"raw_{self.loai}_hospital_{self.vien}"

    @property
    def nhan_lo(self):
        return f"hospital_{self.vien}/{self.dot}"

    @property
    def mo_ta_doi_ten(self):
        return ", ".join(f"{cu} -> {moi} (duyệt #{ma})" for cu, moi, ma in self.doi_ten)


def ap_doi_ten_da_duyet(cac_file, db_path=None):
    """
    Gắn các đổi tên cột đã duyệt (governance/schema_registry.approve_rename) vào file của đúng bảng, khi file
    còn dùng tên mới và chưa có cột theo tên hợp đồng.
    """
    theo_bang = defaultdict(list)
    for a in get_schema_approvals(db_path=db_path):
        theo_bang[a["table_name"]].append(a)
    ket_qua = []
    for f in cac_file:
        doi_ten = tuple((a["source_column"], a["target_column"], a["id"]) for a in theo_bang.get(f.bang, [])
                        if a["source_column"] in f.tieu_de and a["target_column"] not in f.tieu_de)
        ket_qua.append(dataclasses.replace(f, doi_ten=doi_ten) if doi_ten else f)
    return ket_qua


def _doc_tieu_de(duong_dan):
    with open(duong_dan, newline="", encoding="utf-8") as f:
        return tuple(c.strip() for c in next(csv.reader(f), []))


def tim_file_gui(raw_dir=RAW_DIR):
    """Mọi file gửi lên trong tầng raw: data/raw/hospital_<mã>/<đợt>/<loại>.csv"""
    # Đường dẫn tuyệt đối, đã giải symlink một lần cho cả thư mục: Spark trả về đúng chuỗi này trong
    # _metadata.file_path nên so khớp được mà không phải xử lý từng file
    raw_dir = os.path.realpath(raw_dir)
    cac_file = []
    for vien in danh_sach_vien(raw_dir):
        thu_muc_vien = os.path.join(raw_dir, f"hospital_{vien}")
        for dot in sorted(os.listdir(thu_muc_vien)):
            for loai, ten_file in FILE_THEO_LOAI.items():
                duong_dan = os.path.join(thu_muc_vien, dot, ten_file)
                if os.path.isfile(duong_dan):
                    cac_file.append(FileGui(vien, dot, loai, duong_dan, _doc_tieu_de(duong_dan),
                                            file_checksum(duong_dan)))
    return cac_file


def doc_csv_spark(spark, cac_file):
    """
    Đọc nhiều file CSV cùng loại thành một DataFrame Spark toàn cột chuỗi, thêm cột _file.
    File được gom theo dòng tiêu đề (và đổi tên đã duyệt), mỗi nhóm đọc với đúng tên cột của nó, đổi tên
    các cột đã duyệt về tên hợp đồng, rồi nối các nhóm theo TÊN cột.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    theo_tieu_de = defaultdict(list)
    for f in cac_file:
        if f.tieu_de:
            theo_tieu_de[(f.tieu_de, f.doi_ten)].append(f.duong_dan)
    ket_qua = None
    for (tieu_de, doi_ten), cac_duong_dan in theo_tieu_de.items():
        schema = T.StructType([T.StructField(c, T.StringType()) for c in tieu_de])
        phan = (spark.read.option("header", True).schema(schema).csv(cac_duong_dan)
                .withColumnsRenamed({cu: moi for cu, moi, _ in doi_ten})
                .withColumn("_file", F.col("_metadata.file_path")))
        ket_qua = phan if ket_qua is None else ket_qua.unionByName(phan, allowMissingColumns=True)
    return ket_qua


def lap_ho_so(spark, cac_file, hop_dong, db_path=None):
    """
    Hồ sơ chất lượng của từng file (khóa: đường dẫn). File đã có hồ sơ (cùng checksum, cùng hợp đồng)
    thì dùng lại; các file còn lại gom theo dòng tiêu đề, mỗi nhóm là một job Spark.
    File có đổi tên đã duyệt luôn tính lại và không lưu (hồ sơ phụ thuộc cả việc đổi tên, không chỉ checksum).
    Trả về (hồ sơ theo đường dẫn, số file phải tính mới bằng Spark).
    """
    da_luu = load_cached_profiles({f.checksum for f in cac_file if not f.doi_ten}, hop_dong, db_path)
    ket_qua = {f.duong_dan: da_luu[f.checksum] for f in cac_file if not f.doi_ten and f.checksum in da_luu}
    can_tinh = [f for f in cac_file if f.doi_ten or f.checksum not in da_luu]

    theo_tieu_de = defaultdict(list)
    for f in can_tinh:
        theo_tieu_de[(f.tieu_de, f.doi_ten)].append(f)
    moi_tinh = {}
    for (tieu_de, doi_ten), nhom in theo_tieu_de.items():
        theo_duong_dan = {}
        if tieu_de:
            theo_uri = profile_spark(doc_csv_spark(spark, nhom), hop_dong, nhom="_file", suy_kieu_tu_chuoi=True)
            theo_duong_dan = {os.path.normpath(unquote(urlparse(u).path)): h for u, h in theo_uri.items()}
        doi = {cu: moi for cu, moi, _ in doi_ten}
        for f in nhom:
            # File chỉ có dòng tiêu đề (0 dòng dữ liệu) không xuất hiện trong kết quả groupBy
            ket_qua[f.duong_dan] = (theo_duong_dan.get(f.duong_dan)
                                    or empty_profile([doi.get(c, c) for c in tieu_de], hop_dong))
            if not doi_ten:
                moi_tinh[f.checksum] = ket_qua[f.duong_dan]
    save_profiles(moi_tinh, hop_dong, db_path)
    return ket_qua, len(can_tinh)


def kiem_tra_lo(f, ho_so, db_path=None):
    """gate1 rồi gate2 trên hồ sơ của một lô, ghi provenance của lô. Trả về (đạt?, id lô)."""
    dat = True
    for gate in ("gate1", "gate2"):
        dat, _ = check_gate(f.bang, ho_so, gate, db_path)
        if not dat:
            break
    ma_lo = record_batch(f.bang, f"Bệnh viện {f.vien}", f.nhan_lo, f.duong_dan, ho_so["so_dong"],
                         "ACCEPTED" if dat else "BLOCKED", schema_version=get_latest_schema(f.bang, db_path)[0],
                         contract=contract_label(find_contract(f.bang)), db_path=db_path, checksum=f.checksum,
                         mapping=f.mo_ta_doi_ten)
    return dat, ma_lo


def ghi_lo_bo_qua(f, db_path=None):
    """Bảng patient của đợt bị chặn -> lab / vital cùng đợt không được xử lý, vẫn ghi lại là SKIPPED."""
    with open(f.duong_dan, "rb") as tep:
        so_dong = max(sum(1 for _ in tep) - 1, 0)
    return record_batch(f.bang, f"Bệnh viện {f.vien}", f.nhan_lo, f.duong_dan, so_dong, "SKIPPED",
                        contract=contract_label(find_contract(f.bang)), db_path=db_path, checksum=f.checksum)


# ---------------------------------------------------------------------------
# Các phép chuẩn hóa (Spark)
# ---------------------------------------------------------------------------

def _lam_sach(cot_so, luat):
    """Giá trị ngoài khoảng [min, max] của hợp đồng -> để trống (gate2 đã đếm và cảnh báo)."""
    from pyspark.sql import functions as F

    ngoai = F.lit(False)
    if luat["min"] is not None:
        ngoai = ngoai | (cot_so < float(luat["min"]))
    if luat["max"] is not None:
        ngoai = ngoai | (cot_so > float(luat["max"]))
    return F.when(ngoai, F.lit(None).cast("double")).otherwise(cot_so)


def _nguon(df):
    """Cột source_hospital, source_batch lấy từ đường dẫn file gửi lên."""
    from pyspark.sql import functions as F

    vien = F.regexp_extract("_file", MAU_FILE, 1)
    return [vien.alias("source_hospital"),
            F.concat(F.lit("hospital_"), vien, F.lit("/"), F.regexp_extract("_file", MAU_FILE, 2)).alias("source_batch")]


def chuan_hoa_benh_nhan(p, hop_dong_raw):
    from pyspark.sql import functions as F

    tuoi = F.trim(p["age"])
    gioi = F.trim(p["gender"])
    can_nang = _lam_sach(p["admissionweight"].try_cast("double"), hop_dong_raw["columns"]["admissionweight"])
    if loi_pipeline() == "can_nang_pound":           # lỗi pipeline giả lập (inject_errors.LOI_PIPELINE)
        can_nang = can_nang * 2.20462
    return p.select(
        p["patientunitstayid"].try_cast("long").alias("patient_id"),
        # age: "> 89" -> 90, ép số, giữ trống
        F.when(tuoi == "> 89", F.lit(90)).otherwise(tuoi.try_cast("int")).alias("age"),
        F.when(gioi == "Male", "M").when(gioi == "Female", "F").otherwise("U").alias("gender"),
        can_nang.alias("weight_kg"),
        F.trim(p["unitdischargestatus"]).alias("unitdischargestatus"),
        *_nguon(p),
    )


def nhip_tim_trung_binh(v, hop_dong_raw):
    from pyspark.sql import functions as F

    nhip_tim = _lam_sach(v["heartrate"].try_cast("double"), hop_dong_raw["columns"]["heartrate"])
    return (v.select(v["patientunitstayid"].try_cast("long").alias("patient_id"), nhip_tim.alias("heartrate"))
            .groupBy("patient_id").agg(F.avg("heartrate").alias("heartrate_mean")))


def chuan_hoa_xet_nghiem(lab):
    return lab.select(
        lab["patientunitstayid"].try_cast("long").alias("patient_id"),
        lab["labname"].alias("lab_name"),
        lab["labresult"].try_cast("double").alias("lab_value"),
        *_nguon(lab),
    )


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


def check_both_gates(table_name, ho_so, db_path=None, inputs=None):
    """
    Chạy gate1 rồi gate2 cho bảng tầng standardized; có `inputs` (đầu vào của phiên bản sắp ghi) thì kiểm tra
    thêm đầu ra có lệch khi code pipeline đổi không (governance/pipeline_drift.py). Trả về True nếu qua hết.
    """
    for gate in ("gate1", "gate2"):
        dat, loi = check_gate(table_name, ho_so, gate, db_path)
        if not dat:
            print(f"[CHẶN] {table_name} ở {gate}:")
            print("\n".join(loi))
            return False
    if inputs is not None:
        dat, lech = check_pipeline_drift(table_name, ho_so, inputs, db_path)
        if not dat:
            print(f"[CHẶN] {table_name}: đầu ra lệch khi code pipeline đổi:")
            print("\n".join(lech))
            return False
    return True


def _ghi_bang_standardized(spark, table_name, sdf, ho_so, dau_vao, std_dir, db_path):
    """Ghi bảng Delta (qua write_table_spark), ghi phiên bản provenance, đăng ký catalog. Trả về id phiên bản."""
    duong_dan = os.path.join(std_dir, table_name)
    so_dong = ho_so["so_dong"]
    write_table_spark(spark, ACTOR, table_name, sdf, path=duong_dan, db_path=db_path,
                      so_file=max(1, math.ceil(so_dong / SO_DONG_MOI_FILE)))
    ma_phien_ban = record_dataset_version(table_name, "standardized", duong_dan, so_dong, dau_vao,
                                          db_path=db_path, ho_so=ho_so)
    register_standardized(table_name, duong_dan, so_dong, db_path)
    return ma_phien_ban


def _chuan_hoa(spark, db_path, raw_dir, std_dir):
    from pyspark.sql import functions as F

    ket_qua = {"so_dot": 0, "so_dot_dat": 0, "bi_chan": [], "so_dong": None, "phien_ban": {},
               "trang_thai_lo": {}, "lam_sach": {}, "ho_so_moi_tinh": 0}
    cac_file = ap_doi_ten_da_duyet(tim_file_gui(raw_dir), db_path)
    if not cac_file:
        print("Không có file nào trong tầng raw!")
        return ket_qua
    cac_vien = danh_sach_vien(raw_dir)
    print(f"Tầng raw: {len(cac_vien)} bệnh viện, {len(cac_file)} file gửi lên"
          + "".join(f"; {f.nhan_lo} {f.loai}: {f.mo_ta_doi_ten}" for f in cac_file if f.doi_ten))

    # Ghi lineage đầy đủ (tham số gốc của nguồn khác chuẩn) -> raw (mọi viện) -> standardized -> ai_ready
    # TRƯỚC mọi check_gate, để khi có vi phạm, violation_engine tìm được bảng AI-ready bị ảnh hưởng.
    record_sample_lineage(db_path, cac_vien)
    record_source_lineage(cac_vien, db_path)
    for a in get_schema_approvals(db_path=db_path):
        record_rename_lineage(a["table_name"], a["source_column"], a["target_column"], f"đổi tên đã duyệt #{a['id']}",
                              db_path)
    # Nguồn định nghĩa cột khác hợp đồng (ví dụ nhãn tử vong) -> cảnh báo, không chặn
    check_source_semantics(cac_vien, db_path)

    # Quyền đọc: kiểm tra (và ghi audit) một lần cho mỗi bảng raw
    for bang in sorted({f.bang for f in cac_file}):
        require_access(ACTOR, bang, "read", db_path=db_path)

    # 1 job Spark cho mỗi loại dữ liệu: hồ sơ chất lượng của mọi lô cùng lúc
    hop_dong = {loai: find_contract(f"raw_{loai}_hospital_x") for loai in FILE_THEO_LOAI}
    ho_so = {}
    for loai in FILE_THEO_LOAI:
        theo_loai, so_moi = lap_ho_so(spark, [f for f in cac_file if f.loai == loai], hop_dong[loai], db_path)
        ho_so.update(theo_loai)
        ket_qua["ho_so_moi_tinh"] += so_moi
    print(f"Hồ sơ chất lượng: {ket_qua['ho_so_moi_tinh']} file tính mới bằng Spark, "
          f"{len(cac_file) - ket_qua['ho_so_moi_tinh']} file không đổi dùng lại hồ sơ cũ")

    # Cổng kiểm tra từng lô, theo thứ tự đợt gửi của từng viện (schema registry so với đợt trước)
    theo_lo = defaultdict(dict)
    for f in cac_file:
        theo_lo[(f.vien, f.dot)][f.loai] = f
    chap_nhan = {loai: [] for loai in FILE_THEO_LOAI}
    ma_lo = {loai: [] for loai in FILE_THEO_LOAI}
    trang_thai = Counter()
    for vien in cac_vien:
        for dot in sorted(d for v, d in theo_lo if v == vien):
            lo = theo_lo[(vien, dot)]
            ket_qua["so_dot"] += 1
            with batch_context(f"hospital_{vien}/{dot}"):
                f_p = lo.get("patient")
                dat_p = False
                if f_p is not None:
                    dat_p, ma = kiem_tra_lo(f_p, ho_so[f_p.duong_dan], db_path)
                    trang_thai[("patient", "ACCEPTED" if dat_p else "BLOCKED")] += 1
                if not dat_p:
                    ket_qua["bi_chan"].append(f"raw_patient_hospital_{vien} / {dot}")
                    for loai in ("vital", "lab"):
                        if loai in lo:
                            ghi_lo_bo_qua(lo[loai], db_path)
                            trang_thai[(loai, "SKIPPED")] += 1
                    continue
                chap_nhan["patient"].append(f_p)
                ma_lo["patient"].append(ma)

                dot_dat = True
                for loai in ("vital", "lab"):
                    f = lo.get(loai)
                    if f is None:
                        dot_dat = False
                        ket_qua["bi_chan"].append(f"raw_{loai}_hospital_{vien} / {dot} (thiếu file)")
                        continue
                    dat, ma = kiem_tra_lo(f, ho_so[f.duong_dan], db_path)
                    trang_thai[(loai, "ACCEPTED" if dat else "BLOCKED")] += 1
                    if dat:
                        chap_nhan[loai].append(f)
                        ma_lo[loai].append(ma)
                    else:
                        dot_dat = False
                        ket_qua["bi_chan"].append(f"{f.bang} / {dot}")
            ket_qua["so_dot_dat"] += int(dot_dat)

    ket_qua["trang_thai_lo"] = {f"{loai}:{tt}": so for (loai, tt), so in sorted(trang_thai.items())}
    print("Kết quả cổng kiểm tra theo lô:", ", ".join(f"{k} {v}" for k, v in ket_qua["trang_thai_lo"].items()))
    if not chap_nhan["patient"]:
        print("Không có lô patient nào qua cổng!")
        return ket_qua

    # Giá trị ngoài khoảng hợp lý sẽ bị để trống khi chuẩn hóa: số lượng lấy luôn từ hồ sơ của các lô đã nhận
    for loai, cot_raw, cot_std in (("patient", "admissionweight", "weight_kg"), ("vital", "heartrate", "heartrate_mean")):
        ket_qua["lam_sach"][cot_std] = sum(
            ho_so[f.duong_dan]["cot"].get(cot_raw, {}).get("vi_pham", {}).get("plausibility.range", [0])[0]
            for f in chap_nhan[loai])

    # Chuẩn hóa bằng Spark, chỉ trên các lô đã qua cổng
    benh_nhan = chuan_hoa_benh_nhan(doc_csv_spark(spark, chap_nhan["patient"]), hop_dong["patient"])
    if chap_nhan["vital"]:
        benh_nhan = benh_nhan.join(nhip_tim_trung_binh(doc_csv_spark(spark, chap_nhan["vital"]), hop_dong["vital"]),
                                   "patient_id", "left")
    else:
        benh_nhan = benh_nhan.withColumn("heartrate_mean", F.lit(None).cast("double"))
    # Giữ trong bộ nhớ: mỗi bảng được dùng 2 lần (lập hồ sơ cho cổng, rồi ghi Delta)
    benh_nhan = benh_nhan.cache()
    xet_nghiem = chuan_hoa_xet_nghiem(doc_csv_spark(spark, chap_nhan["lab"])).cache() if chap_nhan["lab"] else None
    try:
        # Tầng standardized cũng phải qua cả gate1 (schema) và gate2 (chất lượng)
        dau_vao_p = [("batch", m) for m in ma_lo["patient"] + ma_lo["vital"]]
        dau_vao_l = [("batch", m) for m in ma_lo["lab"]]
        ho_so_p = profile_spark(benh_nhan, find_contract("standardized_patients"))
        if not check_both_gates("standardized_patients", ho_so_p, db_path, dau_vao_p):
            print("STANDARDIZED_PATIENTS BỊ CHẶN, không ghi bảng.")
            return ket_qua
        ho_so_l = profile_spark(xet_nghiem, find_contract("standardized_labs")) if xet_nghiem is not None else None
        if ho_so_l is not None and not check_both_gates("standardized_labs", ho_so_l, db_path, dau_vao_l):
            print("STANDARDIZED_LABS BỊ CHẶN, không ghi bảng.")
            ho_so_l = None

        # Ghi bảng Delta, ghi provenance và đăng ký catalog (chỉ bảng đã qua cổng)
        ket_qua["phien_ban"]["standardized_patients"] = _ghi_bang_standardized(
            spark, "standardized_patients", benh_nhan, ho_so_p, dau_vao_p, std_dir, db_path)
        if ho_so_l is not None:
            ket_qua["phien_ban"]["standardized_labs"] = _ghi_bang_standardized(
                spark, "standardized_labs", xet_nghiem, ho_so_l, dau_vao_l, std_dir, db_path)
    finally:
        benh_nhan.unpersist()
        if xet_nghiem is not None:
            xet_nghiem.unpersist()

    ket_qua["so_dong"] = ho_so_p["so_dong"]
    print(f"Đã lưu standardized (Delta Lake): {ho_so_p['so_dong']} bệnh nhân"
          + (f", {ho_so_l['so_dong']} dòng xét nghiệm" if ho_so_l else "")
          + f"; làm sạch giá trị ngoài khoảng hợp lý: {ket_qua['lam_sach']}")
    return ket_qua


def main(db_path=None, raw_dir=RAW_DIR, std_dir=STD_DIR, spark=None):
    """
    Chuẩn hóa mọi đợt raw đã qua cổng. Trả về dict tóm tắt:
    so_dot, so_dot_dat, bi_chan (danh sách 'bảng / đợt'), so_dong (số dòng standardized_patients
    đã lưu, None nếu bị chặn), phien_ban ({bảng: id phiên bản đã ghi}), trang_thai_lo (đếm lô theo
    loại:kết quả), lam_sach (số ô bị để trống vì ngoài khoảng hợp lý), ho_so_moi_tinh (số file phải
    tính hồ sơ bằng Spark, các file còn lại không đổi so với lần trước), run_id.
    """
    os.makedirs(std_dir, exist_ok=True)
    spark = spark or get_spark()
    with pipeline_run(ACTOR, "chuẩn hóa raw -> standardized", db_path) as run_id:
        ket_qua = _chuan_hoa(spark, db_path, raw_dir, std_dir)
        ket_qua["run_id"] = run_id
    return ket_qua


if __name__ == "__main__":
    main()
