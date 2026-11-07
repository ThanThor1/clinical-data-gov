"""
Hồ sơ dữ liệu (data profile): những con số mà cổng kiểm tra cần, tính MỘT LẦN cho mỗi bảng / mỗi lô.

Cổng kiểm tra (gates.py) không đọc thẳng dữ liệu mà đọc hồ sơ này, nhờ vậy cùng một bộ luật
dùng được cho hai "động cơ" xử lý:
    profile_pandas(df, hop_dong)                       dữ liệu nhỏ, kiểm thử, demo governance
    profile_spark(sdf, hop_dong, nhom="_file", ...)    pipeline thật: MỘT job Spark tính hồ sơ cho
                                                       hàng trăm lô cùng lúc (groupBy theo file gửi lên)

Hồ sơ của một bảng / một lô:
{
  "so_dong": 120,
  "hop_dong": "raw_patient v1.0",           # hồ sơ tính theo hợp đồng nào
  "cot": {                                   # đúng thứ tự cột của dữ liệu
    "admissionweight": {
      "type": "float",                       # int | float | string | bool | datetime | unknown
      "null_pct": 12.5,                      # % ô trống (schema registry lưu lại)
      "mean": 81.2,                          # chỉ cột số: so phân phối khi đoán đổi tên cột
      "integral": False,                     # mọi giá trị số đều nguyên (hợp đồng kiểu int nhận 40.0)
      "so_rong": 15,                         # ô NaN / null / chuỗi chỉ có khoảng trắng (Completeness)
      "vi_pham": {                           # chỉ cột có trong hợp đồng: luật -> [số dòng lỗi, vài ví dụ]
        "conformance.type": [0, []],
        "plausibility.range": [2, ["0.5", "1200.0"]]
      }
    }
  }
}

Hồ sơ của file gửi lên được lưu theo checksum (bảng batch_profiles): lần chạy sau, file không đổi thì
dùng lại hồ sơ cũ (load_cached_profiles / save_profiles), Spark chỉ tính cho file mới hoặc file bị sửa.

Chạy thử: python -m governance.profiling
"""

import json
import re
from typing import Optional

import pandas as pd

from catalog_access.init_db import get_db_connection, now_str
from governance.contracts import contract_label
from governance.schema_registry import logical_type

SO_VI_DU = 3

# Đổi cách tính hồ sơ thì tăng số này để mọi hồ sơ đã lưu bị tính lại
PROFILER_VERSION = "1"

# Chuỗi là số nguyên (dùng khi đoán kiểu cột từ file CSV đọc thành chuỗi)
MAU_SO_NGUYEN = r"^\s*[-+]?\d+\s*$"


def empty_profile(cac_cot, hop_dong: Optional[dict] = None) -> dict:
    """Hồ sơ của một lô 0 dòng (chỉ có dòng tiêu đề), ví dụ vitalPeriodic của viện không đo sinh hiệu."""
    return {
        "so_dong": 0,
        "hop_dong": contract_label(hop_dong),
        "cot": {str(c): {"type": "unknown", "null_pct": 0.0, "integral": False, "so_rong": 0, "vi_pham": {}}
                for c in cac_cot},
    }


# ---------------------------------------------------------------------------
# 1. pandas
# ---------------------------------------------------------------------------

def _o_rong(cot: pd.Series) -> pd.Series:
    """Ô trống = NaN/None hoặc chuỗi chỉ có khoảng trắng."""
    return cot.isna() | cot.map(lambda x: isinstance(x, str) and x.strip() == "")


def _vi_pham_pandas(cot: pd.Series, luat: dict) -> dict:
    rong = _o_rong(cot)
    co_gia_tri = cot[~rong]
    ket_qua = {}

    if luat["type"] in ("int", "float"):
        so = pd.to_numeric(co_gia_tri, errors="coerce")
        sai_dinh_dang = so.isna()
        if luat["type"] == "int":
            sai_dinh_dang |= so.notna() & (so % 1 != 0)
        ket_qua["conformance.type"] = [int(sai_dinh_dang.sum()), co_gia_tri[sai_dinh_dang].iloc[:SO_VI_DU].tolist()]
        if luat["min"] is not None or luat["max"] is not None:
            so_hop_le = so[~sai_dinh_dang]
            thap = luat["min"] if luat["min"] is not None else float("-inf")
            cao = luat["max"] if luat["max"] is not None else float("inf")
            ngoai = (so_hop_le < thap) | (so_hop_le > cao)
            ket_qua["plausibility.range"] = [int(ngoai.sum()), so_hop_le[ngoai].iloc[:SO_VI_DU].tolist()]

    if luat["pattern"]:
        mau = re.compile(luat["pattern"])
        sai = ~co_gia_tri.map(lambda x: bool(mau.fullmatch(str(x).strip())))
        ket_qua["conformance.pattern"] = [int(sai.sum()), list(co_gia_tri[sai].unique())[:SO_VI_DU]]

    if luat["allowed_values"]:
        cho_phep = {str(v) for v in luat["allowed_values"]}
        sai = ~co_gia_tri.map(lambda x: str(x).strip()).isin(cho_phep)
        ket_qua["conformance.allowed_values"] = [int(sai.sum()), list(co_gia_tri[sai].unique())[:SO_VI_DU]]
    return ket_qua


def profile_pandas(df: pd.DataFrame, hop_dong: Optional[dict] = None) -> dict:
    """Hồ sơ của một DataFrame pandas (kiểu cột lấy theo dtype của pandas)."""
    luat_cot = hop_dong["columns"] if hop_dong else {}
    ho_so = {"so_dong": len(df), "hop_dong": contract_label(hop_dong), "cot": {}}
    for ten in df.columns:
        cot = df[ten]
        kieu = logical_type(cot)
        thong_tin = {
            "type": kieu,
            "null_pct": round(float(cot.isna().mean() * 100), 2) if len(cot) else 0.0,
            "integral": False,
            "so_rong": int(_o_rong(cot).sum()),
        }
        if kieu in ("int", "float"):
            thong_tin["mean"] = round(float(cot.mean()), 4)
            thong_tin["integral"] = bool((cot.dropna() % 1 == 0).all())
        if ten in luat_cot:
            thong_tin["vi_pham"] = _vi_pham_pandas(cot, luat_cot[ten])
        ho_so["cot"][str(ten)] = thong_tin
    return ho_so


# ---------------------------------------------------------------------------
# 2. Spark
# ---------------------------------------------------------------------------

def _kieu_spark(dtype) -> str:
    from pyspark.sql import types as T
    if isinstance(dtype, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        return "int"
    if isinstance(dtype, (T.FloatType, T.DoubleType, T.DecimalType)):
        return "float"
    if isinstance(dtype, T.BooleanType):
        return "bool"
    if isinstance(dtype, (T.DateType, T.TimestampType)):
        return "datetime"
    return "string"


def profile_spark(sdf, hop_dong: Optional[dict] = None, nhom: Optional[str] = None,
                  suy_kieu_tu_chuoi: bool = False):
    """
    Hồ sơ của một DataFrame Spark, tính bằng MỘT lần quét dữ liệu.

    nhom              : None -> trả về một hồ sơ cho cả bảng;
                        tên cột (ví dụ "_file") -> trả về {giá trị nhóm: hồ sơ} (mỗi lô một hồ sơ)
    suy_kieu_tu_chuoi : True khi dữ liệu là CSV đọc thành chuỗi -> đoán kiểu int / float / string
                        theo nội dung (giống cách pandas.read_csv đoán kiểu)
    """
    from pyspark.sql import functions as F

    luat_cot = hop_dong["columns"] if hop_dong else {}
    cac_cot = [c for c in sdf.columns if c != nhom]
    kieu_cot = {f.name: _kieu_spark(f.dataType) for f in sdf.schema.fields}

    bieu_thuc = [F.count(F.lit(1)).alias("so_dong")]
    for i, ten in enumerate(cac_cot):
        cot = sdf[ten]
        la_chuoi = kieu_cot[ten] == "string"
        chuoi = cot if la_chuoi else cot.cast("string")
        rong = cot.isNull() | (F.trim(chuoi) == "") if la_chuoi else cot.isNull()
        if kieu_cot[ten] == "float":
            rong = rong | F.isnan(cot)
        so = chuoi.try_cast("double") if la_chuoi else cot.cast("double")
        la_so = ~rong & so.isNotNull()
        nguyen = la_so & (so == F.floor(so))

        def _dem(dieu_kien, ten_so):
            return F.sum(F.when(dieu_kien, 1).otherwise(0)).alias(f"{ten_so}__{i}")

        bieu_thuc += [_dem(cot.isNull(), "null"), _dem(rong, "rong"), _dem(~rong, "co_gia_tri"),
                      _dem(la_so, "so"), _dem(nguyen, "nguyen"), F.avg(F.when(la_so, so)).alias(f"tb__{i}")]
        if la_chuoi and suy_kieu_tu_chuoi:
            bieu_thuc.append(_dem(~rong & chuoi.rlike(MAU_SO_NGUYEN), "chuoi_nguyen"))

        luat = luat_cot.get(ten)
        if luat is None:
            continue

        def _vi_pham(dieu_kien, ma, gia_tri=chuoi):
            return [_dem(dieu_kien, f"vp_{ma}"),
                    F.slice(F.collect_set(F.when(dieu_kien, F.trim(gia_tri))), 1, SO_VI_DU).alias(f"vd_{ma}__{i}")]

        if luat["type"] in ("int", "float"):
            sai_dinh_dang = ~rong & so.isNull()
            if luat["type"] == "int":
                sai_dinh_dang = sai_dinh_dang | (la_so & (so != F.floor(so)))
            bieu_thuc += _vi_pham(sai_dinh_dang, "type")
            if luat["min"] is not None or luat["max"] is not None:
                hop_le = ~rong & ~sai_dinh_dang
                ngoai = F.lit(False)
                if luat["min"] is not None:
                    ngoai = ngoai | (so < float(luat["min"]))
                if luat["max"] is not None:
                    ngoai = ngoai | (so > float(luat["max"]))
                bieu_thuc += _vi_pham(hop_le & ngoai, "range")
        if luat["pattern"]:
            bieu_thuc += _vi_pham(~rong & ~F.trim(chuoi).rlike(f"^(?:{luat['pattern']})$"), "pattern")
        if luat["allowed_values"]:
            bieu_thuc += _vi_pham(~rong & ~F.trim(chuoi).isin([str(v) for v in luat["allowed_values"]]),
                                  "allowed")

    ket_qua_gop = sdf.groupBy(nhom).agg(*bieu_thuc) if nhom else sdf.agg(*bieu_thuc)
    co_cot = set(ket_qua_gop.columns)
    dong = ket_qua_gop.collect()

    def _ho_so(r) -> dict:
        ho_so = {"so_dong": int(r["so_dong"]), "hop_dong": contract_label(hop_dong), "cot": {}}
        tong = ho_so["so_dong"]
        for i, ten in enumerate(cac_cot):
            co_gia_tri, so, nguyen = r[f"co_gia_tri__{i}"] or 0, r[f"so__{i}"] or 0, r[f"nguyen__{i}"] or 0
            kieu = kieu_cot[ten]
            if co_gia_tri == 0:
                kieu = "unknown"
            elif kieu == "string" and suy_kieu_tu_chuoi and so == co_gia_tri:
                kieu = "int" if r[f"chuoi_nguyen__{i}"] == co_gia_tri else "float"
            thong_tin = {
                "type": kieu,
                "null_pct": round((r[f"null__{i}"] or 0) / tong * 100, 2) if tong else 0.0,
                "integral": kieu in ("int", "float") and nguyen == so,
                "so_rong": int(r[f"rong__{i}"] or 0),
            }
            if kieu in ("int", "float") and r[f"tb__{i}"] is not None:
                thong_tin["mean"] = round(float(r[f"tb__{i}"]), 4)
            luat = luat_cot.get(ten)
            if luat is not None:
                thong_tin["vi_pham"] = {}
                for ma, ten_luat in (("type", "conformance.type"), ("range", "plausibility.range"),
                                     ("pattern", "conformance.pattern"), ("allowed", "conformance.allowed_values")):
                    if f"vp_{ma}__{i}" in co_cot:
                        thong_tin["vi_pham"][ten_luat] = [int(r[f"vp_{ma}__{i}"] or 0), sorted(r[f"vd_{ma}__{i}"] or [])]
            ho_so["cot"][ten] = thong_tin
        return ho_so

    if nhom is None:
        return _ho_so(dong[0])
    return {r[nhom]: _ho_so(r) for r in dong}


# ---------------------------------------------------------------------------
# 3. Lưu / dùng lại hồ sơ theo checksum
# ---------------------------------------------------------------------------

def load_cached_profiles(cac_checksum, hop_dong: Optional[dict], db_path: Optional[str] = None) -> dict:
    """{checksum: hồ sơ} của những file đã từng được tính hồ sơ theo đúng hợp đồng và phiên bản bộ tính."""
    cac_checksum = list(cac_checksum)
    if not cac_checksum:
        return {}
    nhan = contract_label(hop_dong)
    ket_qua = {}
    conn = get_db_connection(db_path)
    for i in range(0, len(cac_checksum), 500):       # SQLite giới hạn số tham số trong một câu lệnh
        phan = cac_checksum[i:i + 500]
        dong = conn.execute(
            f"SELECT checksum, profile_json FROM batch_profiles WHERE contract = ? AND profiler_version = ? "
            f"AND checksum IN ({','.join('?' * len(phan))})", [nhan, PROFILER_VERSION, *phan]).fetchall()
        ket_qua.update({d["checksum"]: json.loads(d["profile_json"]) for d in dong})
    conn.close()
    return ket_qua


def save_profiles(theo_checksum: dict, hop_dong: Optional[dict], db_path: Optional[str] = None) -> None:
    """Lưu {checksum: hồ sơ} vừa tính để lần chạy sau dùng lại."""
    if not theo_checksum:
        return
    conn = get_db_connection(db_path)
    conn.executemany(
        "INSERT OR REPLACE INTO batch_profiles (checksum, contract, profiler_version, profile_json, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [(ck, contract_label(hop_dong), PROFILER_VERSION, json.dumps(hs, ensure_ascii=False, default=str), now_str())
         for ck, hs in theo_checksum.items()])
    conn.commit()
    conn.close()


if __name__ == "__main__":
    from governance.contracts import find_contract
    from governance.sample_data import make_patient

    df = make_patient("B")
    df.loc[0, "admissionweight"] = 0.5
    ho_so = profile_pandas(df, find_contract("raw_patient_hospital_B"))
    print(f"{ho_so['so_dong']} dòng, hợp đồng {ho_so['hop_dong']}")
    for ten, tt in ho_so["cot"].items():
        print(f"  {ten:<20} {tt['type']:<8} trống {tt['so_rong']:>3}  {tt.get('vi_pham', '')}")
