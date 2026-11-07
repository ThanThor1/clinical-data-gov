"""
Bộ chuyển nguồn: đưa dữ liệu của các nguồn KHÔNG theo schema chuẩn về schema chuẩn của tầng raw, theo đúng
khai báo trong sổ đăng ký nguồn (sources/*.yaml, đọc bằng governance/sources.py).

Kết quả ghi vào data/02_staging/hospital_<mã>/{patient,lab,vitalPeriodic}.csv giống phần của eICU
(pipeline/split_hospitals.py), nên các bước sau (chia 3 đợt, nạp raw, cổng kiểm tra, chuẩn hóa, AI-ready)
dùng chung cho mọi nguồn. Phải chạy SAU split_hospitals (bước đó xóa mọi thư mục hospital_* trong staging).

Định dạng nguồn:
    csv_wide : đã đúng schema chuẩn (eICU) -> không cần chuyển
    eav_long : dạng dài, mỗi lượt nằm ICU một file "Time,Parameter,Value" nằm trong một file .tar.gz
               (đọc thẳng, không giải nén ra đĩa) + một file kết quả điều trị (PhysioNet/CinC Challenge 2012)

Chạy: python -m pipeline.source_adapters
"""

import os
import sys
import tarfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from governance.sources import load_sources

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING_DIR = os.path.join(ROOT, "data", "02_staging")


def doc_eav(duong_dan_tar: str, ma_id: str) -> pd.DataFrame:
    """Mọi file <id>.txt trong file .tar.gz thành MỘT bảng dài: _id, Time, Parameter, Value."""
    cac_phan = []
    with tarfile.open(duong_dan_tar, "r:gz") as tar:
        for muc in tar:
            if not (muc.isfile() and muc.name.endswith(".txt")):
                continue
            df = pd.read_csv(tar.extractfile(muc))
            df.insert(0, "_id", int(df.loc[df["Parameter"] == ma_id, "Value"].iloc[0]))
            cac_phan.append(df)
    return pd.concat(cac_phan, ignore_index=True)


def _phut(cot: pd.Series) -> pd.Series:
    """'hh:mm' -> số phút."""
    phan = cot.str.split(":", expand=True).astype(int)
    return phan[0] * 60 + phan[1]


def _doi_ma(cot: pd.Series, bang_ma: dict) -> pd.Series:
    """Đổi mã theo bảng `values` của file ánh xạ (khóa YAML có thể là số hoặc chuỗi); mã lạ -> trống."""
    theo_chuoi = {str(k): v for k, v in bang_ma.items()}
    return cot.map(lambda x: None if pd.isna(x) else theo_chuoi.get(str(int(x)) if float(x).is_integer() else str(x)))


def _so_nguyen_neu_duoc(cot: pd.Series) -> pd.Series:
    """Cột số mà mọi giá trị đều nguyên -> kiểu Int64 (ghi CSV là '54' chứ không phải '54.0')."""
    if pd.api.types.is_float_dtype(cot) and (cot.dropna() % 1 == 0).all():
        return cot.astype("Int64")
    return cot


def chuyen_eav_long(nguon: dict, landing_dir: str, staging_dir: str) -> dict:
    """Chuyển một nguồn eav_long về schema chuẩn, ghi staging/hospital_<mã>/. Trả về số dòng từng bảng."""
    if len(nguon["hospitals"]) != 1:
        raise ValueError(f"Nguồn eav_long '{nguon['name']}' phải gán cho đúng một mã viện")
    ma_id = nguon["id_parameter"]
    su_kien = doc_eav(os.path.join(landing_dir, nguon["files"]["records"]), ma_id)
    if "missing_value" in nguon:                       # giá trị "không có" của nguồn -> bỏ sự kiện (= ô trống)
        su_kien = su_kien[su_kien["Value"] != nguon["missing_value"]]
    su_kien = su_kien.assign(_phut=_phut(su_kien[nguon["time_column"]])).sort_values(["_id", "_phut"], kind="stable")
    ket_qua_dieu_tri = None
    if "outcomes" in nguon.get("files", {}):
        ket_qua_dieu_tri = pd.read_csv(os.path.join(landing_dir, nguon["files"]["outcomes"])).set_index(ma_id)

    # Bảng patient: mỗi lượt nằm ICU một dòng; giá trị là giá trị hợp lệ ĐẦU TIÊN theo thời gian
    benh_nhan = pd.DataFrame(index=pd.Index(sorted(su_kien["_id"].unique()), name="_id"))
    for cot, luat in nguon["patient"].items():
        if luat["from"] == ma_id:
            gia_tri = pd.Series(benh_nhan.index, index=benh_nhan.index)
        elif luat.get("file") == "outcomes":
            gia_tri = ket_qua_dieu_tri[luat["from"]].reindex(benh_nhan.index)
        else:
            gia_tri = su_kien[su_kien["Parameter"] == luat["from"]].groupby("_id")["Value"].first().reindex(benh_nhan.index)
        if "values" in luat:
            gia_tri = _doi_ma(gia_tri, luat["values"])
        benh_nhan[cot] = _so_nguyen_neu_duoc(gia_tri)

    # Sinh hiệu và xét nghiệm: mỗi lần đo một dòng
    sinh_hieu = pd.DataFrame({"patientunitstayid": [], "observationoffset": []})
    for cot, luat in nguon["vital"].items():
        phan = su_kien[su_kien["Parameter"] == luat["from"]]
        sinh_hieu = pd.DataFrame({"patientunitstayid": phan["_id"], "observationoffset": phan["_phut"],
                                  cot: _so_nguyen_neu_duoc(phan["Value"])})
    phan = su_kien[su_kien["Parameter"].isin(nguon["lab"])]
    luat_lab = phan["Parameter"].map(nguon["lab"])
    xet_nghiem = pd.DataFrame({"patientunitstayid": phan["_id"], "labresultoffset": phan["_phut"],
                               "labname": luat_lab.map(lambda l: l["to"]),
                               "labresult": phan["Value"] * luat_lab.map(lambda l: l.get("scale", 1))})

    thu_muc = os.path.join(staging_dir, f"hospital_{nguon['hospitals'][0]}")
    os.makedirs(thu_muc, exist_ok=True)
    benh_nhan.to_csv(os.path.join(thu_muc, "patient.csv"), index=False)
    sinh_hieu.to_csv(os.path.join(thu_muc, "vitalPeriodic.csv"), index=False)
    xet_nghiem.to_csv(os.path.join(thu_muc, "lab.csv"), index=False)
    return {"patient": len(benh_nhan), "vital": len(sinh_hieu), "lab": len(xet_nghiem)}


BO_CHUYEN = {"eav_long": chuyen_eav_long}


def main(staging_dir=None, sources_dir=None):
    """Chuyển mọi nguồn có bộ chuyển (bỏ qua nguồn chưa tải dữ liệu). Trả về mã các viện đã chuyển."""
    staging_dir = staging_dir or STAGING_DIR
    da_chuyen = []
    for nguon in load_sources(sources_dir):
        if nguon["format"] == "csv_wide":
            continue
        landing = os.path.join(ROOT, nguon["landing"])
        if not all(os.path.isfile(os.path.join(landing, f)) for f in nguon["files"].values()):
            print(f"  Bỏ qua nguồn {nguon['name']}: chưa có dữ liệu ở {landing} (xem README mục 3)")
            continue
        so_dong = BO_CHUYEN[nguon["format"]](nguon, landing, staging_dir)
        print(f"  ✅ {nguon['name']} ({nguon['format']}) -> hospital_{nguon['hospitals'][0]}: "
              f"{so_dong['patient']} lượt nằm ICU, {so_dong['vital']} dòng sinh hiệu, {so_dong['lab']} dòng xét nghiệm")
        da_chuyen += nguon["hospitals"]
    return da_chuyen


if __name__ == "__main__":
    main()
