"""
Sinh dữ liệu GIẢ mô phỏng eICU để test và demo khi chưa có dữ liệu thật.

Dữ liệu giả cố ý giống đặc điểm dữ liệu thật (tài liệu phân công mục 7):
- age là chuỗi, có "> 89" và có ô rỗng; gender là Male/Female (có ô rỗng)
- admissionweight thiếu khoảng 20%
- Bệnh viện B KHÔNG có dòng vitalPeriodic nào (bảng rỗng nhưng vẫn có tên cột)
- unitdischargestatus là Alive/Expired

Ngoài ra có vài hàm "chuẩn hóa" đơn giản (thay cho pipeline của Người 1) để demo
có đủ 3 tầng raw -> standardized -> ai_ready.
"""

import numpy as np
import pandas as pd

from governance.privacy import pseudonymize_series

# Mã hospitalid thật của 3 bệnh viện được chọn và số bệnh nhân
THONG_TIN_VIEN = {
    "A": {"hospitalid": 146, "so_benh_nhan": 40},
    "B": {"hospitalid": 123, "so_benh_nhan": 30},
    "C": {"hospitalid": 171, "so_benh_nhan": 25},
}

COT_VITAL = ["vitalperiodicid", "patientunitstayid", "observationoffset",
             "temperature", "sao2", "heartrate", "respiration"]

# Tên xét nghiệm -> (trung bình, độ lệch chuẩn, nhỏ nhất, lớn nhất), đơn vị như eICU (mg/dL, mmol/L, g/dL)
XET_NGHIEM = {
    "glucose": (140, 45, 40, 600),
    "lactate": (2.0, 1.2, 0.3, 15),
    "creatinine": (1.2, 0.6, 0.3, 10),
    "potassium": (4.1, 0.5, 2.5, 6.5),
    "sodium": (139, 4, 120, 160),
    "Hgb": (11, 2, 6, 17),
}
TEN_XET_NGHIEM = list(XET_NGHIEM)


def _ma_benh_nhan(vien: str, n: int) -> list[int]:
    """Mã patientunitstayid giả, mỗi viện một dải số riêng để không trùng."""
    goc = {"A": 140000, "B": 240000, "C": 340000}[vien]
    return [goc + i for i in range(n)]


def make_patient(vien: str, seed: int = 0) -> pd.DataFrame:
    """Bảng patient giả của một bệnh viện (giống patient.csv của eICU)."""
    rng = np.random.default_rng(seed + ord(vien))
    n = THONG_TIN_VIEN[vien]["so_benh_nhan"]

    tuoi = [str(t) for t in rng.integers(18, 90, n)]
    tuoi[0] = "> 89"                       # có bệnh nhân trên 89 tuổi
    tuoi[1] = None                          # có ô tuổi rỗng

    gioi = list(rng.choice(["Male", "Female"], n))
    gioi[2] = None                          # có ô giới tính rỗng

    can_nang = rng.normal(80, 18, n).clip(40, 180).round(1)
    # Thiếu khoảng 20% cân nặng như dữ liệu thật, rải đều để đợt nào cũng dưới ngưỡng 40% của hợp đồng
    can_nang[::5] = np.nan

    trang_thai = ["Alive"] * n
    trang_thai[-1] = "Expired"              # có ít nhất 1 ca tử vong

    return pd.DataFrame({
        "patientunitstayid": _ma_benh_nhan(vien, n),
        "uniquepid": [f"00{ord(vien) % 10}-{i:05d}" for i in range(n)],
        "gender": gioi,
        "age": tuoi,
        "ethnicity": rng.choice(["Caucasian", "African American", "Asian"], n),
        "hospitalid": THONG_TIN_VIEN[vien]["hospitalid"],
        "admissionheight": rng.normal(170, 10, n).round(1),
        "admissionweight": can_nang,
        "unitdischargestatus": trang_thai,
    })


def make_lab(vien: str, so_dong_moi_bn: int = 5, seed: int = 0) -> pd.DataFrame:
    """Bảng lab giả (giống lab.csv của eICU)."""
    rng = np.random.default_rng(seed + ord(vien) + 1)
    ma_bn = np.repeat(_ma_benh_nhan(vien, THONG_TIN_VIEN[vien]["so_benh_nhan"]), so_dong_moi_bn)
    n = len(ma_bn)
    ten = rng.choice(TEN_XET_NGHIEM, n)
    tb, lech, thap, cao = (np.array([XET_NGHIEM[t][i] for t in ten]) for i in range(4))
    return pd.DataFrame({
        "labid": np.arange(n) + 1,
        "patientunitstayid": ma_bn,
        "labresultoffset": rng.integers(-300, 3000, n),
        "labname": ten,
        "labresult": rng.normal(tb, lech).clip(thap, cao).round(2),
        "labmeasurenamesystem": "mg/dL",
    })


def make_vital(vien: str, so_dong_moi_bn: int = 10, seed: int = 0) -> pd.DataFrame:
    """Bảng vitalPeriodic giả. Bệnh viện B trả về bảng 0 dòng (giống dữ liệu thật)."""
    if vien == "B":
        return pd.DataFrame(columns=COT_VITAL)
    rng = np.random.default_rng(seed + ord(vien) + 2)
    ma_bn = np.repeat(_ma_benh_nhan(vien, THONG_TIN_VIEN[vien]["so_benh_nhan"]), so_dong_moi_bn)
    n = len(ma_bn)
    return pd.DataFrame({
        "vitalperiodicid": np.arange(n) + 1,
        "patientunitstayid": ma_bn,
        "observationoffset": np.arange(n) * 5,
        "temperature": np.nan,               # eICU: nhiệt độ hầu như trống
        "sao2": rng.integers(90, 101, n).astype(float),
        "heartrate": rng.normal(85, 12, n).clip(45, 160).round(0),
        "respiration": rng.normal(18, 4, n).clip(8, 40).round(0),
    })


def make_hospital(vien: str, seed: int = 0) -> dict[str, pd.DataFrame]:
    """Cả 3 bảng raw của một bệnh viện: {'patient': ..., 'lab': ..., 'vital': ...}."""
    return {
        "patient": make_patient(vien, seed),
        "lab": make_lab(vien, seed=seed),
        "vital": make_vital(vien, seed=seed),
    }


# ---------------------------------------------------------------------------
# Chuẩn hóa đơn giản (chỉ để demo, pipeline thật do Người 1 viết)
# ---------------------------------------------------------------------------

def _tuoi_so(gia_tri) -> float:
    """'> 89' -> 90, '45' -> 45, rỗng -> NaN."""
    if gia_tri is None or (isinstance(gia_tri, float) and np.isnan(gia_tri)):
        return np.nan
    gia_tri = str(gia_tri).strip()
    if gia_tri.startswith(">"):
        return 90.0
    return float(gia_tri) if gia_tri else np.nan


def standardize_patients(raw: dict[str, dict[str, pd.DataFrame]], dot: str = "dot_mau") -> pd.DataFrame:
    """Ghép bảng patient của các viện thành standardized_patients (đúng tên cột chuẩn)."""
    phan = []
    for vien, bang in raw.items():
        p = bang["patient"]
        df = pd.DataFrame({
            "patient_id": p["patientunitstayid"].astype(int),
            "age": p["age"].map(_tuoi_so).astype("Int64"),
            "gender": p["gender"].map({"Male": "M", "Female": "F"}).fillna("U"),
            "weight_kg": pd.to_numeric(p["admissionweight"], errors="coerce"),
            "source_hospital": f"Bệnh viện {vien}",
            "source_batch": f"hospital_{vien}/{dot}",
            "unitdischargestatus": p["unitdischargestatus"],
        })
        # Nhịp tim trung bình mỗi bệnh nhân (viện B không có vital -> NaN)
        v = bang["vital"]
        if len(v):
            tb = v.groupby("patientunitstayid")["heartrate"].mean()
            df["heartrate_mean"] = df["patient_id"].map(tb)
        else:
            df["heartrate_mean"] = np.nan
        phan.append(df)
    return pd.concat(phan, ignore_index=True)


def standardize_labs(raw: dict[str, dict[str, pd.DataFrame]], dot: str = "dot_mau") -> pd.DataFrame:
    """Ghép bảng lab của các viện thành standardized_labs."""
    phan = []
    for vien, bang in raw.items():
        lab = bang["lab"]
        phan.append(pd.DataFrame({
            "patient_id": lab["patientunitstayid"].astype(int),
            "lab_name": lab["labname"],
            "lab_value": pd.to_numeric(lab["labresult"], errors="coerce"),
            "source_hospital": f"Bệnh viện {vien}",
            "source_batch": f"hospital_{vien}/{dot}",
        }))
    return pd.concat(phan, ignore_index=True)


def build_ai_ready(std_patients: pd.DataFrame) -> pd.DataFrame:
    """Tạo ai_ready_icu_mortality: giả danh hóa patient_id (HMAC-SHA256) và tạo nhãn tử vong 0/1."""
    return pd.DataFrame({
        "patient_hash": pseudonymize_series(std_patients["patient_id"]),
        "age": std_patients["age"],
        "gender": std_patients["gender"],
        "weight_kg": std_patients["weight_kg"],
        "heartrate_mean": std_patients["heartrate_mean"],
        "icu_mortality": (std_patients["unitdischargestatus"] == "Expired").astype(int),
    })
