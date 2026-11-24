"""
data_quality.py
================
Module kiểm tra chất lượng dữ liệu tổng quát cho dự án ICU Mortality Prediction.

Cách dùng:
    from src.data_quality import audit, check_overview, check_plausibility

    df = pd.read_csv("data/raw/eicu/lab.csv")
    audit(df, name="lab.csv")
"""

import pandas as pd
import numpy as np


# ============================================================
# 1. TỔNG QUAN
# ============================================================
def check_overview(df: pd.DataFrame, name: str = "DataFrame") -> None:
    """Kiểm tra tổng quan: shape, kiểu dữ liệu, missing, trùng lặp."""
    print(f"\n{'=' * 60}")
    print(f"TỔNG QUAN: {name}")
    print(f"{'=' * 60}")
    print(f"Kích thước : {df.shape[0]:,} dòng × {df.shape[1]} cột")
    print(f"Bộ nhớ     : {df.memory_usage(deep=True).sum() / 1024:.1f} KB")

    print(f"\nKiểu dữ liệu:")
    print(df.dtypes.to_string())

    miss = df.isna().sum()
    miss_pct = (miss / len(df) * 100).round(2)
    miss_df = pd.DataFrame({"missing": miss, "pct": miss_pct})
    miss_df = miss_df[miss_df["missing"] > 0]
    print(f"\nMissing values:")
    if len(miss_df) == 0:
        print("  (không có)")
    else:
        print(miss_df.to_string())

    print(f"\nSố dòng trùng lặp hoàn toàn: {df.duplicated().sum()}")


# ============================================================
# 2. PLAUSIBILITY (ngưỡng sinh lý)
# ============================================================
def check_plausibility(df: pd.DataFrame, rules: dict = None) -> None:
    """
    Kiểm tra các cột theo ngưỡng sinh lý hợp lý.

    Tham số:
        rules: dict {col: (low, high, unit)}. Nếu None, dùng bộ mặc định
               cho bảng patient.
    """
    if rules is None:
        rules = {
            "age": (18, 120, "tuổi"),
            "male_sex": (0, 1, "0=Female, 1=Male"),
            "bmi": (10, 80, "kg/m²"),
            "mortality": (0, 1, "0=Alive, 1=Expired"),
        }

    print(f"\n{'=' * 60}")
    print("KIỂM TRA PLAUSIBILITY (ngưỡng sinh lý)")
    print(f"{'=' * 60}")

    for col, (lo, hi, unit) in rules.items():
        if col not in df.columns:
            continue
        s = df[col].dropna()
        if len(s) == 0:
            print(f"⚠️ {col:15s} | toàn bộ NaN")
            continue
        n_bad = ((s < lo) | (s > hi)).sum()
        status = "✅" if n_bad == 0 else "⚠️"
        print(f"{status} {col:15s} | min={s.min():>10.2f} max={s.max():>10.2f} "
              f"| ngoài [{lo}, {hi}] ({unit}): {n_bad} dòng")


# ============================================================
# 3. PHÂN BỐ & MẤT CÂN BẰNG NHÃN
# ============================================================
def check_distribution(df: pd.DataFrame, label_col: str = "mortality") -> None:
    """Kiểm tra phân bố các cột số và tỷ lệ nhãn."""
    print(f"\n{'=' * 60}")
    print("PHÂN BỐ CÁC CỘT SỐ")
    print(f"{'=' * 60}")

    num_cols = df.select_dtypes(include=np.number).columns
    for col in num_cols:
        s = df[col].dropna()
        if len(s) == 0:
            continue
        print(f"\n--- {col} ---")
        print(f"  count={len(s):,}  mean={s.mean():.2f}  std={s.std():.2f}")
        print(f"  min={s.min():.2f}  q25={s.quantile(.25):.2f}  "
              f"median={s.median():.2f}  q75={s.quantile(.75):.2f}  max={s.max():.2f}")
        skew = s.skew()
        if abs(skew) > 2:
            print(f"  ⚠️ Phân bố lệch mạnh (skew={skew:.2f})")

    if label_col in df.columns:
        print(f"\n{'=' * 60}")
        print(f"MẤT CÂN BẰNG NHÃN ({label_col})")
        print(f"{'=' * 60}")
        vc = df[label_col].value_counts()
        vc_pct = df[label_col].value_counts(normalize=True).round(4) * 100
        for k in vc.index:
            print(f"  {label_col}={k}: {vc[k]:,} ({vc_pct[k]:.2f}%)")
        ratio = vc.max() / vc.min() if vc.min() > 0 else np.inf
        print(f"  Tỷ lệ mất cân bằng: {ratio:.1f}:1")
        if ratio > 5:
            print(f"  ⚠️ Mất cân bằng đáng kể — dùng scale_pos_weight khi train")


# ============================================================
# 4. VẤN ĐỀ ẨN
# ============================================================
def check_hidden_issues(df: pd.DataFrame, sentinels: list = None) -> None:
    """Tìm NaN đội lốt, sentinel, cột hằng số, cột toàn NaN."""
    if sentinels is None:
        sentinels = [-1, -999, -9999, 9999, 99999]

    print(f"\n{'=' * 60}")
    print("KIỂM TRA VẤN ĐỀ ẨN")
    print(f"{'=' * 60}")

    for col in df.columns:
        s = df[col]
        if s.dtype == "object":
            print(f"⚠️ {col}: kiểu object — unique mẫu: {list(s.unique()[:5])}")
            continue
        if pd.api.types.is_numeric_dtype(s):
            for sentinel in sentinels:
                n = (s == sentinel).sum()
                if n > 0:
                    print(f"⚠️ {col}: {n} dòng = {sentinel} (sentinel missing?)")
        if s.isna().all():
            print(f"⚠️ {col}: TOÀN BỘ là NaN — nên loại bỏ")
        elif s.nunique(dropna=True) == 1:
            print(f"⚠️ {col}: chỉ có 1 giá trị duy nhất — vô dụng cho mô hình")


# ============================================================
# 5. LOGIC GIỮA CÁC CỘT
# ============================================================
def check_logic(df: pd.DataFrame, label_col: str = "mortality") -> None:
    """Kiểm tra các ràng buộc logic giữa các cột."""
    print(f"\n{'=' * 60}")
    print("KIỂM TRA LOGIC GIỮA CÁC CỘT")
    print(f"{'=' * 60}")

    if "patientunitstayid" in df.columns:
        n = (df["patientunitstayid"] <= 0).sum()
        print(f"{'✅' if n == 0 else '⚠️'} patientunitstayid <= 0: {n} dòng")

    if "age" in df.columns:
        n = (df["age"] < 18).sum()
        print(f"{'✅' if n == 0 else '⚠️'} age < 18 (lọt cohort): {n} dòng")

    if label_col in df.columns:
        bad = df[label_col].dropna()
        n = (~bad.isin([0, 1])).sum()
        print(f"{'✅' if n == 0 else '⚠️'} {label_col} không phải 0/1: {n} dòng")
        print(f"   NaN trong {label_col}: {df[label_col].isna().sum()}")

    if "male_sex" in df.columns:
        bad = df["male_sex"].dropna()
        n = (~bad.isin([0, 1])).sum()
        print(f"{'✅' if n == 0 else '⚠️'} male_sex không phải 0/1: {n} dòng")


# ============================================================
# 6. PHÂN TÍCH NHÓM THIẾU BMI (đặc thù patient)
# ============================================================
def check_bmi_missing(df: pd.DataFrame, label_col: str = "mortality") -> None:
    """So sánh nhóm có BMI vs thiếu BMI — quyết định giữ NaN hay không."""
    if "bmi" not in df.columns or label_col not in df.columns:
        return

    print(f"\n{'=' * 60}")
    print("PHÂN TÍCH NHÓM THIẾU BMI")
    print(f"{'=' * 60}")

    df_tmp = df.copy()
    df_tmp["bmi_missing"] = df_tmp["bmi"].isna()
    result = df_tmp.groupby("bmi_missing")[label_col].agg(
        count="count", deaths="sum", mortality_rate="mean"
    ).round(4)
    result.index = ["Có BMI", "Thiếu BMI"]
    print(result.to_string())

    if len(result) == 2:
        diff = result.loc["Thiếu BMI", "mortality_rate"] - result.loc["Có BMI", "mortality_rate"]
        print(f"\nChênh lệch: {diff:+.4f} ({diff * 100:+.2f} điểm %)")
        if abs(diff) > 0.02:
            print("→ ⚠️ Khác biệt đáng kể — thiếu BMI mang tín hiệu! GIỮ NaN.")
        else:
            print("→ Không khác biệt đáng kể — giữ NaN vẫn an toàn.")


# ============================================================
# HÀM TỔNG: audit
# ============================================================
def audit(df: pd.DataFrame,
          name: str = "DataFrame",
          label_col: str = "mortality",
          plausibility_rules: dict = None,
          sentinels: list = None) -> None:
    """
    Chạy toàn bộ kiểm tra chất lượng dữ liệu.

    Tham số:
        df                 : DataFrame cần kiểm tra
        name               : tên hiển thị
        label_col          : tên cột nhãn (mặc định 'mortality')
        plausibility_rules : dict ngưỡng sinh lý (None = mặc định patient)
        sentinels          : list giá trị sentinel (None = mặc định)
    """
    check_overview(df, name)
    check_plausibility(df, rules=plausibility_rules)
    check_distribution(df, label_col=label_col)
    check_hidden_issues(df, sentinels=sentinels)
    check_logic(df, label_col=label_col)
    check_bmi_missing(df, label_col=label_col)

    print(f"\n{'=' * 60}")
    print(f"✅ HOÀN TẤT KIỂM TRA: {name}")
    print(f"{'=' * 60}\n")