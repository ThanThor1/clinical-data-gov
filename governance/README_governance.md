# Governance – Cổng kiểm tra, phát hiện vi phạm, lineage (Người 2)

## 1. Engine hoạt động thế nào

Cổng kiểm tra không đọc thẳng dữ liệu mà đọc **hồ sơ dữ liệu** (`profiling.py`): số dòng, kiểu, số ô trống,
số dòng sai kiểu / ngoài khoảng / ngoài tập giá trị của từng cột. Hồ sơ tính được bằng hai cách, luật giống nhau:

- `check_gate(ten_bang, df_pandas, cong)` → tự tính hồ sơ bằng pandas (test, demo nhỏ).
- `check_gate(ten_bang, ho_so, cong)` → hồ sơ đã tính bằng Spark: pipeline tính hồ sơ cho **mọi lô của
  một loại dữ liệu trong một job** (`profile_spark(..., nhom="_file")`, 558 lô / job với 186 viện).
  Hồ sơ được lưu theo checksum file (`batch_profiles`) nên lần chạy sau chỉ tính cho file mới / bị sửa.

```
Người 1 gọi check_gate(ten_bang, df | ho_so, "gate1" | "gate2")
   │
   ├─ contracts.find_contract(ten_bang)        tìm file contracts/*.yaml khớp tên bảng (hỗ trợ mẫu *)
   ├─ profiling.profile_pandas(df)             (nếu truyền DataFrame) hồ sơ dữ liệu
   │
   ├─ gate1 (schema + hợp đồng)
   │    ├─ schema_registry.register_schema()   lưu schema_versions, so với phiên bản HỢP LỆ gần nhất
   │    │                                      → COLUMN_RENAMED / REMOVED / TYPE_CHANGED / ADDED
   │    ├─ đối chiếu hợp đồng                  cột bắt buộc có mặt? đúng kiểu?
   │    └─ privacy.check_privacy()             (chỉ tầng ai_ready) có cột định danh? cột ngoài hợp đồng?
   ├─ gate2 (chất lượng – khung Kahn)
   │    ├─ Conformance   đúng kiểu số, giá trị thuộc allowed_values, khớp pattern
   │    ├─ Completeness  % ô trống <= max_null_pct, cột nullable:false không trống
   │    └─ Plausibility  giá trị trong [min, max]
   │
   ├─ mỗi lỗi BREAKING/WARNING → violation_engine.report_violation()
   │    ├─ suy ra bệnh viện từ tên bảng (…_hospital_B → "Bệnh viện B")
   │    ├─ lineage.get_affected_ai_tables(bảng, cột)  → bảng AI-ready bị ảnh hưởng
   │    ├─ INSERT violations (action "đã chặn" / "cảnh báo", kèm run_id và lô đang xử lý)
   │    └─ log_event("violation_engine", loại_lỗi, bảng, BLOCKED | SUCCESS)
   │
   └─ log_event("governance_gate", cong, bảng, BLOCKED | SUCCESS)  → trả về (đạt?, [lỗi])
```

**Ai quyết định mức độ?** Schema registry chỉ *phát hiện* thay đổi; **hợp đồng** quyết định
mức độ: cột *bắt buộc* bị đổi tên/xóa/sai kiểu → BREAKING; cột ngoài hợp đồng → WARNING.
Một lô lỗi bị chặn được lưu lại (is_breaking = 1) nhưng **không trở thành chuẩn** cho lô sau.

**Duyệt thay đổi schema:** đổi tên cột là hợp lệ (nội dung không đổi) thì kỹ sư dữ liệu gọi
`schema_registry.approve_rename(người_duyệt, bảng, cột_mới, cột_hợp_đồng, lý_do)` (hoặc bấm trên dashboard):
cần quyền ghi bảng đó, có ghi audit, lưu ở `schema_approvals`. Pipeline đổi tên khi đọc file của bảng đó nên lô
qua cổng, provenance của lô ghi ánh xạ đã áp, lineage có thêm cạnh từ cột mới.

**Vòng đời vi phạm:** khi một lần chạy kết thúc (`provenance.finish_run` → `violation_engine.update_statuses_after_run`),
vi phạm OPEN của các lần chạy trước chuyển **RESOLVED** nếu lô / bảng đó được kiểm tra lại và qua cổng, hoặc
**SUPERSEDED** nếu lần chạy này ghi lại đúng lỗi đó (cùng bảng, lô, loại lỗi).

## 1b. Vi phạm do pipeline đổi code (`pipeline_drift.py`)

Hợp đồng chỉ bắt giá trị sai luật. Lỗi code có thể cho ra dữ liệu đúng luật nhưng sai nghĩa (cân nặng đổi sang
pound vẫn nằm trong [20, 400]; nhãn tử vong bị đảo vẫn là 0/1). Mỗi phiên bản bảng đầu ra lưu phiên bản code
(git + băm nội dung mã nguồn), dấu vân tay đầu vào (theo checksum các lô raw gốc) và hồ sơ đầu ra. Trước khi ghi
phiên bản mới, nếu code đã đổi: so hồ sơ với phiên bản trước (cột thêm / bớt / đổi kiểu, trung bình lệch > 20%,
% trống tăng > 10 điểm, số dòng khác) → **cùng đầu vào: BREAKING, chặn**; khác đầu vào: WARNING. Không so khi
code không đổi, khi đã có phiên bản do chính code này tạo từ cùng đầu vào, hoặc khi phiên bản code đã được duyệt
(`approve_code_version`, thay đổi đầu ra là cố ý).

## 1c. Nguồn không đồng nhất (`sources.py`)

Sổ đăng ký `sources/*.yaml` mô tả từng hệ thống nguồn và ánh xạ về schema chuẩn (= các hợp đồng `raw_*`). Từ đó:
lineage từ **tham số gốc** của nguồn (`nguon_challenge2012.Weight → raw_patient_hospital_c2012.admissionweight`),
và cảnh báo `SEMANTIC_MISMATCH` khi `semantics` của nguồn khác `semantics` của cột trong hợp đồng.

## 2. Loại lỗi và mức độ

| Loại lỗi | Khi nào | Mức độ |
|---|---|---|
| `SCHEMA_COLUMN_RENAMED` | cột mất + cột mới cùng kiểu, phân phối/tên gần giống | BREAKING nếu cột bắt buộc, không thì WARNING |
| `SCHEMA_COLUMN_REMOVED` | cột biến mất | như trên |
| `SCHEMA_TYPE_CHANGED` | kiểu thực tế không hợp kiểu hợp đồng (vd float → chuỗi) | như trên |
| `MISSING_REQUIRED_COLUMN` | thiếu cột bắt buộc (khi chưa có schema trước để so) | BREAKING |
| `COMPLETENESS` | % ô trống > `max_null_pct` | BREAKING nếu ≥ `min_bad_rows` ô trống (mặc định 5), không thì WARNING |
| `CONFORMANCE` | sai định dạng số / giá trị ngoài `allowed_values` / không khớp `pattern` | BREAKING nếu > 1% dòng **và** ≥ `min_bad_rows` dòng, không thì WARNING |
| `PLAUSIBILITY` | giá trị ngoài `[min, max]` | như trên; giá trị ngoài khoảng được để trống khi chuẩn hóa |
| `NO_CONTRACT` | bảng chưa có hợp đồng | WARNING (vẫn đạt) |
| `PRIVACY_SENSITIVE_COLUMN` | bảng tầng ai_ready có cột định danh (cột `sensitive: true` ở bất kỳ hợp đồng nào) | BREAKING |
| `PRIVACY_UNDECLARED_COLUMN` | bảng tầng ai_ready có cột không khai báo trong hợp đồng | BREAKING |
| `PROVENANCE_UNVERIFIED` | file đầu vào không khớp checksum với phiên bản nào pipeline đã ghi (`provenance.verify_input`) | BREAKING |
| `PIPELINE_OUTPUT_DRIFT` | code pipeline đổi mà đầu ra lệch so với phiên bản trước (`pipeline_drift.check_pipeline_drift`) | BREAKING nếu cùng đầu vào, không thì WARNING |
| `SEMANTIC_MISMATCH` | nguồn định nghĩa một cột khác hợp đồng (`sources.check_source_semantics`) | WARNING |

**Vì sao có `min_bad_rows`:** chia 186 viện thành 3 đợt thì trung vị chỉ 4 bệnh nhân / lô. Lô 4 bệnh nhân thiếu
2 cân nặng đã là 50% > 40% → không có luật này thì 20 lô patient của dữ liệu thật bị chặn chỉ vì 1–3 ô lỗi.
Có luật này, trên dữ liệu thật chỉ còn 6 lô vital bị chặn, đều có nhiều dòng lỗi (viện 246 thiếu 64–100% nhịp tim;
3 lô có > 1% và ≥ 5 dòng nhịp tim < 20, phần lớn bằng 0).

`check_gate` trả `False` khi có ít nhất một lỗi BREAKING. Thêm cột mới chỉ là INFO (không ghi violations).

## 3. Người 1 gọi thế nào

```python
from governance.gates import check_gate
from governance.lineage import record_lineage

df = pd.read_csv(".../hospital_B/<snapshot>/patient.csv")
dat, loi = check_gate("raw_patient_hospital_B", df, "gate1")      # schema + hợp đồng
if dat:
    dat, loi = check_gate("raw_patient_hospital_B", df, "gate2")  # chất lượng
if not dat:
    print("Bị chặn:", *loi, sep="\n  ")       # KHÔNG đưa lên tầng standardized
else:
    ... # ghi dữ liệu lên tầng trên

# Khai báo lineage mỗi khi biến đổi cột (gọi lại nhiều lần không bị trùng)
record_lineage("raw_patient_hospital_B", "standardized_patients",
               {"admissionweight": "weight_kg", "patientunitstayid": "patient_id"})
record_lineage("standardized_patients", "ai_ready_icu_mortality",
               {"weight_kg": "weight_kg"}, transformation="chọn biến đầu vào AI")
```

Mọi hàm nhận thêm `db_path=None` (mặc định `metadata.db`). Muốn báo cáo chi tiết thì dùng
`check_gate_detailed(...)` (trả list dict: rule, column, severity, message, observed, expected) –
mỗi lô chỉ gọi **một trong hai** hàm vì cả hai đều ghi schema/violations.

## 4. Chạy

```bash
pytest governance/tests -q                       # 2 test dùng dữ liệu thật, tự skip nếu thiếu
python -m governance.demo_governance --reset     # demo, ghi vào metadata.db cho dashboard
python -m governance.demo_governance --fake --db /tmp/x.db   # demo bằng dữ liệu giả
python -m governance.violation_engine            # thống kê vi phạm trong metadata.db
```

Module: `contracts.py` (đọc YAML) · `schema_registry.py` (+ duyệt đổi tên cột) · `lineage.py` · `provenance.py`
(lần chạy, lô, phiên bản, checksum, phiên bản code) · `pipeline_drift.py` · `sources.py` (sổ đăng ký nguồn) ·
`privacy.py` (giả danh HMAC, luật tầng AI-ready) · `violation_engine.py` (+ vòng đời vi phạm) · `gates.py` ·
`sample_data.py` (dữ liệu giả, kể cả nguồn giả dạng Challenge 2012) · `demo_governance.py`.
