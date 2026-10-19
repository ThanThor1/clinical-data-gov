# Governance – Cổng kiểm tra, phát hiện vi phạm, lineage (Người 2)

## 1. Engine hoạt động thế nào

```
Người 1 gọi check_gate(ten_bang, df, "gate1" | "gate2")
   │
   ├─ contracts.find_contract(ten_bang)        tìm file contracts/*.yaml khớp tên bảng (hỗ trợ mẫu *)
   │
   ├─ gate1 (schema + hợp đồng)
   │    ├─ schema_registry.register_schema()   lưu schema_versions, so với phiên bản HỢP LỆ gần nhất
   │    │                                      → COLUMN_RENAMED / REMOVED / TYPE_CHANGED / ADDED
   │    └─ đối chiếu hợp đồng                  cột bắt buộc có mặt? đúng kiểu?
   ├─ gate2 (chất lượng – khung Kahn)
   │    ├─ Conformance   đúng kiểu số, giá trị thuộc allowed_values
   │    ├─ Completeness  % ô trống <= max_null_pct, cột nullable:false không trống
   │    └─ Plausibility  giá trị trong [min, max]
   │
   ├─ mỗi lỗi BREAKING/WARNING → violation_engine.report_violation()
   │    ├─ suy ra bệnh viện từ tên bảng (…_hospital_B → "Bệnh viện B")
   │    ├─ lineage.get_affected_ai_tables(bảng, cột)  → bảng AI-ready bị ảnh hưởng
   │    ├─ INSERT violations (action "đã chặn" / "cảnh báo")
   │    └─ log_event("violation_engine", loại_lỗi, bảng, BLOCKED | SUCCESS)
   │
   └─ log_event("governance_gate", cong, bảng, BLOCKED | SUCCESS)  → trả về (đạt?, [lỗi])
```

**Ai quyết định mức độ?** Schema registry chỉ *phát hiện* thay đổi; **hợp đồng** quyết định
mức độ: cột *bắt buộc* bị đổi tên/xóa/sai kiểu → BREAKING; cột ngoài hợp đồng → WARNING.
Một lô lỗi bị chặn được lưu lại (is_breaking = 1) nhưng **không trở thành chuẩn** cho lô sau.

## 2. Loại lỗi và mức độ

| Loại lỗi | Khi nào | Mức độ |
|---|---|---|
| `SCHEMA_COLUMN_RENAMED` | cột mất + cột mới cùng kiểu, phân phối/tên gần giống | BREAKING nếu cột bắt buộc, không thì WARNING |
| `SCHEMA_COLUMN_REMOVED` | cột biến mất | như trên |
| `SCHEMA_TYPE_CHANGED` | kiểu thực tế không hợp kiểu hợp đồng (vd float → chuỗi) | như trên |
| `MISSING_REQUIRED_COLUMN` | thiếu cột bắt buộc (khi chưa có schema trước để so) | BREAKING |
| `COMPLETENESS` | % ô trống > `max_null_pct` | BREAKING |
| `CONFORMANCE` | sai định dạng số / giá trị ngoài `allowed_values` | BREAKING nếu > 1% dòng, không thì WARNING |
| `PLAUSIBILITY` | giá trị ngoài `[min, max]` | BREAKING nếu > 1% dòng, không thì WARNING |
| `NO_CONTRACT` | bảng chưa có hợp đồng | WARNING (vẫn đạt) |

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
pytest governance/tests -q                       # 59 test (2 test dùng dữ liệu thật, tự skip nếu thiếu)
python -m governance.demo_governance --reset     # demo, ghi vào metadata.db cho dashboard
python -m governance.demo_governance --fake --db /tmp/x.db   # demo bằng dữ liệu giả
python -m governance.violation_engine            # thống kê vi phạm trong metadata.db
```

Module: `contracts.py` (đọc YAML) · `schema_registry.py` · `lineage.py` · `violation_engine.py` ·
`gates.py` · `sample_data.py` (dữ liệu giả + chuẩn hóa mẫu cho demo) · `demo_governance.py`.
