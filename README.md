# Quản trị dữ liệu lâm sàng đa bệnh viện (Clinical Data Governance)

Đề tài 5: *Trustworthy Clinical Data Governance and Provenance for Multi-Hospital AI-Ready Data*.

Hệ thống nhận dữ liệu ICU từ 3 bệnh viện, đưa qua 3 tầng **raw → standardized → AI-ready** và
kiểm soát dữ liệu ở mọi bước trước khi dữ liệu được dùng để huấn luyện mô hình dự đoán tử vong ICU:

| Mục tiêu của đề | Làm ở đâu |
|---|---|
| Metadata catalog | `catalog_access/catalog.py` – bảng `datasets` (tầng, chủ sở hữu, cột nhạy cảm, hợp đồng, vị trí file, số dòng) |
| Lineage / provenance | `governance/lineage.py` (cột → cột), `governance/provenance.py` (lần chạy, lô dữ liệu, checksum, phiên bản) |
| Data contract + quality gate | `contracts/*.yaml`, `governance/gates.py` (gate1: schema + hợp đồng + quyền riêng tư, gate2: Kahn) |
| Theo dõi thay đổi schema | `governance/schema_registry.py` – phát hiện đổi tên / xóa / đổi kiểu cột |
| Audit + phân quyền | `catalog_access/access.py`, `data_access.py`, `audit.py` – RBAC được thực thi khi đọc/ghi, sổ audit có chuỗi băm |
| Phát hiện vi phạm governance | `governance/violation_engine.py` – ghi vi phạm, chỉ ra bảng AI-ready bị ảnh hưởng và lô gây lỗi |

Dữ liệu dùng: **eICU-CRD Demo v2.0.1** (PhysioNet, truy cập tự do), 3 bảng `patient`, `lab`, `vitalPeriodic`.

---

## 1. Kiến trúc

```
data/01_landing (eICU gốc)
   │ split_hospitals: lấy 3 bệnh viện đông bệnh nhân nhất
   ▼
data/02_staging/hospital_{A,B,C}
   │ ingest_raw: chia mỗi viện thành 3 đợt gửi, đăng ký 9 bảng raw vào catalog
   ▼
data/raw/hospital_X/<thời điểm gửi>/{patient,lab,vitalPeriodic}.csv          ← tầng RAW
   │ standardize (tài khoản pipeline_service, đọc qua read_table)
   │   gate1: schema registry + hợp đồng      gate2: Conformance / Completeness / Plausibility
   │   mỗi lô: ghi provenance (file, SHA-256, số dòng, ACCEPTED / BLOCKED / SKIPPED)
   ▼
data/standardized/standardized_{patients,labs}.csv                            ← tầng STANDARDIZED
   │ build_ai_ready
   │   cổng provenance: file đầu vào phải khớp checksum một phiên bản pipeline đã ghi
   │   giả danh patient_id bằng HMAC-SHA256, thêm đặc trưng xét nghiệm
   │   gate1 (+ luật quyền riêng tư tầng AI-ready) và gate2
   ▼
data/ai_ready/ai_ready_icu_mortality.csv                                      ← tầng AI-READY

metadata.db (SQLite): datasets, schema_versions, lineage_edges, violations, audit_log,
                      pipeline_runs, batch_ingestions, dataset_versions, dataset_version_inputs
dashboard/app.py (Streamlit): Catalog · Truy cập dữ liệu · Lineage · Provenance · Vi phạm · Audit
```

Mức độ vi phạm: **BREAKING** chặn dữ liệu, **WARNING** cho qua nhưng ghi lại, **INFO** chỉ để biết.
Lỗi giá trị (sai định dạng, ngoài khoảng) chỉ BREAKING khi vượt 1% số dòng.

---

## 2. Cấu trúc thư mục

```
catalog_access/   catalog, phân quyền (policies.yaml), đọc/ghi có kiểm soát, sổ audit, khóa bí mật
governance/       hợp đồng, schema registry, lineage, provenance, quyền riêng tư, cổng kiểm tra, vi phạm
contracts/        hợp đồng dữ liệu YAML cho từng bảng (raw / standardized / ai_ready)
pipeline/         tách viện, chia đợt, nạp raw, chuẩn hóa, tạo AI-ready, gây lỗi giả lập
dashboard/        giao diện Streamlit
run_demo.py       chạy toàn bộ kịch bản demo bằng một lệnh
run_eval.py       đo số liệu đánh giá (dùng cho báo cáo)
*/tests/          kiểm thử (pytest)
```

---

## 3. Cài đặt

Cần Python 3.12 trở lên.

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**Dữ liệu:** tải eICU-CRD Demo tại https://physionet.org/content/eicu-crd-demo/2.0.1/, giải nén
`patient.csv.gz`, `lab.csv.gz`, `vitalPeriodic.csv.gz` và đặt vào `data/01_landing/`:

```
data/01_landing/patient.csv
data/01_landing/lab.csv
data/01_landing/vitalPeriodic.csv
```

Thư mục `data/` không được commit (xem `.gitignore`).

---

## 4. Chạy

### 4.1. Demo một lệnh

```bash
python run_demo.py --reset
```

Mất khoảng 10 giây, chạy 6 kịch bản và in ✅ / ❌ cho từng kịch bản:

1. Dữ liệu sạch của 3 viện đi qua 3 tầng, không có vi phạm BREAKING.
2. Viện B đổi tên cột `admissionweight → weight` ở đợt 3: bị chặn, chỉ ra bảng `ai_ready_icu_mortality`
   bị ảnh hưởng, audit có dòng BLOCKED, provenance ghi lô đó BLOCKED (lab / vital cùng đợt SKIPPED).
3. Viện B sửa lại dữ liệu → chạy lại → qua cổng.
4. Nhà nghiên cứu đọc bảng raw bị từ chối (DENIED); chuyên viên phân tích đọc standardized nhưng
   cột `patient_id` bị ẩn.
5. Truy ngược bảng AI-ready về 27 lô nguồn (kèm checksum); sửa tay file standardized → tạo AI-ready bị chặn.
6. Sổ audit toàn vẹn; sửa một dòng trên bản sao DB → bị phát hiện.

`--reset` xóa `metadata.db` cũ trước khi chạy. Bỏ `--reset` thì chạy tiếp trên DB hiện có.

### 4.2. Chạy từng bước

```bash
python -m pipeline.split_hospitals        # data/01_landing -> data/02_staging
python -m pipeline.ingest_raw             # chia 3 đợt -> data/raw, đăng ký catalog
python -m pipeline.standardize            # raw -> standardized
python -m pipeline.build_ai_ready         # standardized -> ai_ready

# Gây lỗi giả lập cho đợt 3 rồi chạy lại standardize để xem bị chặn
python -m pipeline.inject_errors --loi doi_ten          # doi_ten | doi_kieu | xoa_cot | nhip_tim_500 | thieu_can_nang
python -m pipeline.inject_errors --sua-lai --vien B     # khôi phục đợt 3 của viện B
```

Xem nhanh từ dòng lệnh:

```bash
python -m governance.violation_engine     # thống kê vi phạm
python -m governance.provenance           # cây nguồn gốc của bảng AI-ready mới nhất + kiểm tra checksum
python -m catalog_access.access           # thử phân quyền
```

### 4.3. Dashboard

```bash
streamlit run dashboard/app.py            # http://localhost:8501
```

Chọn tài khoản ở thanh bên (`engineer_alice`, `analyst_dave`, `researcher_bob`, `guest_charlie`).
Trang **Truy cập dữ liệu** đọc bảng thật qua `read_table` nên kết quả (được phép / bị từ chối / bị ẩn cột)
và dòng audit giống hệt khi đọc từ code.

### 4.4. Đánh giá

```bash
python run_eval.py                        # in các bảng kết quả
python run_eval.py --out ket_qua_eval     # ghi thêm CSV + tong_hop.json
```

Chạy trên thư mục tạm và DB tạm, không đụng `data/` hay `metadata.db` thật.

### 4.5. Kiểm thử

```bash
pytest                                    # 178 test; test dashboard tự bỏ qua nếu chưa cài streamlit
```

Test dùng DB tạm và khóa tạm, không cần dữ liệu thật (riêng `governance/tests/test_real_data.py`
tự bỏ qua nếu chưa có `data/02_staging`).

---

## 5. Triển khai bằng Docker

```bash
CODE_VERSION=$(git describe --always --dirty) docker compose build
docker compose run --rm pipeline                         # = python run_demo.py --reset
docker compose run --rm pipeline python run_eval.py      # đánh giá
docker compose up -d dashboard                           # http://localhost:8501
```

- `data/` của máy được gắn vào container (cần có `data/01_landing` như mục 3).
- `metadata.db` và các khóa bí mật nằm trong volume `state`, dùng chung cho pipeline và dashboard,
  không nằm trong ảnh.
- `CODE_VERSION` được ghi vào provenance của mỗi lần chạy (trong container không có git).
- Container chạy bằng người dùng thường (uid 1000), có health check `/_stcore/health`.

---

## 6. Cấu hình

### 6.1. Biến môi trường

| Biến | Ý nghĩa | Mặc định |
|---|---|---|
| `CLINICAL_GOV_DB` | Đường dẫn metadata.db | `metadata.db` ở gốc repo |
| `PATIENT_HASH_SECRET` | Khóa HMAC giả danh bệnh nhân (≥ 16 ký tự) | đọc từ file khóa |
| `PATIENT_HASH_KEY_FILE` | File khóa giả danh | `.secrets/patient_hash.key` |
| `AUDIT_LOG_SECRET` | Khóa HMAC chuỗi băm sổ audit (≥ 16 ký tự) | đọc từ file khóa |
| `AUDIT_LOG_KEY_FILE` | File khóa sổ audit | `.secrets/audit_log.key` |
| `CODE_VERSION` | Phiên bản code khi không có git | rỗng |

Chưa có file khóa thì hệ thống tự sinh khóa ngẫu nhiên 256 bit (quyền 600). Thư mục `.secrets/`
nằm trong `.gitignore`, **không commit**.

### 6.2. Phân quyền – `catalog_access/policies.yaml`

| Vai trò | raw | standardized | ai_ready | Cột nhạy cảm | Sổ audit |
|---|---|---|---|---|---|
| `data_engineer` (Kỹ sư dữ liệu) | đọc + ghi | đọc + ghi | đọc + ghi | thấy | xem |
| `analyst` (Chuyên viên phân tích) | ⛔ | đọc | đọc | **bị ẩn** | ⛔ |
| `researcher` (Nhà nghiên cứu) | ⛔ | ⛔ | đọc | bị ẩn | ⛔ |
| `guest` (Khách) | ⛔ | ⛔ | ⛔ | – | ⛔ |

Thêm người dùng: thêm một dòng `ten_dang_nhap: vai_tro` vào mục `users`. Người không có trong danh sách
nhận `default_role: guest` (mặc định từ chối). Pipeline chạy bằng tài khoản `pipeline_service`.

### 6.3. Hợp đồng dữ liệu – `contracts/*.yaml`

Mỗi file áp dụng cho một hoặc nhiều bảng (`applies_to`, cho phép mẫu `*`). Mỗi cột có thể khai báo:

| Trường | Ý nghĩa |
|---|---|
| `type` | `int` / `float` / `string` / `category` |
| `required` | cột bắt buộc: thiếu, đổi tên, xóa, sai kiểu → BREAKING |
| `nullable`, `max_null_pct` | có được trống không, % trống tối đa (Completeness) |
| `min`, `max` | khoảng hợp lý (Plausibility) |
| `allowed_values`, `pattern` | tập giá trị / biểu thức chính quy (Conformance) |
| `sensitive` | cột định danh: bị ẩn với vai trò không được xem, không được xuất hiện ở tầng AI-ready |
| `description` | mô tả, hiện trong catalog |

Hợp đồng có `tier: ai_ready` được áp thêm luật quyền riêng tư: bảng chỉ được chứa đúng các cột đã
khai báo và không được có cột nào là `sensitive` ở bất kỳ hợp đồng nào.

---

## 7. Vận hành

**Đọc dữ liệu từ code** (luôn đi qua lớp kiểm soát quyền):

```python
from catalog_access import read_table, AccessDenied

df = read_table("researcher_bob", "ai_ready_icu_mortality")      # được phép
try:
    read_table("researcher_bob", "raw_patient_hospital_B")       # bị từ chối, audit ghi DENIED
except AccessDenied as loi:
    print(loi.reason)
```

**Khi có vi phạm BREAKING:** mở dashboard trang *Vi phạm* → xem bảng, cột, lô gây lỗi
(`hospital_X/<thời điểm gửi>`) và bảng AI-ready bị ảnh hưởng → báo bệnh viện gửi lại → chạy lại pipeline.
Lô bị chặn không bao giờ lên tầng trên; schema của lô lỗi không trở thành chuẩn cho các lô sau.

**Kiểm tra định kỳ:**
- Toàn vẹn sổ audit: trang *Audit* trên dashboard, hoặc `verify_audit_chain()`.
- Nguồn gốc dữ liệu AI-ready: trang *Provenance* → *Kiểm tra lại checksum*, hoặc `python -m governance.provenance`.

**Sao lưu:** sao lưu cùng lúc `metadata.db` và `.secrets/`. Mất khóa sổ audit thì không xác minh được
sổ cũ; đổi khóa giả danh thì mã `patient_hash` của các lần chạy sau sẽ khác trước.

---

## 8. Kết quả đánh giá (`python run_eval.py`)

Chạy trên dữ liệu eICU demo của 3 bệnh viện (95 bệnh nhân, khoảng 28 nghìn dòng raw):

| Phép đo | Kết quả |
|---|---|
| Dữ liệu sạch | 0 BREAKING (không chặn oan), 1 WARNING (1 dòng nhịp tim = 19 ở viện C) |
| 5 loại lỗi giả lập – có governance | phát hiện đúng loại 5/5, chặn đúng lô 5/5, chỉ ra bảng AI-ready 5/5, **0 dòng nhiễm bẩn** |
| 5 loại lỗi – bỏ governance | 2 lỗi làm pipeline sập (đổi tên, xóa cột), 3 lỗi lọt vào AI-ready (9 và 10 dòng sai giá trị, 3 dòng mất cân nặng) |
| Hiệu năng gate1 + gate2 | ~1,7 triệu dòng/giây ở 1 triệu dòng sinh hiệu |
| Phân quyền | 24/24 ô (vai trò × tầng × đọc/ghi) đúng thiết kế; người lạ bị từ chối; cột định danh bị ẩn với analyst |
| Sổ audit | phát hiện 4/4 kiểu can thiệp: sửa dòng, xóa dòng giữa, chèn dòng giả, xóa dòng cuối |
| Provenance | truy ngược AI-ready về 27 lô (30 file khớp checksum); chặn file standardized sửa tay; phát hiện file raw bị sửa sau khi nạp |

(Thời gian chạy có thể chênh tùy máy.)

---

## 9. Hạn chế

- Đăng nhập trên dashboard chỉ là chọn tài khoản, chưa có xác thực mật khẩu / SSO.
- SQLite + CSV đủ cho quy mô demo; dữ liệu lớn nên chuyển sang PostgreSQL và Parquet.
- Người có **cả** khóa sổ audit và quyền ghi DB vẫn có thể ghi lại toàn bộ chuỗi băm. Nên định kỳ lưu
  băm dòng cuối (`hash_cuoi`) ra một nơi khác để đối chiếu.
- 3 "bệnh viện" được cắt từ cùng bộ eICU demo nên khác biệt giữa các viện chủ yếu đến từ lỗi giả lập.

---

## 10. Phân công

- **Người 1 – Pipeline dữ liệu:** `pipeline/`
- **Người 2 – Governance:** `governance/`, `contracts/`
- **Người 3 – Catalog, phân quyền, audit, dashboard:** `catalog_access/`, `dashboard/`
