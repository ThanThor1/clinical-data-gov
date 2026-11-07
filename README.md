# Quản trị dữ liệu lâm sàng đa bệnh viện (Clinical Data Governance)

Đề tài 5: *Trustworthy Clinical Data Governance and Provenance for Multi-Hospital AI-Ready Data*.

Hệ thống nhận dữ liệu ICU từ **hai hệ thống nguồn khác hẳn nhau**: 186 bệnh viện của eICU-CRD Demo (CSV dạng bảng)
và PhysioNet/CinC Challenge 2012 (4.000 lượt nằm ICU, dạng dài "thời gian, tham số, giá trị", mã hóa và định nghĩa
nhãn khác) – tổng khoảng 2,5 triệu dòng. Dữ liệu được xử lý bằng **Apache Spark**, lưu tầng standardized / AI-ready
thành bảng **Delta Lake**, và được kiểm soát ở mọi bước trước khi dùng cho AI (bài toán mẫu: dự đoán tử vong ICU;
nhóm chỉ chuẩn bị dữ liệu AI-ready, không huấn luyện mô hình).

| Mục tiêu của đề | Làm ở đâu |
|---|---|
| Metadata catalog cho nguồn không đồng nhất | Sổ đăng ký nguồn `sources/*.yaml` (định dạng gốc, giấy phép, ánh xạ về schema chuẩn) + bộ chuyển `pipeline/source_adapters.py`; `catalog_access/catalog.py` – 564 bảng (561 bảng raw của 187 nguồn gửi + standardized + AI-ready): tầng, hệ thống nguồn, định dạng gốc, chủ sở hữu, cột nhạy cảm, hợp đồng, vị trí, số dòng |
| Lineage / provenance | `governance/lineage.py` (cột → cột, từ **tham số gốc của nguồn** tới AI-ready; sinh từ file ánh xạ), `governance/provenance.py` (lần chạy, lô dữ liệu, checksum, phiên bản Delta, phiên bản code) |
| Data contract + quality gate | `contracts/*.yaml`, `governance/gates.py` (gate1: schema + hợp đồng + quyền riêng tư, gate2: Kahn) trên hồ sơ dữ liệu tính bằng Spark (`governance/profiling.py`) |
| Theo dõi thay đổi schema và phiên bản | `governance/schema_registry.py` – phát hiện đổi tên / xóa / đổi kiểu cột theo từng viện; kỹ sư dữ liệu **duyệt** đổi tên cột hợp lệ (`approve_rename`) thay cho việc bắt bệnh viện gửi lại; phiên bản bảng Delta + phiên bản code của từng lần chạy |
| Audit + phân quyền | `catalog_access/access.py`, `data_access.py`, `audit.py` – RBAC được thực thi khi đọc/ghi (pandas và Spark), sổ audit có chuỗi băm HMAC; dashboard đăng nhập bằng mật khẩu băm PBKDF2 (`catalog_access/auth.py`) |
| Phát hiện vi phạm do **pipeline hoặc schema** thay đổi | `governance/violation_engine.py` – ghi vi phạm, chỉ ra bảng AI-ready bị ảnh hưởng và lô gây lỗi, vòng đời OPEN → RESOLVED / SUPERSEDED; `governance/pipeline_drift.py` – code pipeline đổi mà đầu ra lệch (cùng đầu vào) → chặn; nguồn định nghĩa cột khác hợp đồng → cảnh báo `SEMANTIC_MISMATCH` |

---

## 1. Kiến trúc

```
data/01_landing/                       (bản gốc, KHÔNG sửa)
   ├─ patient.csv, lab.csv, vitalPeriodic.csv        eICU demo: CSV dạng bảng, 2,07 triệu dòng, 186 viện
   └─ challenge2012/set-a.tar.gz, Outcomes-a.txt     Challenge 2012: dạng dài Time,Parameter,Value, 4.000 lượt
   │ split_hospitals (eICU) + source_adapters (nguồn khác chuẩn, theo sources/*.yaml: đổi tên tham số,
   │ đổi mã 0/1, -1 → trống, hh:mm → phút) → cùng schema chuẩn; split_snapshots: mỗi nguồn gửi 3 đợt
   ▼
data/raw/hospital_<mã>/<thời điểm gửi>/{patient,lab,vitalPeriodic}.csv      ← tầng RAW: 1.683 file, giữ nguyên
   │ standardize  (Apache Spark, tài khoản pipeline_service)
   │   đổi tên cột đã được kỹ sư dữ liệu DUYỆT (schema_approvals) khi đọc; cảnh báo nguồn định nghĩa khác
   │   1 job Spark / loại dữ liệu: hồ sơ chất lượng của MỌI lô cùng lúc (groupBy theo file)
   │   – file gom theo dòng tiêu đề (Spark đọc lẫn nhiều CSV sẽ khớp cột theo vị trí)
   │   – hồ sơ lưu theo checksum: lần sau chỉ tính file mới / bị sửa (xử lý tăng dần)
   │   gate1 + gate2 trên hồ sơ từng lô → ACCEPTED / BLOCKED / SKIPPED, ghi provenance từng lô
   │   chỉ lô đã qua cổng được chuẩn hóa; giá trị ngoài khoảng hợp lý → để trống
   │   trước khi ghi: code pipeline đổi mà đầu ra lệch (cùng đầu vào) → chặn (pipeline_drift)
   ▼
data/standardized/standardized_{patients,labs}/      (Delta Lake)            ← tầng STANDARDIZED
   │ build_ai_ready  (Apache Spark)
   │   cổng provenance: bảng đầu vào phải khớp checksum một phiên bản pipeline đã ghi
   │   giả danh patient_id bằng HMAC-SHA256 (hàm có sẵn của Spark), đặc trưng xét nghiệm (groupBy)
   │   gate1 (+ luật quyền riêng tư tầng AI-ready), gate2, kiểm tra lệch đầu ra khi code đổi
   ▼
data/ai_ready/ai_ready_icu_mortality/                 (Delta Lake)            ← tầng AI-READY

metadata.db (SQLite, chế độ WAL): datasets, schema_versions, schema_approvals, lineage_edges, violations,
             audit_log, pipeline_runs, batch_ingestions, batch_profiles, dataset_versions,
             dataset_version_inputs, code_approvals
dashboard/app.py (Streamlit, đăng nhập bằng mật khẩu): Catalog · Truy cập dữ liệu · Lineage · Provenance ·
             Vi phạm (kèm duyệt đổi tên cột) · Audit
Spark chạy local[*] trên một máy, hoặc trên cụm Spark standalone (1 master + 2 worker) bằng docker-compose.
```

Mức độ vi phạm: **BREAKING** chặn lô, **WARNING** cho qua nhưng ghi lại, **INFO** chỉ để biết.
Trạng thái vi phạm: **OPEN** → **RESOLVED** (lô / bảng qua cổng ở lần chạy sau) hoặc **SUPERSEDED** (lần chạy sau
ghi lại đúng lỗi đó), tự cập nhật khi mỗi lần chạy kết thúc.
Lỗi theo tỷ lệ chỉ BREAKING khi vượt ngưỡng (1% dòng, hoặc `max_null_pct` với ô trống) **và** có ít nhất
`min_bad_rows` (mặc định 5) dòng lỗi – lô chỉ vài bệnh nhân không bị chặn oan vì 1–2 ô trống.

Vì sao tầng standardized / AI-ready không chia phân vùng theo bệnh viện: mỗi viện chỉ có vài nghìn dòng, chia
186 phân vùng sẽ sinh hàng trăm file Parquet rất nhỏ (small files problem) và đọc chậm hơn. Các bảng được gom
thành ít file (khoảng 1 triệu dòng / file); Delta Lake vẫn lưu thống kê min/max từng file để bỏ qua file khi lọc.

---

## 2. Cấu trúc thư mục

```
catalog_access/   catalog, phân quyền (policies.yaml), đọc/ghi có kiểm soát (pandas + Spark), sổ audit, khóa bí mật
governance/       hợp đồng, hồ sơ dữ liệu (pandas / Spark), schema registry, lineage, provenance, quyền riêng tư,
                  cổng kiểm tra, vi phạm
contracts/        hợp đồng dữ liệu YAML cho từng bảng (raw / standardized / ai_ready); hợp đồng raw = schema chuẩn
sources/          sổ đăng ký nguồn: mỗi hệ thống nguồn một file YAML (định dạng gốc, giấy phép, ánh xạ về schema chuẩn)
pipeline/         SparkSession, tách viện, bộ chuyển nguồn, chia đợt, nạp raw, chuẩn hóa (Spark), tạo AI-ready (Spark),
                  gây lỗi giả lập (lỗi dữ liệu và lỗi pipeline)
dashboard/        giao diện Streamlit
run_demo.py       chạy toàn bộ kịch bản demo bằng một lệnh
run_eval.py       đo số liệu đánh giá (dùng cho báo cáo)
*/tests/          kiểm thử (pytest)
```

---

## 3. Cài đặt

Cần **Python 3.12+** và **Java 17+** (Spark chạy trên JVM; Ubuntu: `sudo apt install openjdk-17-jre-headless`).

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt    # có pyspark 4.2.0 + delta-spark 4.4.1
```

Lần đầu khởi động Spark sẽ tải jar Delta Lake từ Maven (khoảng 90 MB, lưu ở `~/.ivy2.5.2`), cần mạng một lần.
Đã chạy thử với Java 17 (trong ảnh Docker) và Java 25, Python 3.12 và 3.13.

**RAM:** chạy demo đầy đủ (eICU + Challenge 2012) cần khoảng **4 GB** trống (đo được 3,7 GB), chủ yếu là JVM của
Spark (driver mặc định 2g). Máy ít RAM thì đặt `SPARK_DRIVER_MEMORY=1g` (giảm khoảng 1 GB, demo vẫn chạy hết) hoặc
giảm số core, ví dụ `SPARK_MASTER=local[4]`.

**Dữ liệu:** tải eICU-CRD Demo tại https://physionet.org/content/eicu-crd-demo/2.0.1/, giải nén
`patient.csv.gz`, `lab.csv.gz`, `vitalPeriodic.csv.gz` và đặt vào `data/01_landing/`:

```
data/01_landing/patient.csv
data/01_landing/lab.csv
data/01_landing/vitalPeriodic.csv
```

**Nguồn thứ hai (không bắt buộc, nên có để demo nguồn không đồng nhất):** tải 2 file của
[PhysioNet/CinC Challenge 2012](https://physionet.org/content/challenge-2012/1.0.0/) (giấy phép ODC-By 1.0,
không cần đăng ký), KHÔNG cần giải nén:

```bash
mkdir -p data/01_landing/challenge2012 && cd data/01_landing/challenge2012
curl -LO https://physionet.org/files/challenge-2012/1.0.0/set-a.tar.gz
curl -LO https://physionet.org/files/challenge-2012/1.0.0/Outcomes-a.txt
```

Chưa có hai file này thì pipeline vẫn chạy với riêng eICU (bộ chuyển báo "Bỏ qua nguồn challenge2012").

Thư mục `data/` không được commit (xem `.gitignore`). Khi dùng dữ liệu, ghi nguồn theo giấy phép: eICU-CRD Demo
(ODbL 1.0), Challenge 2012 (ODC-By 1.0).

---

## 4. Chạy

### 4.1. Demo một lệnh

```bash
python run_demo.py --reset            # 186 viện eICU + Challenge 2012, khoảng 3 phút
python run_demo.py --reset --top 3    # chạy nhanh với 3 viện eICU đông bệnh nhân nhất (+ Challenge 2012 nếu có)
```

Chạy 8 kịch bản và in ✅ / ❌ cho từng kịch bản:

1. Dữ liệu thật của 187 nguồn gửi đi qua 3 tầng. Các lô kém chất lượng **có sẵn** trong eICU bị chặn đúng lô
   (6 lô vital: viện 246 thiếu 64–100% nhịp tim, 3 lô có nhiều nhịp tim < 20); 6.520 bệnh nhân lên AI-ready
   (2.520 eICU + 4.000 Challenge 2012).
   **1b.** Nguồn không đồng nhất: Challenge 2012 (dạng dài, giới tính 0/1, -1 là thiếu, thời gian hh:mm) được bộ
   chuyển đưa về schema chuẩn theo `sources/challenge2012.yaml`; lineage đi từ tham số gốc
   (`nguon_challenge2012.Weight → raw_patient_hospital_c2012.admissionweight → … → ai_ready_icu_mortality.weight_kg`);
   cảnh báo `SEMANTIC_MISMATCH` vì nhãn của nguồn này là tử vong trong **bệnh viện**, không phải lúc ra ICU.
2. Viện 123 (đông bệnh nhân thứ 2 của eICU) đổi tên cột `admissionweight → weight` ở đợt 3: bị chặn, chỉ ra bảng
   `ai_ready_icu_mortality` bị ảnh hưởng, audit có dòng BLOCKED, provenance ghi lô BLOCKED (lab / vital cùng đợt SKIPPED).
3. Kỹ sư dữ liệu **duyệt** thay đổi đó (`weight` chính là `admissionweight`) → chạy lại → lô qua cổng mà bệnh viện
   không phải gửi lại; vi phạm của kịch bản 2 chuyển RESOLVED; provenance của lô ghi ánh xạ đã duyệt; lineage có cạnh
   từ cột `weight`.
4. Nhà nghiên cứu đọc bảng raw bị từ chối (DENIED); chuyên viên phân tích đọc standardized nhưng cột
   `patient_id` bị ẩn.
5. Truy ngược bảng AI-ready về 1.677 lô nguồn của 187 nguồn gửi (kèm checksum); một job Spark ngoài pipeline ghi đè
   bảng standardized → tạo AI-ready bị chặn; lịch sử phiên bản Delta; phiên bản cũ vẫn kiểm tra được nhờ time travel.
6. Sổ audit toàn vẹn; sửa một dòng trên bản sao DB → bị phát hiện.
7. Pipeline đổi code (giả lập lỗi: chuẩn hóa cân nặng sang pound). Dữ liệu sai vẫn gần như nằm trong khoảng hợp lý
   nên cổng hợp đồng không chặn; bộ phát hiện lệch đầu ra thấy cùng đầu vào mà cân nặng trung bình +120% → chặn
   (`PIPELINE_OUTPUT_DRIFT`), tầng AI-ready giữ nguyên; sửa code chạy lại → vi phạm RESOLVED.

`--reset` xóa `metadata.db` cũ trước khi chạy. Bỏ `--reset` thì chạy tiếp trên DB hiện có.
Trong lúc chạy, giao diện Spark ở http://localhost:4040.

### 4.2. Chạy từng bước

```bash
python -m pipeline.split_hospitals        # data/01_landing -> data/02_staging (--top k: chỉ k viện)
python -m pipeline.source_adapters        # nguồn khác chuẩn (Challenge 2012) -> data/02_staging/hospital_c2012
python -m pipeline.ingest_raw             # chia 3 đợt -> data/raw, đăng ký catalog
python -m pipeline.standardize            # raw -> standardized (Spark + Delta)
python -m pipeline.build_ai_ready         # standardized -> ai_ready (Spark + Delta)

# Gây lỗi giả lập cho đợt 3 rồi chạy lại standardize để xem bị chặn
python -m pipeline.inject_errors --loi doi_ten               # doi_ten | doi_kieu | xoa_cot | nhip_tim_500 | thieu_can_nang
python -m pipeline.inject_errors --loi xoa_cot --vien 264    # chỉ định viện
python -m pipeline.inject_errors --sua-lai --vien 123        # khôi phục đợt 3 của viện 123

# Lỗi do PIPELINE (code biến đổi sai, dữ liệu nguồn đúng): can_nang_pound | dao_nhan_tu_vong
PIPELINE_LOI_GIA_LAP=can_nang_pound python -m pipeline.standardize       # bị chặn: PIPELINE_OUTPUT_DRIFT
python -m governance.pipeline_drift --duyet <phiên bản code> --nguoi engineer_alice --ly-do "cố ý đổi đầu ra"
```

Duyệt đổi tên cột (thay cho việc bắt bệnh viện gửi lại): trên dashboard, trang **Vi phạm** → "Duyệt thay đổi
schema", hoặc từ code:

```python
from governance.schema_registry import approve_rename
approve_rename("engineer_alice", "raw_patient_hospital_123", "weight", "admissionweight", "đổi tên hợp lệ")
```

Xem nhanh từ dòng lệnh:

```bash
python -m governance.violation_engine     # thống kê vi phạm
python -m governance.provenance           # cây nguồn gốc của bảng AI-ready mới nhất + kiểm tra checksum
python -m catalog_access.access           # thử phân quyền
python -m governance.sources              # sổ đăng ký nguồn và ánh xạ về schema chuẩn
```

### 4.3. Dashboard

```bash
streamlit run dashboard/app.py            # http://localhost:8501
```

Đăng nhập bằng tài khoản + mật khẩu (mỗi lần đăng nhập, đúng hay sai, đều ghi vào sổ audit). Tài khoản demo:

| Tài khoản | Mật khẩu | Vai trò |
|---|---|---|
| `engineer_alice` | `alice@2026` | Kỹ sư dữ liệu (xem mọi tầng, sổ audit, duyệt đổi tên cột) |
| `analyst_dave` | `dave@2026` | Chuyên viên phân tích (cột định danh bị ẩn) |
| `researcher_bob` | `bob@2026` | Nhà nghiên cứu (chỉ tầng AI-ready) |
| `guest_charlie` | `charlie@2026` | Khách (chỉ catalog) |

Đây là mật khẩu demo, **đổi khi triển khai thật**: `python -m catalog_access.auth "<mật khẩu mới>"` rồi dán chuỗi
băm vào mục `passwords` của `catalog_access/policies.yaml`.
Trang **Truy cập dữ liệu** đọc bảng thật qua `read_table` nên kết quả (được phép / bị từ chối / bị ẩn cột)
và dòng audit giống hệt khi đọc từ code. Dashboard đọc bảng Delta bằng thư viện `deltalake`, không cần Java.

### 4.4. Đánh giá

```bash
python run_eval.py                        # đầy đủ, khoảng 13–15 phút
python run_eval.py --nhanh                # bỏ phép đo theo số core / số viện
python run_eval.py --out ket_qua_eval     # ghi thêm CSV + tong_hop.json
```

Chạy trên thư mục tạm và DB tạm, không đụng `data/` hay `metadata.db` thật.

### 4.5. Kiểm thử

```bash
pytest                    # toàn bộ (test Spark mất vài phút)
pytest -m "not spark"     # chỉ các test không cần Spark (vài giây)
```

Test dùng DB tạm, khóa tạm và dữ liệu giả, không cần dữ liệu thật (riêng `governance/tests/test_real_data.py`
tự bỏ qua nếu chưa có `data/02_staging`). Máy chưa có Java / pyspark thì test Spark tự bỏ qua.

---

## 5. Triển khai bằng Docker (cụm Spark)

```bash
CODE_VERSION=$(git describe --always --dirty) docker compose build

# a) Spark chạy ngay trong container pipeline (local[*])
docker compose run --rm pipeline                         # = python run_demo.py --reset

# b) Spark chạy trên cụm: 1 master + 2 worker
docker compose --profile cluster up -d                   # giao diện master: http://localhost:8080
docker compose run --rm -e SPARK_MASTER=spark://spark-master:7077 pipeline
docker compose --profile cluster down

docker compose run --rm pipeline python run_eval.py      # đánh giá
docker compose up -d dashboard                           # http://localhost:8501
```

- Mọi container dùng **một ảnh** (Spark 4.2.0, Delta Lake 4.4.1, Java 17, Python 3.12) nên các node giống hệt nhau;
  jar Delta Lake được tải sẵn vào ảnh.
- `data/` của máy được gắn vào mọi container ở cùng đường dẫn `/app/data` (thay cho HDFS / S3 khi chạy trên một
  máy); cần có `data/01_landing` như mục 3.
- `metadata.db` và các khóa bí mật nằm trong volume `state`, không nằm trong ảnh.
- Bộ nhớ / số core mỗi worker: `SPARK_WORKER_MEMORY` (mặc định 1g), `SPARK_WORKER_CORES` (mặc định 2).
- Container chạy bằng người dùng thường (uid 1000).
- Chạy trong container chậm hơn chạy thẳng trên máy, khoảng gấp đôi (đo trên cùng laptop 12 core).

---

## 6. Cấu hình

### 6.1. Biến môi trường

| Biến | Ý nghĩa | Mặc định |
|---|---|---|
| `SPARK_MASTER` | `local[*]` (một máy) hoặc `spark://<master>:7077` (cụm) | `local[*]` |
| `SPARK_DRIVER_MEMORY` / `SPARK_EXECUTOR_MEMORY` | bộ nhớ driver / executor | `2g` / `1g` |
| `SPARK_SHUFFLE_PARTITIONS` | số phân vùng khi groupBy / join | `8` |
| `SPARK_UI` | `1` bật giao diện Spark ở cổng 4040, `0` tắt | `1` |
| `SPARK_DRIVER_HOST` | địa chỉ driver cho executor gọi lại khi chạy cụm (`auto` = IP máy) | – |
| `CLINICAL_GOV_DB` | Đường dẫn metadata.db | `metadata.db` ở gốc repo |
| `PATIENT_HASH_SECRET` / `PATIENT_HASH_KEY_FILE` | Khóa HMAC giả danh (≥ 16 ký tự) / file khóa | `.secrets/patient_hash.key` |
| `AUDIT_LOG_SECRET` / `AUDIT_LOG_KEY_FILE` | Khóa HMAC sổ audit (≥ 16 ký tự) / file khóa | `.secrets/audit_log.key` |
| `CODE_VERSION` | Phiên bản code khi không có git (luôn kèm băm nội dung mã nguồn pipeline) | rỗng |
| `PIPELINE_LOI_GIA_LAP` | Giả lập lỗi pipeline cho một lần chạy: `can_nang_pound`, `dao_nhan_tu_vong` | rỗng |

Chưa có file khóa thì hệ thống tự sinh khóa ngẫu nhiên 256 bit (quyền 600). Thư mục `.secrets/`
nằm trong `.gitignore`, **không commit**.

### 6.2. Phân quyền – `catalog_access/policies.yaml`

| Vai trò | raw | standardized | ai_ready | Cột nhạy cảm | Sổ audit |
|---|---|---|---|---|---|
| `data_engineer` (Kỹ sư dữ liệu) | đọc + ghi | đọc + ghi | đọc + ghi | thấy | xem |
| `analyst` (Chuyên viên phân tích) | ⛔ | đọc | đọc | **bị ẩn** | ⛔ |
| `researcher` (Nhà nghiên cứu) | ⛔ | ⛔ | đọc | bị ẩn | ⛔ |
| `guest` (Khách) | ⛔ | ⛔ | ⛔ | – | ⛔ |

Thêm người dùng: thêm một dòng `ten_dang_nhap: vai_tro` vào mục `users`, và băm mật khẩu vào mục `passwords`
(`python -m catalog_access.auth "<mật khẩu>"`) nếu người đó cần đăng nhập dashboard. Vai trò chỉ tra theo tài
khoản trong `users`: gõ tên vai trò (ví dụ `data_engineer`) không được nhận quyền của vai trò đó. Người không có
trong danh sách nhận `default_role: guest` (mặc định từ chối). Pipeline chạy bằng tài khoản `pipeline_service`
(không có mật khẩu nên không đăng nhập dashboard được).

### 6.3. Hợp đồng dữ liệu – `contracts/*.yaml`

Mỗi file áp dụng cho một hoặc nhiều bảng (`applies_to`, cho phép mẫu `*`, ví dụ `raw_patient_hospital_*`
cho mọi viện, kể cả nguồn Challenge 2012 sau khi ánh xạ). Mỗi cột có thể khai báo:

| Trường | Ý nghĩa |
|---|---|
| `type` | `int` / `float` / `string` / `category` |
| `required` | cột bắt buộc: thiếu, đổi tên, xóa, sai kiểu → BREAKING |
| `nullable`, `max_null_pct` | có được trống không, % trống tối đa (Completeness) |
| `min`, `max` | khoảng hợp lý (Plausibility); giá trị ngoài khoảng bị để trống khi chuẩn hóa |
| `allowed_values`, `pattern` | tập giá trị / biểu thức chính quy (Conformance) |
| `sensitive` | cột định danh: bị ẩn với vai trò không được xem, không được xuất hiện ở tầng AI-ready |
| `semantics` | định nghĩa của cột (ví dụ `icu_discharge_status`); nguồn khai báo khác → cảnh báo `SEMANTIC_MISMATCH` |
| `description` | mô tả, hiện trong catalog |

Ở mức hợp đồng: `tier` (raw / standardized / ai_ready) và `min_bad_rows` (mặc định 5). Hợp đồng có
`tier: ai_ready` được áp thêm luật quyền riêng tư: bảng chỉ được chứa đúng các cột đã khai báo và không được
có cột nào là `sensitive` ở bất kỳ hợp đồng nào. Các hợp đồng `raw_*` cũng là **schema chuẩn** mà mọi nguồn phải
được ánh xạ về.

### 6.4. Sổ đăng ký nguồn – `sources/*.yaml`

Mỗi hệ thống nguồn một file (`python -m governance.sources` để xem). Đúng một nguồn có `hospitals: default`
(eICU, đã đúng schema chuẩn); nguồn khác liệt kê mã viện của nó.

| Trường | Ý nghĩa |
|---|---|
| `name`, `system`, `license`, `url` | tên, mô tả, giấy phép, nơi tải – hiện trong catalog |
| `format` | `csv_wide` (đúng schema chuẩn) hoặc `eav_long` (dạng dài, cần bộ chuyển) |
| `landing`, `files` | thư mục và các file gốc (bộ chuyển đọc thẳng `.tar.gz`, không giải nén) |
| `missing_value`, `id_parameter`, `time_column` | giá trị "không có" của nguồn, tham số mã lượt nằm ICU, cột thời gian `hh:mm` |
| `patient`, `vital`, `lab` | ánh xạ: cột chuẩn ← tham số gốc (`from`), đổi mã (`values`), luật lấy giá trị (`rule: first`), labname chuẩn (`to`), hệ số đổi đơn vị (`scale`) |
| `semantics` | định nghĩa của nguồn cho các cột dễ hiểu khác nhau (so với `semantics` trong hợp đồng) |

Thêm nguồn mới cùng định dạng: thêm một file YAML. Định dạng mới: thêm một hàm vào `BO_CHUYEN` của
`pipeline/source_adapters.py`. Sửa file nguồn là đổi phiên bản code pipeline (xem mục 7).

---

## 7. Vận hành

**Đọc dữ liệu từ code** (luôn đi qua lớp kiểm soát quyền):

```python
from catalog_access import read_table, AccessDenied
from catalog_access.data_access import read_table_spark

df = read_table("researcher_bob", "ai_ready_icu_mortality")      # pandas, được phép
try:
    read_table("researcher_bob", "raw_patient_hospital_123")     # bị từ chối, audit ghi DENIED
except AccessDenied as loi:
    print(loi.reason)

# Với Spark: trả về DataFrame Spark, cũng kiểm tra quyền + ẩn cột nhạy cảm
from pipeline.spark_session import get_spark
sdf = read_table_spark(get_spark(), "analyst_dave", "standardized_patients")
```

**Khi có vi phạm BREAKING:** mở dashboard trang *Vi phạm* (mặc định lọc vi phạm đang mở) → xem bảng, cột, lô gây
lỗi (`hospital_<mã>/<thời điểm gửi>`) và bảng AI-ready bị ảnh hưởng, rồi:
- dữ liệu sai → báo bệnh viện gửi lại → chạy lại pipeline;
- bệnh viện chỉ đổi tên cột, nội dung không đổi → kỹ sư dữ liệu bấm *Duyệt đổi tên cột* → chạy lại pipeline;
- `PIPELINE_OUTPUT_DRIFT` (code pipeline đổi, cùng đầu vào mà đầu ra lệch) → sửa code; nếu thay đổi là **cố ý**
  thì duyệt phiên bản code: `python -m governance.pipeline_drift --duyet <phiên bản code> --nguoi <tài khoản>`.
Lần chạy sau tự chuyển vi phạm của lô / bảng đã qua cổng sang RESOLVED. Lô bị chặn không bao giờ lên tầng trên;
schema của lô lỗi không trở thành chuẩn cho các lô sau. Lần chạy lại chỉ phải tính hồ sơ cho file mới / file bị sửa.

**Phiên bản code** của mỗi lần chạy (`pipeline_runs.code_version`, `dataset_versions.code_version`) =
`git describe` + băm nội dung `pipeline/*.py`, `governance/*.py`, `sources/*.yaml`: sửa một dòng là phiên bản đổi.

**Kiểm tra định kỳ:**
- Toàn vẹn sổ audit: trang *Audit* trên dashboard, hoặc `verify_audit_chain()`.
- Nguồn gốc dữ liệu AI-ready: trang *Provenance* → *Kiểm tra lại checksum* (kiểm cả phiên bản cũ nhờ Delta
  time travel), hoặc `python -m governance.provenance`.

**Sao lưu:** sao lưu cùng lúc `metadata.db` và `.secrets/` (dùng `backup_database()` vì DB ở chế độ WAL).
Mất khóa sổ audit thì không xác minh được sổ cũ; đổi khóa giả danh thì mã `patient_hash` các lần sau sẽ khác.
Không chạy `VACUUM` trên bảng Delta nếu còn muốn kiểm tra lại phiên bản cũ.

---

## 8. Kết quả đánh giá (`python run_eval.py`)

Chạy trên toàn bộ dữ liệu: 186 bệnh viện eICU + nguồn Challenge 2012 (187 nguồn gửi), 561 đợt gửi, 1.683 file,
khoảng 2,54 triệu dòng; Spark `local[*]` trên laptop 12 core; cả lần đánh giá mất khoảng 13 phút.

| Phép đo | Kết quả |
|---|---|
| Dữ liệu thật, hai hệ thống nguồn | 1.677 / 1.683 lô qua cổng; chặn 6 lô vital kém chất lượng có sẵn của eICU; 210 cảnh báo; làm sạch 342 nhịp tim và 2 cân nặng ngoài khoảng hợp lý; **6.520 bệnh nhân lên AI-ready (2.520 eICU + 4.000 Challenge 2012)**; 1 cảnh báo `SEMANTIC_MISMATCH` (nhãn tử vong của Challenge 2012 là tử vong trong bệnh viện) |
| 5 loại lỗi dữ liệu giả lập – có governance | phát hiện đúng loại 5/5, chặn đúng lô 5/5, chỉ ra bảng AI-ready 5/5, **0 dòng nhiễm bẩn** |
| 5 loại lỗi – không governance (Spark đọc CSV theo vị trí, không cổng) | 4/5 lỗi ảnh hưởng AI-ready, đều **âm thầm**: xóa cột làm lệch cột (8 dòng sai), nhịp tim 500 (10 dòng sai), đổi kiểu và thiếu cân nặng (9 và 3 dòng mất giá trị); đổi tên cột bị che vì Spark khớp theo vị trí |
| 2 loại lỗi **pipeline** giả lập (đổi cân nặng sang pound, đảo nhãn tử vong) | cổng hợp đồng bắt được 0/2 (dữ liệu sai vẫn đúng luật); bộ phát hiện lệch đầu ra bắt 2/2, chỉ ra bảng AI-ready 2/2, **0 dòng nhiễm bẩn**; tắt bộ phát hiện thì **12.544 dòng** AI-ready bị sai (6.024 cân nặng + 6.520 nhãn); sửa code chạy lại thì tầng AI-ready trở về như lúc sạch |
| Hiệu năng | lập hồ sơ 1.683 lô: Spark 14,1 s (pandas từng file 19,4 s); cả pipeline 31–42 s; theo số core: 1 core 83,3 s → 8 core 38,1 s (nhanh 2,2 lần); theo lượng dữ liệu: 0,75 → 2,54 triệu dòng (×3,4) thì thời gian 18,3 → 33,1 s (×1,8); chạy lại khi không file nào đổi 17,4 s (lần đầu 31,5 s) |
| Phân quyền | 24/24 ô (vai trò × tầng × đọc/ghi) đúng thiết kế; người lạ bị từ chối; gõ tên vai trò (`data_engineer`) không được cấp quyền; cột định danh bị ẩn với analyst |
| Sổ audit | phát hiện 4/4 kiểu can thiệp: sửa dòng, xóa dòng giữa, chèn dòng giả (không có khóa), xóa dòng cuối |
| Provenance | truy ngược AI-ready về 1.677 lô của 187 nguồn gửi; chặn bảng bị ghi ngoài pipeline; kiểm tra lại phiên bản cũ bằng time travel; phát hiện file raw bị sửa sau khi nạp |

(Thời gian chạy có thể chênh tùy máy.)

---

## 9. Hạn chế

- Đăng nhập dashboard dùng mật khẩu băm trong `policies.yaml`: chưa có SSO, xác thực hai lớp hay khóa tài khoản
  sau nhiều lần sai (các lần sai đều đã ghi audit). Lớp kiểm soát quyền nằm ở code đọc / ghi (`read_table`,
  `write_table`); ai có quyền đọc thẳng thư mục `data/` trên máy chủ thì vẫn đọc được file, nên khi triển khai thật
  cần phân quyền ở tầng lưu trữ (HDFS / S3).
- metadata (catalog, audit, provenance) nằm trong SQLite: đủ cho một nhóm nhỏ; nhiều người ghi đồng thời nên
  chuyển sang PostgreSQL. Dữ liệu lớn hơn nhiều nên đặt trên HDFS / S3 thay cho thư mục dùng chung.
- Người có **cả** khóa sổ audit và quyền ghi DB vẫn có thể ghi lại toàn bộ chuỗi băm. Nên định kỳ lưu
  băm dòng cuối (`hash_cuoi`) ra một nơi khác để đối chiếu.
- Phần ra quyết định của cổng kiểm tra và ghi metadata (SQLite) chạy tuần tự trên driver, nên thêm core
  quá 8 không nhanh hơn (đo được: 8 core 38,1 s, 12 core 40,8 s – đúng định luật Amdahl). Muốn mở rộng tiếp
  cần ghi metadata theo lô lớn hoặc dùng cơ sở dữ liệu chịu được ghi song song.
- 186 "bệnh viện" eICU có cùng cấu trúc; tính không đồng nhất thật đến từ nguồn thứ hai (Challenge 2012: khác định
  dạng, tên biến, mã hóa, giá trị thiếu, cách ghi thời gian, định nghĩa nhãn). Nguồn này được coi là một "viện"
  (`c2012`) gửi 3 đợt. Bộ chuyển mới hỗ trợ dạng `eav_long`; định dạng khác (FHIR, HL7) cần thêm bộ chuyển.
- Khác biệt về định nghĩa (nhãn tử vong ICU so với tử vong bệnh viện) chỉ được cảnh báo, không tự sửa: tầng AI-ready
  đang gộp hai định nghĩa, người dùng dữ liệu cần lọc theo nguồn nếu bài toán đòi hỏi.
- Bộ phát hiện lệch đầu ra so số dòng, kiểu, % ô trống và trung bình cột số với ngưỡng cố định (20%, 10 điểm):
  lỗi code chỉ đổi phân phối của cột chữ (ví dụ đảo mã giới tính) hoặc lệch nhỏ hơn ngưỡng sẽ không bị bắt;
  khi cả code lẫn dữ liệu đầu vào cùng đổi thì chỉ cảnh báo, không chặn.

---

## 10. Phân công

- **Người 1 – Pipeline dữ liệu:** `pipeline/`
- **Người 2 – Governance:** `governance/`, `contracts/`
- **Người 3 – Catalog, phân quyền, audit, dashboard:** `catalog_access/`, `dashboard/`
