"""
Package pipeline
Phụ trách bởi: Người 1 (Pipeline dữ liệu)

Các bước:
- split_hospitals  : tách 3 bệnh viện đông bệnh nhân nhất từ dữ liệu eICU (data/01_landing -> 02_staging)
- split_snapshots  : chia dữ liệu mỗi viện thành 3 đợt gửi (data/raw/hospital_X/<thời điểm>/)
- ingest_raw       : chạy split_snapshots và đăng ký 9 bảng raw vào catalog
- standardize      : raw -> standardized (qua gate1 + gate2, ghi provenance từng lô)
- build_ai_ready   : standardized -> ai_ready_icu_mortality (giả danh HMAC, đặc trưng xét nghiệm)
- inject_errors    : gây lỗi giả lập cho đợt 3 để thử hệ thống governance
"""
