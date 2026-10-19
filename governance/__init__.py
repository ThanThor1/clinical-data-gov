"""
Package governance
Phụ trách bởi: Người 2 (Governance lead)

Các module:
- contracts        : đọc hợp đồng dữ liệu YAML trong thư mục contracts/
- schema_registry  : lưu lịch sử schema, phát hiện đổi tên / xóa / đổi kiểu cột
- lineage          : ghi và tra cứu đường đi dữ liệu (raw -> standardized -> ai_ready)
- violation_engine : ghi vi phạm, tìm bảng AI-ready bị ảnh hưởng, ghi audit
- gates            : cổng kiểm tra gate1 (schema + hợp đồng) và gate2 (chất lượng Kahn)

Hàm dùng chung cho cả nhóm (xem CLAUDE.md mục 4):
    from governance.gates import check_gate
    from governance.lineage import record_lineage
"""
