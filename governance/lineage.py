"""
Lineage (truy vết nguồn gốc dữ liệu): cột nào của bảng nào chảy vào cột nào của bảng nào.

- Trong DB (bảng lineage_edges) chỉ lưu cạnh MỨC CỘT: bang_nguon.cot -> bang_dich.cot.
- Khi tra cứu, ta dựng đồ thị networkx gồm:
    + node cột  "bang.cot"  và cạnh cột -> cột
    + node bảng "bang"      và cạnh bảng -> bảng (suy ra từ cạnh cột)
- Từ đồ thị, tìm mọi bảng phía sau (downstream) và lọc ra bảng tầng AI-ready.

Hàm dùng chung (CLAUDE.md mục 4): record_lineage(nguon, dich, anh_xa_cot)

Chạy thử: python -m governance.lineage
"""

from typing import Optional

import networkx as nx

from catalog_access.init_db import get_db_connection


# ---------------------------------------------------------------------------
# 1. Ghi lineage
# ---------------------------------------------------------------------------

def record_lineage(nguon: str, dich: str, anh_xa_cot: dict[str, str],
                   transformation: str = "direct", db_path: Optional[str] = None) -> None:
    """
    Ghi lineage từ bảng `nguon` sang bảng `dich`.
    anh_xa_cot: {cột_nguồn: cột_đích}, ví dụ {"admissionweight": "weight_kg"}.
    Mỗi cặp cột thành 1 dòng trong lineage_edges; cặp đã có thì không ghi lại (không trùng).
    """
    conn = get_db_connection(db_path)
    for cot_nguon, cot_dich in anh_xa_cot.items():
        da_co = conn.execute(
            "SELECT 1 FROM lineage_edges WHERE source_table = ? AND source_column = ? "
            "AND target_table = ? AND target_column = ?",
            (nguon, cot_nguon, dich, cot_dich),
        ).fetchone()
        if not da_co:
            conn.execute(
                "INSERT INTO lineage_edges (source_table, source_column, target_table, "
                "target_column, transformation) VALUES (?, ?, ?, ?, ?)",
                (nguon, cot_nguon, dich, cot_dich, transformation),
            )
    conn.commit()
    conn.close()


def record_sample_lineage(db_path: Optional[str] = None) -> None:
    """
    Ghi lineage mẫu của cả dự án: raw (3 viện) -> standardized -> ai_ready.
    Dùng cho demo/test chạy độc lập; pipeline thật của Người 1 nên gọi record_lineage tương tự.
    """
    for vien in "ABC":
        bn = f"raw_patient_hospital_{vien}"
        record_lineage(bn, "standardized_patients", {"patientunitstayid": "patient_id"},
                       "đổi tên cột", db_path)
        record_lineage(bn, "standardized_patients", {"age": "age"},
                       "'> 89' -> 90, chuyển sang số nguyên", db_path)
        record_lineage(bn, "standardized_patients", {"gender": "gender"},
                       "Male/Female -> M/F/U", db_path)
        record_lineage(bn, "standardized_patients", {"admissionweight": "weight_kg"},
                       "chuẩn hóa đơn vị kg", db_path)
        record_lineage(bn, "standardized_patients", {"hospitalid": "source_hospital"},
                       "mã viện -> tên bệnh viện", db_path)
        record_lineage(bn, "standardized_patients", {"unitdischargestatus": "unitdischargestatus"},
                       "direct", db_path)

        record_lineage(f"raw_lab_hospital_{vien}", "standardized_labs",
                       {"patientunitstayid": "patient_id", "labname": "lab_name",
                        "labresult": "lab_value"}, "đổi tên cột", db_path)

        record_lineage(f"raw_vital_hospital_{vien}", "standardized_patients",
                       {"heartrate": "heartrate_mean"}, "trung bình theo bệnh nhân", db_path)

    ai = "ai_ready_icu_mortality"
    record_lineage("standardized_patients", ai, {"patient_id": "patient_hash"},
                   "giả danh hóa SHA-256", db_path)
    record_lineage("standardized_patients", ai,
                   {"age": "age", "gender": "gender", "weight_kg": "weight_kg",
                    "heartrate_mean": "heartrate_mean"}, "chọn biến đầu vào AI", db_path)
    record_lineage("standardized_patients", ai, {"unitdischargestatus": "icu_mortality"},
                   "Expired -> 1, Alive -> 0", db_path)
    # standardized_labs KHÔNG chảy vào ai_ready_icu_mortality (bảng AI-ready hiện chưa có cột lab),
    # nên lỗi ở raw_lab vẫn được ghi violation nhưng không có bảng AI-ready nào bị ảnh hưởng.


# ---------------------------------------------------------------------------
# 2. Dựng đồ thị
# ---------------------------------------------------------------------------

def get_lineage_edges(db_path: Optional[str] = None) -> list[dict]:
    """Đọc toàn bộ cạnh lineage trong DB."""
    conn = get_db_connection(db_path)
    dong = conn.execute(
        "SELECT source_table, source_column, target_table, target_column, transformation "
        "FROM lineage_edges ORDER BY id"
    ).fetchall()
    conn.close()
    return [dict(d) for d in dong]


def build_graph(db_path: Optional[str] = None) -> nx.DiGraph:
    """
    Đồ thị có hướng gồm node cột "bang.cot" (kind='column') và node bảng "bang" (kind='table').
    """
    do_thi = nx.DiGraph()
    for canh in get_lineage_edges(db_path):
        nguon = f"{canh['source_table']}.{canh['source_column']}"
        dich = f"{canh['target_table']}.{canh['target_column']}"
        do_thi.add_node(nguon, kind="column", table=canh["source_table"], column=canh["source_column"])
        do_thi.add_node(dich, kind="column", table=canh["target_table"], column=canh["target_column"])
        do_thi.add_edge(nguon, dich, transformation=canh["transformation"])

        do_thi.add_node(canh["source_table"], kind="table", table=canh["source_table"])
        do_thi.add_node(canh["target_table"], kind="table", table=canh["target_table"])
        do_thi.add_edge(canh["source_table"], canh["target_table"])
    return do_thi


# ---------------------------------------------------------------------------
# 3. Tra cứu tác động
# ---------------------------------------------------------------------------

def _node_bat_dau(do_thi: nx.DiGraph, table: str, column: Optional[str]) -> Optional[str]:
    """
    Chọn node để bắt đầu tìm: node cột nếu cột có trong lineage, không thì node bảng.
    (Cột chưa có lineage -> tra theo bảng cho chắc: chưa biết thì coi như có ảnh hưởng.)
    """
    if column and f"{table}.{column}" in do_thi:
        return f"{table}.{column}"
    if table in do_thi:
        return table
    return None


def get_downstream_tables(table: str, column: Optional[str] = None,
                          db_path: Optional[str] = None) -> list[str]:
    """Mọi bảng nằm phía sau `table` (hoặc phía sau cột `table.column`) trong lineage."""
    do_thi = build_graph(db_path)
    bat_dau = _node_bat_dau(do_thi, table, column)
    if bat_dau is None:
        return []
    bang = {do_thi.nodes[n]["table"] for n in nx.descendants(do_thi, bat_dau)}
    bang.discard(table)
    return sorted(bang)


def get_table_tier(table: str, db_path: Optional[str] = None) -> str:
    """Tầng của bảng: tra bảng datasets trước; chưa đăng ký thì đoán theo tiền tố tên bảng."""
    conn = get_db_connection(db_path)
    dong = conn.execute("SELECT tier FROM datasets WHERE name = ?", (table,)).fetchone()
    conn.close()
    if dong:
        return dong["tier"]
    if table.startswith("ai_ready_"):
        return "ai_ready"
    if table.startswith("standardized_"):
        return "standardized"
    return "raw"


def get_affected_ai_tables(table: str, column: Optional[str] = None,
                           db_path: Optional[str] = None) -> list[str]:
    """
    Các bảng tầng AI-ready bị ảnh hưởng khi `table` (hoặc cột `table.column`) có vấn đề.
    Nếu bản thân `table` là bảng AI-ready thì nó cũng nằm trong danh sách.
    """
    ung_vien = get_downstream_tables(table, column, db_path)
    if get_table_tier(table, db_path) == "ai_ready":
        ung_vien.append(table)
    return sorted({b for b in ung_vien if get_table_tier(b, db_path) == "ai_ready"})


def get_impact_paths(table: str, column: Optional[str] = None,
                     db_path: Optional[str] = None) -> list[str]:
    """
    Các đường đi từ cột/bảng lỗi tới bảng AI-ready, dạng chuỗi dễ đọc, ví dụ:
    raw_patient_hospital_B.admissionweight -> standardized_patients.weight_kg -> ai_ready_icu_mortality.weight_kg
    """
    do_thi = build_graph(db_path)
    bat_dau = _node_bat_dau(do_thi, table, column)
    if bat_dau is None:
        return []
    loai = do_thi.nodes[bat_dau]["kind"]
    duong_di = []
    for dich in nx.descendants(do_thi, bat_dau):
        if do_thi.nodes[dich]["kind"] != loai:
            continue
        if get_table_tier(do_thi.nodes[dich]["table"], db_path) != "ai_ready":
            continue
        for path in nx.all_simple_paths(do_thi, bat_dau, dich):
            duong_di.append(" -> ".join(path))
    return sorted(duong_di)


if __name__ == "__main__":
    import os
    import tempfile

    from catalog_access.init_db import init_database

    db = os.path.join(tempfile.mkdtemp(), "thu_lineage.db")
    init_database(db, seed_sample_data=False)
    record_sample_lineage(db)

    g = build_graph(db)
    print(f"Đồ thị lineage: {g.number_of_nodes()} node, {g.number_of_edges()} cạnh")
    print("Phía sau raw_patient_hospital_B:", get_downstream_tables("raw_patient_hospital_B", db_path=db))
    print("Bảng AI bị ảnh hưởng nếu B.admissionweight lỗi:",
          get_affected_ai_tables("raw_patient_hospital_B", "admissionweight", db))
    for p in get_impact_paths("raw_patient_hospital_B", "admissionweight", db):
        print("  ", p)
