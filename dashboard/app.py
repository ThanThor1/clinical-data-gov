"""
Ứng dụng Dashboard Quản trị Dữ liệu Lâm sàng Đa Bệnh viện
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit, Dashboard và Báo cáo)

Người xem chọn tài khoản ở thanh bên; mọi lần đọc dữ liệu trên dashboard đều đi qua
catalog_access.read_table nên được kiểm tra quyền và ghi vào sổ audit như pipeline.

Gồm 6 trang:
1. Catalog: Danh sách các bảng qua 3 tầng (Raw, Standardized, AI-ready), từ điển cột theo hợp đồng
2. Truy cập dữ liệu: Đọc thử một bảng với tài khoản hiện tại (được phép / bị từ chối / bị ẩn cột)
3. Lineage: Đồ thị đường đi dữ liệu từ nguồn bệnh viện đến AI-ready, phân tích tác động theo cột
4. Provenance: Các lần chạy pipeline, lô dữ liệu (checksum), phiên bản bảng và cây nguồn gốc
5. Vi phạm (Violations): Lỗi gây hỏng (Breaking) TÔ MÀU ĐỎ và chỉ rõ bảng AI bị ảnh hưởng, lô gây lỗi
6. Audit: Kiểm tra toàn vẹn sổ (chuỗi băm) và sổ nhật ký hành động toàn hệ thống

Chạy: streamlit run dashboard/app.py
"""

import html
import json
import os
import sys
import pandas as pd
import streamlit as st
import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Thêm thư mục gốc vào sys.path để import catalog_access
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from catalog_access.init_db import get_db_connection, init_database
from catalog_access.catalog import list_datasets, get_dataset
from catalog_access.audit import get_audit_logs, verify_audit_chain
from catalog_access.access import AccessDenied, can_view_audit, get_role_info, load_policies
from catalog_access.data_access import read_table
from governance.contracts import find_contract
from governance.lineage import get_affected_ai_tables, get_impact_paths, get_table_tier
from governance.provenance import format_trace, get_batches, get_dataset_versions, get_runs, trace_version, verify_version
from governance.schema_registry import get_schema_history

# Cấu hình trang Streamlit
st.set_page_config(
    page_title="Hệ Thống Quản Trị Dữ Liệu Lâm Sàng Đa Bệnh Viện",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# DB cũ thì được nâng cấp, DB chưa có thì được tạo (không nạp dữ liệu giả)
init_database(seed_sample_data=False)

# Custom CSS cho giao diện hiện đại, chuyên nghiệp
st.markdown("""
<style>
    .main-header {
        font-size: 26px;
        font-weight: 700;
        color: #1e3a8a;
        margin-bottom: 5px;
    }
    .sub-header {
        font-size: 14px;
        color: #64748b;
        margin-bottom: 20px;
    }
    .breaking-box {
        background-color: #fee2e2;
        border: 2px solid #ef4444;
        border-radius: 8px;
        padding: 16px;
        color: #991b1b;
        margin-bottom: 12px;
    }
</style>
""", unsafe_allow_html=True)

TEN_TANG = {"raw": "Thô (Raw)", "standardized": "Chuẩn hóa (Standardized)", "ai_ready": "AI-Ready"}
MAU_TRANG_THAI = {"SUCCESS": "🟢", "PARTIAL": "🟡", "BLOCKED": "🔴", "FAILED": "🔴", "RUNNING": "⏳"}


def load_violations():
    """Tải danh sách các vi phạm từ metadata.db."""
    conn = get_db_connection()
    df = pd.read_sql_query("""
    SELECT id, error_type, severity, hospital, affected_table, affected_ai_table, action, details,
           timestamp, run_id, batch
    FROM violations ORDER BY id DESC;
    """, conn)
    conn.close()
    return df


def load_lineage():
    """Tải các cạnh truy vết nguồn gốc (lineage edges)."""
    conn = get_db_connection()
    df = pd.read_sql_query("""
    SELECT source_table, source_column, target_table, target_column, transformation
    FROM lineage_edges ORDER BY id ASC;
    """, conn)
    conn.close()
    return df


def bang_tu_dien_cot(ten_bang):
    """Từ điển cột theo hợp đồng dữ liệu của bảng (None nếu chưa có hợp đồng)."""
    hop_dong = find_contract(ten_bang)
    if not hop_dong:
        return None
    dong = []
    for cot, luat in hop_dong["columns"].items():
        khoang = ""
        if luat["min"] is not None or luat["max"] is not None:
            khoang = f"[{luat['min']}, {luat['max']}]"
        dong.append({
            "Cột": cot,
            "Kiểu": luat["type"],
            "Bắt buộc": "✔" if luat["required"] else "",
            "Cho trống": "✔" if luat["nullable"] else "",
            "% trống tối đa": luat["max_null_pct"] if luat["max_null_pct"] is not None else "",
            "Khoảng hợp lý": khoang,
            "Giá trị cho phép": ", ".join(map(str, luat["allowed_values"] or [])) or (luat["pattern"] or ""),
            "Nhạy cảm": "🔒" if luat["sensitive"] else "",
            "Mô tả": luat["description"],
        })
    return pd.DataFrame(dong)


# ---------------------------------------------------------
# SIDEBAR: TÀI KHOẢN, MENU, TRẠNG THÁI HỆ THỐNG
# ---------------------------------------------------------
st.sidebar.image("https://img.icons8.com/fluency/96/hospital-3.png", width=64)
st.sidebar.title("Quản Trị Lâm Sàng")
st.sidebar.caption("Đề tài 5: Quản trị dữ liệu lâm sàng đa bệnh viện (eICU Demo)")

policies = load_policies()
tai_khoan = [u for u in ("engineer_alice", "analyst_dave", "researcher_bob", "guest_charlie")
             if u in policies.get("users", {})]
nguoi_dung = st.sidebar.selectbox("ĐĂNG NHẬP VỚI TÀI KHOẢN:", tai_khoan, index=0)
vai_tro = get_role_info(nguoi_dung, policies)
st.sidebar.caption(f"Vai trò: **{vai_tro.get('display_name', vai_tro['role'])}** – {vai_tro.get('description', '')}")

menu = st.sidebar.radio(
    "CHỌN TRANG XEM:",
    ["1. 📚 Catalog (Danh bạ dữ liệu)",
     "2. 🔐 Truy cập dữ liệu (RBAC)",
     "3. 🔗 Lineage (Đồ thị nguồn gốc)",
     "4. 🧾 Provenance (Lần chạy & lô dữ liệu)",
     "5. ⚠️ Vi phạm (Violations & Chặn lỗi)",
     "6. 📋 Audit (Sổ nhật ký)"],
    index=0
)

# Trạng thái hệ thống theo LẦN CHẠY GẦN NHẤT (không phải mọi lỗi trong lịch sử)
violations_df = load_violations()
cac_lan_chay = get_runs(limit=200)
lan_cuoi = cac_lan_chay[0] if cac_lan_chay else None
breaking_lan_cuoi = (violations_df[(violations_df["severity"] == "BREAKING")
                                   & (violations_df["run_id"] == lan_cuoi["run_id"])]
                     if lan_cuoi else violations_df.iloc[0:0])
has_breaking = not breaking_lan_cuoi.empty
toan_ven = verify_audit_chain()

st.sidebar.markdown("---")
st.sidebar.markdown("**TRẠNG THÁI HỆ THỐNG:**")
if lan_cuoi is None:
    st.sidebar.info("Chưa có lần chạy pipeline nào. Chạy: `python run_demo.py --reset`")
elif has_breaking:
    st.sidebar.error(f"🔴 **LẦN CHẠY GẦN NHẤT CÓ LỖI BREAKING** ({len(breaking_lan_cuoi)})\n\n"
                     "Cổng kiểm soát đã chặn dữ liệu hỏng để bảo vệ mô hình AI!")
else:
    st.sidebar.success("🟢 **HỆ THỐNG AN TOÀN**\n\nLần chạy gần nhất không có lỗi gây hỏng.")
if lan_cuoi is not None:
    st.sidebar.caption(f"Lần chạy gần nhất: `{lan_cuoi['run_id']}` – "
                       f"{MAU_TRANG_THAI.get(lan_cuoi['status'], '')} {lan_cuoi['status']}")
if toan_ven["ok"]:
    st.sidebar.caption(f"🔏 Sổ audit toàn vẹn ({toan_ven['so_dong']} dòng)")
else:
    st.sidebar.error(f"🚨 Sổ audit bị can thiệp: {toan_ven['ly_do']}")

st.sidebar.markdown("---")
st.sidebar.markdown("""
**Phân công Người 3:**
- Danh bạ dữ liệu (Catalog)
- Phân quyền theo vai trò (RBAC)
- Sổ ghi chép (Audit Log)
- Màn hình Dashboard 6 trang
- Tài liệu phương hướng
""")


# =========================================================
# TRANG 1: CATALOG (DANH BẠ DỮ LIỆU)
# =========================================================
if "1. 📚 Catalog" in menu:
    st.markdown('<div class="main-header">📚 Metadata Catalog - Danh Bạ Bảng Dữ Liệu</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Quản lý định danh, nguồn gốc bệnh viện, tầng dữ liệu, hợp đồng áp dụng và gắn nhãn các cột nhạy cảm</div>', unsafe_allow_html=True)

    datasets_df = list_datasets(as_df=True)

    # Hiển thị số liệu thống kê (KPIs)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Tổng số bảng dữ liệu", len(datasets_df))
    with col2:
        st.metric("Tầng Thô (Raw)", int((datasets_df['tier'] == 'raw').sum()), "Từ 3 BV")
    with col3:
        st.metric("Tầng Chuẩn Hóa", int((datasets_df['tier'] == 'standardized').sum()), "Đã gộp schema")
    with col4:
        st.metric("Tầng AI-Ready", int((datasets_df['tier'] == 'ai_ready').sum()), "Sạch & Ẩn danh")

    st.markdown("---")

    if datasets_df.empty:
        st.info("Catalog chưa có bảng nào. Chạy `python run_demo.py --reset` để nạp dữ liệu.")
    else:
        # Bộ lọc
        f_col1, f_col2 = st.columns([1, 2])
        with f_col1:
            tier_filter = st.selectbox("Lọc theo tầng dữ liệu:", ["Tất cả", "raw", "standardized", "ai_ready"])
        with f_col2:
            hosp_filter = st.selectbox("Lọc theo bệnh viện / đơn vị:", ["Tất cả"] + sorted(list(datasets_df['hospital'].unique())))

        filtered_df = datasets_df.copy()
        if tier_filter != "Tất cả":
            filtered_df = filtered_df[filtered_df['tier'] == tier_filter]
        if hosp_filter != "Tất cả":
            filtered_df = filtered_df[filtered_df['hospital'] == hosp_filter]

        st.subheader(f"Danh sách các bảng ({len(filtered_df)} bảng)")

        # Bảng chi tiết
        display_df = filtered_df[['id', 'name', 'hospital', 'tier', 'owner', 'sensitive_columns', 'contract',
                                  'row_count', 'updated_at']].copy()
        display_df.columns = ['ID', 'Tên Bảng', 'Bệnh Viện', 'Tầng Dữ Liệu', 'Người Phụ Trách',
                              'Cột Nhạy Cảm (Sensitive)', 'Hợp Đồng', 'Số Dòng', 'Cập Nhật']
        st.dataframe(display_df, width="stretch", hide_index=True)

        # Chi tiết một bảng: mô tả, vị trí file, từ điển cột theo hợp đồng, schema thực tế mới nhất
        st.subheader("🔎 Chi tiết bảng")
        ten_bang = st.selectbox("Chọn bảng:", list(filtered_df['name']) or list(datasets_df['name']))
        ds = get_dataset(ten_bang) or {}
        st.markdown(f"**Mô tả:** {ds.get('description') or '(chưa có)'}  \n"
                    f"**Hợp đồng:** `{ds.get('contract') or 'chưa có hợp đồng'}` | "
                    f"**Vị trí file:** `{ds.get('file_path') or '(chưa đăng ký)'}`")
        tu_dien = bang_tu_dien_cot(ten_bang)
        if tu_dien is not None:
            st.caption("Từ điển cột theo hợp đồng dữ liệu (contracts/*.yaml)")
            st.dataframe(tu_dien, width="stretch", hide_index=True)
        lich_su = get_schema_history(ten_bang)
        if lich_su:
            moi_nhat = lich_su[-1]
            st.caption(f"Schema thực tế phiên bản v{moi_nhat['version']} (ghi lúc {moi_nhat['recorded_at']})")
            ho_so = pd.DataFrame(json.loads(moi_nhat["columns_json"])).T.reset_index()
            st.dataframe(ho_so.rename(columns={"index": "Cột", "type": "Kiểu thực tế", "null_pct": "% trống",
                                               "mean": "Trung bình"}),
                         width="stretch", hide_index=True)

    # Chi tiết Schema đã ghi nhận trong schema_versions
    with st.expander("🔍 Xem Lịch Sử Phiên Bản Schema (Schema Evolution Log)"):
        conn = get_db_connection()
        schemas_df = pd.read_sql_query("""
        SELECT table_name, version, columns_json, change_type, is_breaking, recorded_at
        FROM schema_versions ORDER BY id DESC;
        """, conn)
        conn.close()
        st.dataframe(schemas_df, width="stretch")


# =========================================================
# TRANG 2: TRUY CẬP DỮ LIỆU (RBAC ĐƯỢC THỰC THI)
# =========================================================
elif "2. 🔐 Truy cập" in menu:
    st.markdown('<div class="main-header">🔐 Truy Cập Dữ Liệu Theo Vai Trò (RBAC)</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Mọi lần đọc đều qua read_table: kiểm tra quyền theo policies.yaml, ẩn cột nhạy cảm, ghi sổ audit</div>', unsafe_allow_html=True)

    st.subheader("Ma trận phân quyền (catalog_access/policies.yaml)")
    ma_tran = []
    for ma_vai_tro, thong_tin in policies.get("roles", {}).items():
        dong = {"Vai trò": thong_tin.get("display_name", ma_vai_tro)}
        for tang in ("raw", "standardized", "ai_ready"):
            if tang not in thong_tin.get("allowed_tiers", []):
                dong[TEN_TANG[tang]] = "⛔"
            else:
                quyen = [q for q, co in (("đọc", thong_tin.get("can_read")), ("ghi", thong_tin.get("can_write"))) if co]
                dong[TEN_TANG[tang]] = " + ".join(quyen) or "⛔"
        dong["Xem cột nhạy cảm"] = "✔" if thong_tin.get("can_view_sensitive") else "ẩn"
        dong["Xem sổ audit"] = "✔" if thong_tin.get("can_view_audit") else "⛔"
        ma_tran.append(dong)
    st.dataframe(pd.DataFrame(ma_tran), width="stretch", hide_index=True)

    st.markdown("---")
    st.subheader(f"🧪 Đọc dữ liệu với tài khoản `{nguoi_dung}`")
    st.caption("Tiêu chí nghiệm thu: 'Đăng nhập vai trò Nhà nghiên cứu mà đọc dữ liệu thô thì bị từ chối và ghi vào sổ audit'")
    datasets_df = list_datasets(as_df=True)
    co_file = datasets_df[datasets_df["file_path"].fillna("") != ""]
    if co_file.empty:
        st.info("Chưa có bảng nào có file dữ liệu. Chạy `python run_demo.py --reset` trước.")
    else:
        c1, c2 = st.columns([3, 1])
        with c1:
            bang_doc = st.selectbox("Chọn bảng muốn đọc:",
                                    [f"{r['name']} ({TEN_TANG.get(r['tier'], r['tier'])})" for _, r in co_file.iterrows()])
        with c2:
            st.write("")
            st.write("")
            bam_doc = st.button("🚀 Đọc dữ liệu", width="stretch")
        if bam_doc:
            ten = bang_doc.split(" ")[0]
            try:
                df = read_table(nguoi_dung, ten)
                nhay_cam = [c for c in (get_dataset(ten) or {}).get("sensitive_columns", "").split(",") if c]
                bi_an = [c for c in nhay_cam if c not in df.columns]
                st.success(f"✅ **ĐƯỢC PHÉP (ALLOWED):** `{nguoi_dung}` đọc `{ten}` – {len(df)} dòng. Đã ghi vào sổ audit.")
                if bi_an:
                    st.warning(f"🙈 Vai trò không được xem cột nhạy cảm → đã ẩn: {', '.join(bi_an)}")
                st.dataframe(df.head(50), width="stretch", hide_index=True)
            except AccessDenied as loi:
                st.error(f"🛑 **TỪ CHỐI TRUY CẬP (DENIED):** {loi.reason}. Sự việc đã được ghi nhận vào sổ audit!")
            except FileNotFoundError as loi:
                st.warning(f"Không tìm thấy file dữ liệu: {loi}")


# =========================================================
# TRANG 3: LINEAGE (ĐỒ THỊ NGUỒN GỐC DỮ LIỆU)
# =========================================================
elif "3. 🔗 Lineage" in menu:
    st.markdown('<div class="main-header">🔗 Data Lineage - Đồ Thị Đường Đi Của Dữ Liệu</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Truy vết nguồn gốc: Bệnh viện nào -> Cột nào tầng Thô -> Chuẩn hóa cột nào -> Đi vào biến nào của mô hình AI</div>', unsafe_allow_html=True)

    lineage_df = load_lineage()
    if lineage_df.empty:
        st.info("Chưa có lineage. Chạy `python run_demo.py --reset` trước.")
    else:
        muc = st.radio("Mức hiển thị:", ["Mức bảng", "Mức cột"], horizontal=True)
        col_graph, col_table = st.columns([3, 2])

        with col_graph:
            st.subheader("Trực quan hóa luồng dữ liệu (Pipeline Graph)")
            # Xây dựng đồ thị NetworkX; xếp theo tầng: raw (trái) -> standardized -> AI-ready (phải)
            G = nx.DiGraph()
            for _, row in lineage_df.iterrows():
                if muc == "Mức bảng":
                    src, tgt = row['source_table'], row['target_table']
                    G.add_node(src, bang=src)
                    G.add_node(tgt, bang=tgt)
                else:
                    src = f"{row['source_table']}\n.{row['source_column']}"
                    tgt = f"{row['target_table']}\n.{row['target_column']}"
                    G.add_node(src, bang=row['source_table'])
                    G.add_node(tgt, bang=row['target_table'])
                G.add_edge(src, tgt)

            thu_tu_tang = {"raw": 0, "standardized": 1, "ai_ready": 2}
            mau_theo_tang = ["#60a5fa", "#fbbf24", "#4ade80"]    # Xanh dương - raw, Vàng - standardized, Xanh lá - AI
            for _, du_lieu in G.nodes(data=True):
                du_lieu["tang"] = thu_tu_tang.get(get_table_tier(du_lieu["bang"]), 0)
            pos = nx.multipartite_layout(G, subset_key="tang")
            node_colors = [mau_theo_tang[G.nodes[n]["tang"]] for n in G.nodes()]

            fig, ax = plt.subplots(figsize=(10, 7 if muc == "Mức bảng" else 12))
            nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=2200 if muc == "Mức bảng" else 900,
                                   alpha=0.9, ax=ax)
            nx.draw_networkx_labels(G, pos, font_size=8 if muc == "Mức bảng" else 5, font_family="sans-serif",
                                    font_weight="bold", ax=ax)
            nx.draw_networkx_edges(G, pos, edge_color="#94a3b8", arrows=True, arrowstyle="-|>", arrowsize=14,
                                   width=1.2, ax=ax)
            ax.set_title("Sơ đồ biến đổi từ Dữ liệu thô BV -> Chuẩn hóa -> Bảng AI-Ready", fontsize=11, fontweight="bold")
            ax.axis("off")
            st.pyplot(fig)
            plt.close(fig)

            st.caption("🔵 Xanh dương: Tầng Thô (Raw) | 🟡 Vàng: Chuẩn Hóa (Standardized) | 🟢 Xanh lá: AI-Ready")

        with col_table:
            st.subheader("Chi tiết ánh xạ cột (Column Mappings)")
            st.dataframe(
                lineage_df.rename(columns={
                    'source_table': 'Bảng Nguồn',
                    'source_column': 'Cột Nguồn',
                    'target_table': 'Bảng Đích',
                    'target_column': 'Cột Đích',
                    'transformation': 'Phương Pháp Biến Đổi'
                }),
                width="stretch",
                hide_index=True
            )

        st.markdown("---")
        st.subheader("🎯 Phân tích tác động (Impact Analysis)")
        st.caption("Nếu một cột ở bệnh viện nguồn bị lỗi thì bảng AI-ready nào bị ảnh hưởng, qua đường đi nào?")
        bang_nguon = sorted(lineage_df["source_table"].unique())
        i1, i2 = st.columns(2)
        with i1:
            bang_chon = st.selectbox("Bảng:", bang_nguon,
                                     index=bang_nguon.index("raw_patient_hospital_B") if "raw_patient_hospital_B" in bang_nguon else 0)
        with i2:
            cot_nguon = sorted(lineage_df[lineage_df["source_table"] == bang_chon]["source_column"].unique())
            cot_chon = st.selectbox("Cột:", ["(cả bảng)"] + cot_nguon)
        cot = None if cot_chon == "(cả bảng)" else cot_chon
        anh_huong = get_affected_ai_tables(bang_chon, cot)
        if anh_huong:
            st.error(f"Bảng AI-ready bị ảnh hưởng: **{', '.join(anh_huong)}**")
            for duong in get_impact_paths(bang_chon, cot):
                st.code(duong, language=None)
        else:
            st.success("Không có bảng AI-ready nào bị ảnh hưởng (theo lineage).")


# =========================================================
# TRANG 4: PROVENANCE (LẦN CHẠY, LÔ DỮ LIỆU, PHIÊN BẢN)
# =========================================================
elif "4. 🧾 Provenance" in menu:
    st.markdown('<div class="main-header">🧾 Provenance - Dữ Liệu Được Tạo Ra Từ Đâu, Khi Nào, Bởi Ai</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Mỗi lần chạy pipeline ghi lại từng lô dữ liệu nguồn (file, checksum SHA-256, số dòng, kết quả cổng) và từng phiên bản bảng đầu ra</div>', unsafe_allow_html=True)

    if not cac_lan_chay:
        st.info("Chưa có lần chạy nào. Chạy `python run_demo.py --reset` trước.")
    else:
        runs_df = pd.DataFrame(cac_lan_chay)
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Số lần chạy", len(runs_df))
        with m2:
            st.metric("Thành công", int((runs_df["status"] == "SUCCESS").sum()))
        with m3:
            st.metric("Một phần bị chặn", int((runs_df["status"] == "PARTIAL").sum()))
        with m4:
            st.metric("Bị chặn / lỗi", int(runs_df["status"].isin(["BLOCKED", "FAILED"]).sum()))

        st.subheader("Các lần chạy pipeline")
        hien_thi = runs_df.assign(status=runs_df["status"].map(lambda s: f"{MAU_TRANG_THAI.get(s, '')} {s}"))
        st.dataframe(hien_thi.rename(columns={"run_id": "Run ID", "actor": "Tài khoản", "started_at": "Bắt đầu",
                                              "finished_at": "Kết thúc", "status": "Trạng thái",
                                              "code_version": "Phiên bản code", "note": "Ghi chú"}),
                     width="stretch", hide_index=True)

        run_chon = st.selectbox("Xem các lô dữ liệu của lần chạy:", list(runs_df["run_id"]))
        lo_df = pd.DataFrame(get_batches(run_chon))
        if lo_df.empty:
            st.caption("Lần chạy này không đọc lô dữ liệu raw nào (ví dụ chỉ tạo tầng AI-ready).")
        else:
            lo_df["checksum"] = lo_df["checksum"].str[:16] + "…"
            st.dataframe(lo_df[["id", "table_name", "batch", "status", "row_count", "schema_version", "contract",
                                "checksum", "source_file"]].rename(columns={
                "id": "Lô #", "table_name": "Bảng", "batch": "Đợt gửi", "status": "Kết quả", "row_count": "Số dòng",
                "schema_version": "Schema", "contract": "Hợp đồng", "checksum": "SHA-256", "source_file": "File nguồn"}),
                width="stretch", hide_index=True)

        st.markdown("---")
        st.subheader("🌳 Cây nguồn gốc của một phiên bản dữ liệu")
        phien_ban = get_dataset_versions()
        if not phien_ban:
            st.caption("Chưa có phiên bản bảng đầu ra nào.")
        else:
            pb_df = pd.DataFrame(phien_ban)
            bang_pb = st.selectbox("Bảng:", sorted(pb_df["table_name"].unique(), reverse=True))
            cac_pb = pb_df[pb_df["table_name"] == bang_pb]
            pb_chon = st.selectbox("Phiên bản:", [f"#{r['id']} – {r['created_at']} – {r['row_count']} dòng – {r['run_id']}"
                                                   for _, r in cac_pb.iterrows()])
            ma_pb = int(pb_chon.split(" ")[0].lstrip("#"))
            st.code("\n".join(format_trace(trace_version(ma_pb))), language=None)
            if st.button("🔁 Kiểm tra lại checksum của phiên bản và mọi file nguồn"):
                kq = verify_version(ma_pb)
                if kq["ok"]:
                    st.success(f"✅ Toàn bộ {kq['da_kiem']} file khớp checksum đã ghi.")
                else:
                    st.error("🚨 Có file không còn khớp provenance:\n\n" + "\n".join(f"- {v}" for v in kq["van_de"]))


# =========================================================
# TRANG 5: VI PHẠM (VIOLATIONS & CHẶN LỖI)
# =========================================================
elif "5. ⚠️ Vi phạm" in menu:
    st.markdown('<div class="main-header">⚠️ Vi Phạm & Trạm Kiểm Soát Chất Lượng</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Phát hiện dữ liệu sai hợp đồng, đổi tên schema, rò rỉ định danh, file mất nguồn gốc và chặn không cho chảy vào tầng AI</div>', unsafe_allow_html=True)

    if has_breaking:
        st.markdown("""
        <div class="breaking-box">
            <h3 style="margin-top:0;">🚨 CẢNH BÁO NGUY HIỂM: PHÁT HIỆN LỖI GÂY HỎNG (BREAKING CHANGE)</h3>
            <p>Lần chạy gần nhất có dữ liệu vi phạm hợp đồng. Dữ liệu đã bị <b>CHẶN LẠI</b> tại Cổng kiểm soát.</p>
        </div>
        """, unsafe_allow_html=True)

    f1, f2 = st.columns(2)
    with f1:
        run_loc = st.selectbox("Lọc theo lần chạy:", ["Tất cả"] + [r["run_id"] for r in cac_lan_chay])
    with f2:
        muc_loc = st.selectbox("Lọc theo mức độ:", ["Tất cả", "BREAKING", "WARNING"])
    loc_df = violations_df
    if run_loc != "Tất cả":
        loc_df = loc_df[loc_df["run_id"] == run_loc]
    if muc_loc != "Tất cả":
        loc_df = loc_df[loc_df["severity"] == muc_loc]

    # Số lượng vi phạm
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Tổng số vi phạm", len(loc_df))
    with m2:
        st.metric("Lỗi gây hỏng (Breaking)", int((loc_df['severity'] == 'BREAKING').sum()), "Tô màu đỏ (Đã chặn)")
    with m3:
        st.metric("Cảnh báo chất lượng", int((loc_df['severity'] == 'WARNING').sum()), "Vượt ngưỡng Kahn")

    st.markdown("---")
    st.subheader("Danh sách chi tiết vi phạm:")

    for _, v in loc_df.iterrows():
        is_break = v['severity'] in ('BREAKING', 'CRITICAL')
        box_style = "border-left: 6px solid #dc2626; background-color: #fef2f2;" if is_break else "border-left: 6px solid #f59e0b; background-color: #fffbeb;"
        badge_color = "#dc2626" if is_break else "#d97706"
        ai_color = "#b91c1c" if is_break else "#92400e"
        nguon_goc = (f" | <b>Lô:</b> <code>{html.escape(v['batch'])}</code>" if v['batch'] else "") + \
                    (f" | <b>Lần chạy:</b> <code>{html.escape(v['run_id'])}</code>" if v['run_id'] else "")

        st.markdown(f"""
        <div style="border-radius: 8px; padding: 14px; margin-bottom: 12px; {box_style}">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span style="font-weight:bold; font-size:16px; color:{badge_color}">
                    [{html.escape(v['error_type'])}] - Mức độ: {html.escape(v['severity'])}
                </span>
                <span style="background-color:{badge_color}; color:white; padding:3px 10px; border-radius:12px; font-size:12px; font-weight:bold;">
                    HÀNH ĐỘNG: {html.escape(v['action'].upper())}
                </span>
            </div>
            <p style="margin: 8px 0 4px 0; color:#334155;"><b>Bệnh viện vi phạm:</b> {html.escape(v['hospital'])} | <b>Bảng phát hiện:</b> <code>{html.escape(v['affected_table'])}</code>{nguon_goc}</p>
            <p style="margin: 4px 0; color:{ai_color};"><b>Bảng AI-ready bị đe dọa:</b> <code>{html.escape(v['affected_ai_table'] or 'N/A')}</code></p>
            <p style="margin: 4px 0; color:#475569;"><b>Chi tiết lỗi:</b> {html.escape(v['details'])}</p>
            <small style="color:#94a3b8;">Thời gian ghi nhận: {html.escape(str(v['timestamp']))}</small>
        </div>
        """, unsafe_allow_html=True)


# =========================================================
# TRANG 6: AUDIT LOG
# =========================================================
elif "6. 📋 Audit" in menu:
    st.markdown('<div class="main-header">📋 Sổ Nhật Ký Kiểm Toán (Audit Log)</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Cuốn sổ ghi chép mọi hành động: ai, làm gì, lúc nào, trên bảng nào, kết quả ra sao – có chuỗi băm chống sửa</div>', unsafe_allow_html=True)

    st.subheader("🔏 Kiểm tra toàn vẹn sổ audit")
    if toan_ven["ok"]:
        st.success(f"✅ {toan_ven['ly_do']}: {toan_ven['so_dong']} dòng, không dòng nào bị sửa / xóa / chèn. "
                   f"Băm dòng cuối: `{toan_ven['hash_cuoi']}`")
    else:
        st.error(f"🚨 SỔ AUDIT BỊ CAN THIỆP – {toan_ven['ly_do']}")
    st.caption("Mỗi dòng lưu HMAC-SHA256 của nội dung nối với băm của dòng trước. Sửa / xóa / chèn dòng đều làm đứt chuỗi.")

    st.markdown("---")
    if not can_view_audit(nguoi_dung):
        st.warning(f"⛔ Vai trò **{vai_tro.get('display_name', vai_tro['role'])}** không được xem nội dung sổ audit "
                   "(policies.yaml: can_view_audit). Đăng nhập bằng tài khoản Kỹ sư dữ liệu để xem.")
    else:
        st.subheader("📖 Danh Sách Sổ Ghi Chép Kiểm Toán")

        filter_col1, filter_col2, filter_col3 = st.columns(3)
        with filter_col1:
            res_filter = st.selectbox("Lọc theo Kết Quả:", ["Tất cả", "ALLOWED", "DENIED", "BLOCKED", "SUCCESS", "FAILED"])
        with filter_col2:
            search_actor = st.text_input("Tìm kiếm theo Tên Người Dùng (Actor):", "")
        with filter_col3:
            run_filter = st.selectbox("Lọc theo lần chạy:", ["Tất cả"] + [r["run_id"] for r in cac_lan_chay])

        audit_df = get_audit_logs(
            limit=500,
            result=res_filter if res_filter != "Tất cả" else None,
            actor=search_actor if search_actor.strip() else None,
            run_id=run_filter if run_filter != "Tất cả" else None,
            as_df=True
        )

        if not audit_df.empty:
            # Hiển thị sổ audit gồm đúng 5 thông tin cốt lõi: thời gian, ai, làm gì, bảng nào, kết quả (+ lần chạy)
            display_audit = audit_df[['id', 'thoi_gian', 'ai', 'lam_gi', 'bang_nao', 'ket_qua', 'run_id']].rename(columns={
                'id': 'ID',
                'thoi_gian': 'Thời Gian',
                'ai': 'Ai (Người Dùng)',
                'lam_gi': 'Làm Gì (Hành Động)',
                'bang_nao': 'Bảng Nào',
                'ket_qua': 'Kết Quả',
                'run_id': 'Lần Chạy'
            })
            st.dataframe(display_audit, width="stretch", hide_index=True)
        else:
            st.info("Chưa có bản ghi audit nào phù hợp bộ lọc.")
