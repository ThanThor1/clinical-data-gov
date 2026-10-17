"""
Ứng dụng Dashboard Quản trị Dữ liệu Lâm sàng Đa Bệnh viện
Phụ trách bởi: Người 3 (Catalog, Phân quyền, Audit, Dashboard và Báo cáo)

Gồm 4 trang chức năng chuẩn:
1. Catalog: Danh sách và thông tin chi tiết các bảng dữ liệu qua 3 tầng (Raw, Standardized, AI-ready)
2. Lineage: Đồ thị trực quan đường đi dữ liệu (Data Lineage) từ nguồn bệnh viện đến AI-ready
3. Vi phạm (Violations): Cảnh báo chất lượng dữ liệu, lỗi gây hỏng (Breaking) TÔ MÀU ĐỎ và chỉ rõ bảng AI bị ảnh hưởng
4. Audit: Sổ nhật ký hành động toàn hệ thống kèm công cụ kiểm thử phân quyền RBAC trực tiếp
"""

import os
import sys
import sqlite3
import pandas as pd
import streamlit as st
import networkx as nx
import matplotlib.pyplot as plt

# Thêm thư mục gốc vào sys.path để import catalog_access
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from catalog_access.init_db import get_db_connection, DEFAULT_DB_PATH
from catalog_access.catalog import list_datasets, get_dataset
from catalog_access.audit import get_audit_logs, log_event
from catalog_access.access import check_access, load_policies, resolve_role

# Cấu hình trang Streamlit
st.set_page_config(
    page_title="Hệ Thống Quản Trị Dữ Liệu Lâm Sàng Đa Bệnh Viện",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
    .metric-card {
        background-color: #f8fafc;
        border-radius: 8px;
        padding: 16px;
        border-left: 5px solid #2563eb;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .badge-raw {
        background-color: #e0f2fe;
        color: #0369a1;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-std {
        background-color: #fef3c7;
        color: #b45309;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .badge-ai {
        background-color: #dcfce7;
        color: #15803d;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
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


def load_violations():
    """Tải danh sách các vi phạm từ metadata.db."""
    conn = get_db_connection()
    df = pd.read_sql_query("""
    SELECT id, error_type, severity, hospital, affected_table, affected_ai_table, action, details, timestamp
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


# ---------------------------------------------------------
# SIDEBAR NAVIGATION & TỔNG QUAN HỆ THỐNG
# ---------------------------------------------------------
st.sidebar.image("https://img.icons8.com/fluency/96/hospital-3.png", width=64)
st.sidebar.title("Quản Trị Lâm Sàng")
st.sidebar.caption("Đề tài 5: Quản trị dữ liệu lâm sàng đa bệnh viện (eICU Demo)")

menu = st.sidebar.radio(
    "CHỌN TRANG XEM:",
    ["1. 📚 Catalog (Danh bạ dữ liệu)",
     "2. 🔗 Lineage (Đồ thị nguồn gốc)",
     "3. ⚠️ Vi phạm (Violations & Chặn lỗi)",
     "4. 📋 Audit (Sổ nhật ký & RBAC)"],
    index=0
)

# Kiểm tra trạng thái hệ thống (Có lỗi breaking hay toàn màu xanh)
violations_df = load_violations()
has_breaking = not violations_df[violations_df['severity'] == 'BREAKING'].empty

st.sidebar.markdown("---")
st.sidebar.markdown("**TRẠNG THÁI HỆ THỐNG:**")
if has_breaking:
    st.sidebar.error("🔴 **PHÁT HIỆN LỖI BREAKING**\n\nCổng kiểm soát đã chặn dữ liệu hỏng để bảo vệ mô hình AI!")
else:
    st.sidebar.success("🟢 **HỆ THỐNG AN TOÀN**\n\nToàn bộ dữ liệu 3 bệnh viện hợp lệ và sẵn sàng.")

st.sidebar.markdown("---")
st.sidebar.markdown("""
**Phân công Người 3:**
- Danh bạ dữ liệu (Catalog)
- Phân quyền theo vai trò (RBAC)
- Sổ ghi chép (Audit Log)
- Màn hình Dashboard 4 trang
- Tài liệu phương hướng
""")


# =========================================================
# TRANG 1: CATALOG (DANH BẠ DỮ LIỆU)
# =========================================================
if "1. 📚 Catalog" in menu:
    st.markdown('<div class="main-header">📚 Metadata Catalog - Danh Bạ Bảng Dữ Liệu</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Quản lý định danh, nguồn gốc bệnh viện, tầng dữ liệu và gắn nhãn các cột nhạy cảm theo chuẩn HIPAA</div>', unsafe_allow_html=True)

    datasets_df = list_datasets(as_df=True)

    # Hiển thị số liệu thống kê (KPIs)
    col1, col2, col3, col4 = st.columns(4)
    total_ds = len(datasets_df)
    raw_ds = len(datasets_df[datasets_df['tier'] == 'raw'])
    std_ds = len(datasets_df[datasets_df['tier'] == 'standardized'])
    ai_ds = len(datasets_df[datasets_df['tier'] == 'ai_ready'])

    with col1:
        st.metric("Tổng số bảng dữ liệu", total_ds)
    with col2:
        st.metric("Tầng Thô (Raw)", raw_ds, "Từ 3 BV")
    with col3:
        st.metric("Tầng Chuẩn Hóa", std_ds, "Đã gộp schema")
    with col4:
        st.metric("Tầng AI-Ready", ai_ds, "Sạch & Ẩn danh")

    st.markdown("---")

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
    display_df = filtered_df[['id', 'name', 'hospital', 'tier', 'owner', 'sensitive_columns']].copy()
    display_df.columns = ['ID', 'Tên Bảng', 'Bệnh Viện', 'Tầng Dữ Liệu', 'Người Phụ Trách', 'Cột Nhạy Cảm (Sensitive)']
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Chi tiết Schema đã ghi nhận trong schema_versions
    with st.expander("🔍 Xem Lịch Sử Phiên Bản Schema (Schema Evolution Log)"):
        conn = get_db_connection()
        schemas_df = pd.read_sql_query("""
        SELECT table_name, version, columns_json, change_type, is_breaking, recorded_at
        FROM schema_versions ORDER BY id DESC;
        """, conn)
        conn.close()
        st.dataframe(schemas_df, use_container_width=True)


# =========================================================
# TRANG 2: LINEAGE (ĐỒ THỊ NGUỒN GỐC DỮ LIỆU)
# =========================================================
elif "2. 🔗 Lineage" in menu:
    st.markdown('<div class="main-header">🔗 Data Lineage - Đồ Thị Đường Đi Của Dữ Liệu</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Truy vết nguồn gốc: Bệnh viện nào -> Cột nào tầng Thô -> Chuẩn hóa cột nào -> Đi vào biến nào của mô hình AI</div>', unsafe_allow_html=True)

    lineage_df = load_lineage()

    col_graph, col_table = st.columns([3, 2])

    with col_graph:
        st.subheader("Trực quan hóa luồng dữ liệu (Pipeline Graph)")
        
        # Xây dựng đồ thị NetworkX
        G = nx.DiGraph()
        for _, row in lineage_df.iterrows():
            src = f"{row['source_table']}\n.{row['source_column']}"
            tgt = f"{row['target_table']}\n.{row['target_column']}"
            G.add_edge(src, tgt, label=row['transformation'])

        fig, ax = plt.subplots(figsize=(10, 6))
        pos = nx.spring_layout(G, seed=42, k=1.8)

        # Màu sắc theo tầng
        node_colors = []
        for node in G.nodes():
            if "raw" in node.lower():
                node_colors.append("#60a5fa")  # Xanh dương - raw
            elif "standardized" in node.lower():
                node_colors.append("#fbbf24")  # Vàng cam - standardized
            else:
                node_colors.append("#4ade80")  # Xanh lá - AI-ready

        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=2800, alpha=0.9, ax=ax)
        nx.draw_networkx_labels(G, pos, font_size=8, font_family="sans-serif", font_weight="bold", ax=ax)
        nx.draw_networkx_edges(G, pos, edge_color="#94a3b8", arrows=True, arrowstyle="-|>", arrowsize=18, width=1.8, ax=ax)

        ax.set_title("Sơ đồ biến đổi từ Dữ liệu thô BV -> Chuẩn hóa -> Bảng AI-Ready", fontsize=11, fontweight="bold")
        ax.axis("off")
        st.pyplot(fig)

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
            use_container_width=True,
            hide_index=True
        )

        st.info("💡 **Khả năng phân tích tác động (Impact Analysis):** Khi Người 2 phát hiện Bệnh viện B đổi tên cột `admissionweight` thành `weight`, đồ thị Lineage lập tức phát hiện cột này trực tiếp cung cấp dữ liệu cho `weight_kg` của bảng `ai_ready_icu_mortality`, từ đó chặn pipeline kịp thời!")


# =========================================================
# TRANG 3: VI PHẠM (VIOLATIONS & CHẶN LỖI)
# =========================================================
elif "3. ⚠️ Vi phạm" in menu:
    st.markdown('<div class="main-header">⚠️ Vi Phạm & Trạm Kiểm Soát Chất Lượng</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Phát hiện dữ liệu sai hợp đồng, đổi tên schema và chặn không cho chảy vào tầng AI</div>', unsafe_allow_html=True)

    if has_breaking:
        st.markdown("""
        <div class="breaking-box">
            <h3 style="margin-top:0;">🚨 CẢNH BÁO NGUY HIỂM: PHÁT HIỆN LỖI GÂY HỎNG (BREAKING CHANGE)</h3>
            <p>Hệ thống tự động phát hiện bệnh viện tự ý thay đổi cấu trúc bảng so với Data Contract cam kết. Dữ liệu đã bị <b>CHẶN LẠI</b> tại Cổng kiểm soát.</p>
        </div>
        """, unsafe_allow_html=True)

    # Số lượng vi phạm
    crit_count = len(violations_df[violations_df['severity'] == 'BREAKING'])
    warn_count = len(violations_df[violations_df['severity'] == 'WARNING'])

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Tổng số vi phạm", len(violations_df))
    with m2:
        st.metric("Lỗi gây hỏng (Breaking)", crit_count, "Tô màu đỏ (Đã chặn)")
    with m3:
        st.metric("Cảnh báo chất lượng", warn_count, "Vượt ngưỡng Kahn")

    st.markdown("---")
    st.subheader("Danh sách chi tiết vi phạm:")

    for _, v in violations_df.iterrows():
        is_break = v['severity'] == 'BREAKING'
        box_style = "border-left: 6px solid #dc2626; background-color: #fef2f2;" if is_break else "border-left: 6px solid #f59e0b; background-color: #fffbeb;"
        badge_color = "#dc2626" if is_break else "#d97706"

        st.markdown(f"""
        <div style="border-radius: 8px; padding: 14px; margin-bottom: 12px; {box_style}">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span style="font-weight:bold; font-size:16px; color:{badge_color}">
                    [{v['error_type']}] - Mức độ: {v['severity']}
                </span>
                <span style="background-color:{badge_color}; color:white; padding:3px 10px; border-radius:12px; font-size:12px; font-weight:bold;">
                    HÀNH ĐỘNG: {v['action'].upper()}
                </span>
            </div>
            <p style="margin: 8px 0 4px 0; color:#334155;"><b>Bệnh viện vi phạm:</b> {v['hospital']} | <b>Bảng phát hiện:</b> <code>{v['affected_table']}</code></p>
            <p style="margin: 4px 0; color:#b91c1c if is_break else #92400e;"><b>Bảng AI-ready bị đe dọa:</b> <code>{v['affected_ai_table'] or 'N/A'}</code></p>
            <p style="margin: 4px 0; color:#475569;"><b>Chi tiết lỗi:</b> {v['details']}</p>
            <small style="color:#94a3b8;">Thời gian ghi nhận: {v['timestamp']}</small>
        </div>
        """, unsafe_allow_html=True)


# =========================================================
# TRANG 4: AUDIT LOG & PHÂN QUYỀN RBAC
# =========================================================
elif "4. 📋 Audit" in menu:
    st.markdown('<div class="main-header">📋 Sổ Nhật Ký Kiểm Toán (Audit Log) & Kiểm Tra Phân Quyền (RBAC)</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Cuốn sổ ghi chép mọi hành động: ai, làm gì, lúc nào, trên bảng nào, kết quả ra sao</div>', unsafe_allow_html=True)

    # Phần 1: Bộ mô phỏng kiểm tra phân quyền trực tiếp (Live RBAC Test)
    st.subheader("🧪 Kiểm thử Phân quyền (RBAC Simulator)")
    st.caption("Kiểm thử tiêu chí nghiệm thu: 'Đăng nhập vai trò Nhà nghiên cứu mà đọc dữ liệu thô thì bị từ chối và ghi vào sổ audit'")

    sim_col1, sim_col2, sim_col3 = st.columns([1.5, 1.5, 1])
    
    with sim_col1:
        test_user = st.selectbox(
            "Chọn Người Dùng / Vai Trò:",
            [
                "researcher_bob (Nhà nghiên cứu)",
                "engineer_alice (Kỹ sư dữ liệu)",
                "guest_charlie (Khách xem)",
                "nhà nghiên cứu",
                "kỹ sư dữ liệu",
                "khách"
            ]
        )
    with sim_col2:
        test_table = st.selectbox(
            "Chọn Bảng Dữ Liệu Muốn Truy Cập:",
            [
                "raw_patient_hospital_B (Tầng Thô)",
                "raw_patient_hospital_A (Tầng Thô)",
                "standardized_patients (Tầng Chuẩn Hóa)",
                "ai_ready_icu_mortality (Tầng AI-Ready)"
            ]
        )
    with sim_col3:
        st.write("")
        st.write("")
        btn_test = st.button("🚀 Thử Truy Cập Dữ Liệu", use_container_width=True)

    if btn_test:
        user_id = test_user.split(" ")[0]
        table_name = test_table.split(" ")[0]
        allowed = check_access(user_id, table_name, action="read")
        
        if allowed:
            st.success(f"✅ **CHẤP THUẬN (ALLOWED):** Tài khoản `{user_id}` được phép đọc bảng `{table_name}`. Hành động đã ghi vào sổ Audit.")
        else:
            st.error(f"🛑 **TỪ CHỐI TRUY CẬP (DENIED):** Vi phạm chính sách phân quyền! `{user_id}` không có thẩm quyền đọc dữ liệu `{table_name}`. Sự việc đã được ghi nhận vào sổ Audit!")

    st.markdown("---")

    # Phần 2: Xem toàn bộ sổ Audit Log
    st.subheader("📖 Danh Sách Sổ Ghi Chép Kiểm Toán")
    
    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        res_filter = st.selectbox("Lọc theo Kết Quả:", ["Tất cả", "ALLOWED", "DENIED", "BLOCKED", "SUCCESS", "FAILED"])
    with filter_col2:
        search_actor = st.text_input("Tìm kiếm theo Tên Người Dùng (Actor):", "")

    audit_df = get_audit_logs(
        limit=200,
        result=res_filter if res_filter != "Tất cả" else None,
        actor=search_actor if search_actor.strip() else None,
        as_df=True
    )

    if not audit_df.empty:
        # Hiển thị sổ audit gồm đúng 5 thông tin cốt lõi: thời gian, ai, làm gì, bảng nào, kết quả
        display_cols = ['id', 'thoi_gian', 'ai', 'lam_gi', 'bang_nao', 'ket_qua']
        available_cols = [c for c in display_cols if c in audit_df.columns]
        display_audit = audit_df[available_cols].copy()
        display_audit = display_audit.rename(columns={
            'id': 'ID',
            'thoi_gian': 'Thời Gian',
            'ai': 'Ai (Người Dùng)',
            'lam_gi': 'Làm Gì (Hành Động)',
            'bang_nao': 'Bảng Nào',
            'ket_qua': 'Kết Quả'
        })
        st.dataframe(display_audit, use_container_width=True, hide_index=True)
    else:
        st.info("Chưa có bản ghi audit nào phù hợp bộ lọc.")
