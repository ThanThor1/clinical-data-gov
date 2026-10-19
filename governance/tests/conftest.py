"""Fixture dùng chung cho các test của governance: mỗi test có một metadata.db tạm riêng."""

import pytest

from catalog_access.init_db import init_database


@pytest.fixture
def db_path(tmp_path):
    """DB tạm, KHÔNG nạp dữ liệu mẫu (seed_sample_data=False), không đụng metadata.db thật."""
    duong_dan = str(tmp_path / "test_metadata.db")
    init_database(duong_dan, seed_sample_data=False)
    return duong_dan
