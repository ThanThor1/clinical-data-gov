"""Chạy toàn bộ demo governance trên dữ liệu giả với DB tạm."""

from governance.demo_governance import main


def test_demo_chay_thanh_cong(tmp_path, capsys):
    assert main(["--db", str(tmp_path / "demo.db"), "--fake"]) == 0
    assert "DEMO THÀNH CÔNG" in capsys.readouterr().out


def test_demo_chay_lai_lan_hai_van_thanh_cong(tmp_path):
    db = str(tmp_path / "demo.db")
    assert main(["--db", db, "--fake"]) == 0
    assert main(["--db", db, "--fake"]) == 0
