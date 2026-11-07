"""
SparkSession dùng chung cho pipeline: Apache Spark + Delta Lake.

Cấu hình bằng biến môi trường:
    SPARK_MASTER              local[*] (mặc định: chạy trên một máy, dùng mọi core)
                              spark://spark-master:7077 (cụm Spark trong docker-compose)
    SPARK_DRIVER_MEMORY       bộ nhớ của driver, mặc định 2g
    SPARK_EXECUTOR_MEMORY     bộ nhớ mỗi executor khi chạy trên cụm, mặc định 1g
    SPARK_SHUFFLE_PARTITIONS  số phân vùng khi shuffle (groupBy / join), mặc định 8.
                              Mặc định 200 của Spark sinh quá nhiều tác vụ rỗng với dữ liệu cỡ vài triệu dòng.
    SPARK_UI                  1 (mặc định) -> giao diện theo dõi job ở http://localhost:4040 khi đang chạy
    SPARK_DRIVER_HOST         địa chỉ của driver mà executor gọi ngược lại (chỉ cần khi chạy trên cụm);
                              "auto" -> lấy địa chỉ IP của máy / container đang chạy driver

Lần chạy đầu tiên Spark tải jar Delta Lake từ Maven (khoảng 90 MB, lưu ở ~/.ivy2.5.2), các lần sau dùng lại.
Cần Java 17 trở lên.
"""

import os
import warnings

# Pipeline không chuyển dữ liệu qua lại giữa pandas và Spark (không dùng toPandas / pandas-on-Spark)
warnings.filterwarnings("ignore", message="PySpark does not yet fully support pandas")

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def get_spark(app_name: str = "clinical-data-gov"):
    """SparkSession đang chạy, hoặc tạo mới theo cấu hình ở đầu file."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.enabled", "true" if os.environ.get("SPARK_UI", "1") == "1" else "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        # Delta: không cho tự gộp file nhỏ / tối ưu ngầm để số file, số phiên bản dễ theo dõi trong provenance
        .config("spark.databricks.delta.autoCompact.enabled", "false")
    )
    driver_host = os.environ.get("SPARK_DRIVER_HOST")
    if driver_host:
        if driver_host == "auto":
            import socket
            driver_host = socket.gethostbyname(socket.gethostname())
        builder = (builder.config("spark.driver.host", driver_host)
                   .config("spark.driver.bindAddress", "0.0.0.0"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_spark() -> None:
    """Dừng SparkSession đang chạy (nếu có), ví dụ để chạy lại với số core khác khi đo hiệu năng."""
    from pyspark.sql import SparkSession

    phien = SparkSession.getActiveSession()
    if phien is not None:
        phien.stop()


def spark_available() -> bool:
    """Có chạy được Spark không (đã cài pyspark + delta-spark và có Java)."""
    import shutil
    try:
        import delta  # noqa: F401
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return bool(os.environ.get("JAVA_HOME") or shutil.which("java"))
