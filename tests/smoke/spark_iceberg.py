"""
Unified Spark + Iceberg smoke test with a simple pre-check to avoid Spark Connect hang.

Behavior:
- Single-shot socket connectivity check to Spark Connect Server (no retries).
- Connect via Spark Connect.
- Use Iceberg catalog "my_catalog" and test table "my_db.my_table".
- Create namespace if needed, write sample data, read it back, then clean up table and namespace.
"""

import socket
import sys
from pyspark.sql import SparkSession


SPARK_HOST = "spark-connect-server"
SPARK_PORT = 15002
SOCKET_TIMEOUT_SECONDS = 5

ICEBERG_CATALOG = "my_catalog"
NAMESPACE = "my_db"
TABLE_NAME = f"{NAMESPACE}.my_table"  # resolved under current catalog


def check_connectivity(host: str, port: int, timeout_sec: int = SOCKET_TIMEOUT_SECONDS) -> None:
    """Single attempt to open a TCP connection; raises on failure."""
    socket.create_connection((host, port), timeout=timeout_sec)


def main() -> int:
    # Simple connectivity check to avoid Spark Connect hang
    try:
        check_connectivity(SPARK_HOST, SPARK_PORT)
        print(f"✅ Success: A connection could be established to {SPARK_HOST}:{SPARK_PORT}")
    except ConnectionRefusedError:
        print(f"❌ Connection Refused Error: Connection to {SPARK_HOST}:{SPARK_PORT} was refused by the server.")
        return 1
    except Exception as e:
        print(f"❌ Exception Error: An unexpected error occurred while checking connectivity: {e}")
        return 1

    spark = None
    exit_code = 0
    try:
        spark = SparkSession.builder.remote(f"sc://{SPARK_HOST}:{SPARK_PORT}").getOrCreate()
        print(f"✅ Connected to Spark! Version: {spark.version}")

        spark.catalog.setCurrentCatalog(ICEBERG_CATALOG)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

        data = [(1, "apple"), (2, "banana"), (3, "cherry")]
        df = spark.createDataFrame(data, ["id", "data"])

        df.writeTo(TABLE_NAME).createOrReplace()
        _ = spark.read.table(TABLE_NAME).limit(1).count()
        print("✅ Success: Write and read Iceberg table")

    except Exception as e:
        print(f"❌ Smoke test failed: {e}")
        exit_code = 2
    finally:
        # Cleanup (best-effort)
        try:
            if spark is not None:
                # Ensure we are in the expected catalog before cleanup
                spark.catalog.setCurrentCatalog(ICEBERG_CATALOG)
                spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
                # Drop namespace after table is removed (only if empty)
                spark.sql(f"DROP NAMESPACE IF EXISTS {NAMESPACE}")
        except Exception as cleanup_err:
            # Don't overwrite the primary failure code for cleanup errors
            print(f"Cleanup encountered an issue (ignored): {cleanup_err}")
        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception:
                    pass

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
