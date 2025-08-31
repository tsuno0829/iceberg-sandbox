"""
Unified Spark + Iceberg smoke test with a simple pre-check to avoid Spark Connect hang.

Behavior:
- Socket connectivity check to Spark Connect Server with retries for up to 3 minutes.
- Connect via Spark Connect.
- Use Iceberg catalog "my_catalog" and test table "my_db.my_table".
- Create namespace if needed, write sample data, read it back, then clean up table and namespace.
"""

import socket
import sys
import time
from pyspark.sql import SparkSession


SPARK_HOST = "spark-connect-server"
SPARK_PORT = 15002
SOCKET_TIMEOUT_SECONDS = 5
MAX_WAIT_TIME_SECONDS = 180  # 3 minutes maximum wait time
RETRY_INTERVAL_SECONDS = 5   # Time between retry attempts

ICEBERG_CATALOG = "my_catalog"
NAMESPACE = "my_db"
TABLE_NAME = f"{NAMESPACE}.my_table"  # resolved under current catalog


def check_connectivity(host: str, port: int, timeout_sec: int = SOCKET_TIMEOUT_SECONDS) -> None:
    """Attempt to open a TCP connection with retries for up to 3 minutes."""
    start_time = time.time()
    last_exception = None
    
    while time.time() - start_time < MAX_WAIT_TIME_SECONDS:
        try:
            socket.create_connection((host, port), timeout=timeout_sec)
            return  # Connection successful
        except Exception as e:
            last_exception = e
            elapsed = int(time.time() - start_time)
            remaining = MAX_WAIT_TIME_SECONDS - elapsed
            print(f"Connection attempt failed ({elapsed}s elapsed, {remaining}s remaining): {e}")
            if remaining > 0:
                time.sleep(min(RETRY_INTERVAL_SECONDS, remaining))
    
    # If we get here, we've timed out
    raise TimeoutError(f"Could not connect to {host}:{port} after {MAX_WAIT_TIME_SECONDS} seconds. Last error: {last_exception}")


def main() -> int:
    # Connectivity check with retries for up to 3 minutes
    try:
        print(f"Checking connectivity to {SPARK_HOST}:{SPARK_PORT} (will retry for up to {MAX_WAIT_TIME_SECONDS} seconds)...")
        check_connectivity(SPARK_HOST, SPARK_PORT)
        print(f"✅ Success: A connection could be established to {SPARK_HOST}:{SPARK_PORT}")
    except TimeoutError as e:
        print(f"❌ Timeout Error: {e}")
        return 1
    except Exception as e:
        print(f"❌ Exception Error: An unexpected error occurred while checking connectivity: {e}")
        return 1
    
    # ... rest of the code remains unchanged
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