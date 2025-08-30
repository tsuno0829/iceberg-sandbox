"""
Simple health check for Spark Connect server
"""
import sys
import time
from pyspark.sql import SparkSession

def check_spark_server(max_retries=3, retry_interval=2):
    """
    Attempts to connect to the Spark Connect server and returns 
    success or failure with appropriate exit code
    """
    print("🔍 Checking Spark Connect server status...")
    
    for attempt in range(1, max_retries + 1):
        try:
            # Attempt to create a SparkSession
            spark = SparkSession.builder.remote("sc://spark-connect-server:15002").getOrCreate()
            
            # If successful, print version and exit
            print(f"✅ Spark Connect server is running! Version: {spark.version}")
            spark.stop()
            return 0
            
        except Exception as e:
            if attempt < max_retries:
                print(f"⚠️ Attempt {attempt} failed. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print(f"❌ Failed to connect to Spark Connect server after {max_retries} attempts.")
                print(f"Error: {str(e)}")
                return 1

if __name__ == "__main__":
    sys.exit(check_spark_server())
