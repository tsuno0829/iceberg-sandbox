import socket

# 確認したいサーバーとポート
SPARK_HOST = "spark-connect-server"
SPARK_PORT = 15002
TIMEOUT_SECONDS = 5

try:
    # 指定したホストとポートに、タイムアウト付きで接続を試みる
    socket.create_connection((SPARK_HOST, SPARK_PORT), timeout=TIMEOUT_SECONDS)
    
    # ここまで到達すれば接続成功
    print(f"✅ Success: A connection could be established to {SPARK_HOST}:{SPARK_PORT}")

except socket.timeout:
    # タイムアウトした場合
    print(f"❌ Socket Timeout Error: Connection to {SPARK_HOST}:{SPARK_PORT} timed out after {TIMEOUT_SECONDS} seconds.")

except ConnectionRefusedError:
    # サーバーから接続を拒否された場合
    print(f"❌ Connection Refused Error: Connection to {SPARK_HOST}:{SPARK_PORT} was refused by the server.")
    
except socket.gaierror:
    # ホスト名が見つからない場合
    print(f"❌ Socket Gai Error: The hostname '{SPARK_HOST}' could not be resolved.")

except Exception as e:
    # その他のエラー
    print(f"❌ Exception Error: An unexpected error occurred: {e}")
