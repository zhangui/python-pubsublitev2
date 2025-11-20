import socket
import sys

def check_port(host, port):
    print(f"Checking connectivity to {host}:{port}...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        if result == 0:
            print("Success: Port is open and reachable.")
            return True
        else:
            print(f"Failure: Port is not reachable (Error code: {result})")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False
    finally:
        sock.close()

if __name__ == "__main__":
    # Default from cursor_operations.py
    host = "bootstrap.testpsl.us-central1.managedkafka.ygnahz-eg-codelab.cloud.goog"
    port = 9092
    
    if len(sys.argv) > 1:
        parts = sys.argv[1].split(':')
        host = parts[0]
        if len(parts) > 1:
            port = int(parts[1])
            
    check_port(host, port)
