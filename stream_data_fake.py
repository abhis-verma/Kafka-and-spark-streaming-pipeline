import socket
import time
import random

HOST = 'localhost'
PORT = 9999

def start_server():

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"📡 Weather Station is live! Listening on {PORT}...")
    print("Waiting for Spark to connect...")


    conn, addr = s.accept()
    print(f"✅ Connected by {addr}")

    try:
        while True:
            
            city = random.choice(["London", "New York", "Tokyo", "Paris"])
            temp = random.randint(10, 40)
            message = f"{city},{temp}\n"  

            
            conn.send(message.encode('utf-8'))
            print(f"Sent: {message.strip()}")
            time.sleep(1) 
    except (BrokenPipeError, ConnectionResetError):
        print("❌ Client disconnected.")
    finally:
        conn.close()

if __name__ == "__main__":
    start_server()