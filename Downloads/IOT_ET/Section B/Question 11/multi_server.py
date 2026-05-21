import socket
import threading

# Thread-safe client storage
clients = {}  # {addr: socket}
clients_lock = threading.Lock()


def broadcast(message, sender_addr):
    """Broadcast message to all clients except the sender."""
    with clients_lock:
        for addr, client_socket in list(clients.items()):
            if addr != sender_addr:
                try:
                    client_socket.send(message.encode())
                except:
                    # Remove disconnected client
                    del clients[addr]


def handle_client(conn, addr):
    """Handle individual client connection in a separate thread."""
    print(f"[+] New connection from {addr}")

    while True:
        try:
            data = conn.recv(1024)
            if not data:
                break
            message = data.decode()
            print(f"[{addr}] {message}")
            broadcast(message, addr)
        except:
            break

    print(f"[-] {addr} disconnected")
    with clients_lock:
        if addr in clients:
            del clients[addr]
    conn.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 5001))
    server.listen(5)
    print("[*] Multi-client chat server listening on port 5000...")

    while True:
        conn, addr = server.accept()
        with clients_lock:
            clients[addr] = conn
        client_thread = threading.Thread(target=handle_client, args=(conn, addr))
        client_thread.daemon = True
        client_thread.start()


if __name__ == "__main__":
    main()