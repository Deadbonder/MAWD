import socket
import threading
import sys
import time

stop_event = threading.Event()


def receive_messages(client_socket):
    """Receive broadcast messages from server in separate thread."""
    while not stop_event.is_set():
        try:
            client_socket.settimeout(0.5)
            data = client_socket.recv(1024)
            if data:
                print(f"\n[data received]: {data.decode()}\n> ", end="")
            sys.stdout.flush()
        except socket.timeout:
            continue
        except:
            break


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="localhost")
    parser.add_argument("--user", default="User")
    parser.add_argument("--msg", default=None)
    args = parser.parse_args()

    server_ip = args.ip
    username = args.user

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((server_ip, 5001))
    print(f"[*] Connected to {server_ip}:5001 as {username}")

    # Start receive thread
    recv_thread = threading.Thread(target=receive_messages, args=(client,))
    recv_thread.daemon = True
    recv_thread.start()

    if args.msg:
        client.send(f"{username}: {args.msg}".encode())
        print(f"[*] Sent: {args.msg}")
        time.sleep(1)
    else:
        # Interactive mode
        print("Start typing messages (type 'bye' to disconnect):")
        while True:
            msg = input("> ")
            if msg.lower() == "bye":
                try:
                    client.send(f"{username} has left the chat".encode())
                except:
                    pass
                break
            try:
                client.send(f"{username}: {msg}".encode())
            except:
                print("[-] Connection lost")
                break

    stop_event.set()
    client.close()
    print("[*] Disconnected from server")


if __name__ == "__main__":
    main()