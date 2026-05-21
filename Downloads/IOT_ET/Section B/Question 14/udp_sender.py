"""
UDP Video Sender
Captures webcam frames using OpenCV, encodes as JPEG, and sends via UDP socket.
"""

import socket
import cv2

# Configuration
SERVER_IP = "127.0.0.1"
SERVER_PORT = 5005
MAX_DGRAM = 65507  # Max UDP packet size

def main():
    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Open webcam (0 = default camera)
    cap = cv2.VideoCapture(0)

    if not cap.isOpened():
        print("[!] Error: Could not open webcam")
        return

    print(f"[*] Sending webcam frames to {SERVER_IP}:{SERVER_PORT}")
    print("[*] Press Ctrl+C to stop\n")

    try:
        while True:
            # Read a frame from webcam
            ret, frame = cap.read()
            if not ret:
                print("[!] Failed to grab frame")
                break

            # Resize frame to smaller size for UDP
            frame = cv2.resize(frame, (320, 240))

            # Encode frame as JPEG (very low quality for smaller size)
            ret, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 10])
            if not ret:
                continue

            # Convert to bytes and send via UDP
            data = buffer.tobytes()
            
            # Final check to prevent crashing if a frame happens to still be too large
            if len(data) > MAX_DGRAM:
                print(f"[!] Frame too large ({len(data)} bytes), skipping...")
                continue
                
            sock.sendto(data, (SERVER_IP, SERVER_PORT))
            print(f"[>] Sent frame: {len(data)} bytes")

    except KeyboardInterrupt:
        print("\n[*] Sender stopped")
    finally:
        cap.release()
        sock.close()
        print("[*] Webcam released, socket closed")


if __name__ == "__main__":
    main()
