"""
UDP Video Receiver
Receives JPEG frames via UDP, converts to grayscale, applies Canny edge detection,
and displays in a live window.
"""

import socket
import cv2
import numpy as np

# Configuration
LISTEN_IP = "127.0.0.1"
LISTEN_PORT = 5005
MAX_DGRAM = 65507  # Max UDP packet size

# Canny Edge Detection thresholds
# Threshold1 (low) = 50  : Weak edges below this are discarded
# Threshold2 (high) = 150 : Strong edges above this are kept
# Justification: A 1:3 ratio (50:150) is a commonly recommended ratio.
#   - 50 is low enough to capture faint edges (e.g., facial features, background objects)
#   - 150 is high enough to filter out noise while keeping strong edges (e.g., outlines)
CANNY_THRESHOLD1 = 50
CANNY_THRESHOLD2 = 150


def main():
    # Create UDP socket and bind to listen address
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((LISTEN_IP, LISTEN_PORT))

    print(f"[*] Listening for video frames on {LISTEN_IP}:{LISTEN_PORT}")
    print("[*] Press 'q' in the display window to stop\n")

    try:
        while True:
            # Receive data from sender
            data, addr = sock.recvfrom(MAX_DGRAM)

            # Convert bytes to numpy array
            np_data = np.frombuffer(data, dtype=np.uint8)

            # Decode JPEG bytes back into an image (frame)
            frame = cv2.imdecode(np_data, cv2.IMREAD_COLOR)

            if frame is None:
                print("[!] Failed to decode frame, skipping...")
                continue

            # --- Part B: Convert to Grayscale ---
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            # --- Part C: Apply Canny Edge Detection ---
            edges = cv2.Canny(gray, CANNY_THRESHOLD1, CANNY_THRESHOLD2)

            # Display both windows
            cv2.imshow("Grayscale Feed", gray)
            cv2.imshow("Canny Edge Detection", edges)

            # Press 'q' to quit
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

    except KeyboardInterrupt:
        print("\n[*] Receiver stopped")
    finally:
        sock.close()
        cv2.destroyAllWindows()
        print("[*] Socket closed, windows destroyed")


if __name__ == "__main__":
    main()
