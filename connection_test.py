#!/usr/bin/env python3

import socket
import time
import sys

def test_connection():
    host = "data.adsbhub.org"
    port = 5002
    
    print(f"Testing connection to {host}:{port}")
    print("="*50)
    
    try:
        # Create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        
        print(f"Attempting to connect...")
        start_time = time.time()
        
        # Connect
        sock.connect((host, port))
        connect_time = time.time() - start_time
        
        print(f"✓ Connection successful in {connect_time:.2f}s")
        
        # Try to receive data
        print("Waiting for data (30 seconds)...")
        sock.settimeout(30)
        
        data_received = False
        total_bytes = 0
        message_count = 0
        
        try:
            buffer = ""
            while True:
                data = sock.recv(1024).decode('utf-8', errors='ignore')
                if not data:
                    print("Connection closed by server")
                    break
                
                if not data_received:
                    print("✓ First data received!")
                    data_received = True
                
                total_bytes += len(data)
                buffer += data
                
                # Count complete lines
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if line.strip():
                        message_count += 1
                        if message_count <= 3:
                            print(f"Sample message {message_count}: {line.strip()}")
                
                if message_count >= 10:  # Stop after 10 messages for testing
                    break
                    
        except socket.timeout:
            if data_received:
                print("Timeout after receiving some data")
            else:
                print("✗ Timeout - no data received")
        
        sock.close()
        
        print("\nTest Results:")
        print(f"- Data received: {'Yes' if data_received else 'No'}")
        print(f"- Total bytes: {total_bytes}")
        print(f"- Messages parsed: {message_count}")
        
        if not data_received:
            print("\nPossible issues:")
            print("1. Your IP address may not be registered in ADSBHub profile")
            print("2. You may need to actively feed data to ADSBHub first")
            print("3. Account registration may be required")
            print("4. Your feeder may need to be currently active")
        
        return data_received
        
    except socket.timeout:
        print("✗ Connection timeout")
        return False
    except ConnectionRefusedError:
        print("✗ Connection refused")
        return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False

if __name__ == "__main__":
    test_connection()