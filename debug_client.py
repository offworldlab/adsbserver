#!/usr/bin/env python3

import socket
import time
import select

def debug_connection():
    host = "data.adsbhub.org"
    port = 5002
    
    print(f"Debug connection to {host}:{port}")
    print("="*50)
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        
        print(f"Connecting...")
        sock.connect((host, port))
        print(f"✓ Connected successfully")
        
        # Make socket non-blocking to better debug
        sock.setblocking(False)
        
        print("Waiting for data (60 seconds)...")
        start_time = time.time()
        data_received = False
        total_data = ""
        
        while time.time() - start_time < 60:
            # Use select to check if data is available
            ready = select.select([sock], [], [], 1.0)
            
            if ready[0]:
                try:
                    data = sock.recv(4096).decode('utf-8', errors='ignore')
                    if data:
                        if not data_received:
                            print("✓ First data chunk received!")
                            data_received = True
                        total_data += data
                        print(f"Received {len(data)} bytes, total: {len(total_data)}")
                        
                        # Show first few lines
                        lines = data.split('\n')
                        for i, line in enumerate(lines[:3]):
                            if line.strip():
                                print(f"Sample: {line.strip()}")
                    else:
                        print("Connection closed by server")
                        break
                except BlockingIOError:
                    # No data available right now
                    pass
                except Exception as e:
                    print(f"Error reading data: {e}")
                    break
            else:
                # No data available, show progress
                elapsed = time.time() - start_time
                if int(elapsed) % 10 == 0 and elapsed > 0:
                    print(f"Still waiting... {elapsed:.0f}s elapsed")
        
        sock.close()
        
        print(f"\nDebug Results:")
        print(f"- Data received: {'Yes' if data_received else 'No'}")
        print(f"- Total bytes: {len(total_data)}")
        
        if total_data:
            lines = total_data.split('\n')
            valid_lines = [line for line in lines if line.strip()]
            print(f"- Valid lines: {len(valid_lines)}")
            
            # Save sample data
            with open('debug_sample.txt', 'w') as f:
                f.write(total_data)
            print("- Sample data saved to debug_sample.txt")
        
        return data_received
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    debug_connection()