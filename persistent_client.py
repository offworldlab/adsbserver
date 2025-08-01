#!/usr/bin/env python3

import socket
import time
import csv
import logging
from datetime import datetime
from typing import Optional

class PersistentADSBClient:
    def __init__(self, host: str = "data.adsbhub.org", port: int = 5002):
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('persistent_adsb.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.csv_file = None
        self.csv_writer = None
        self.message_count = 0
        self.connection_attempts = 0
        self._setup_csv_output()
    
    def _setup_csv_output(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"persistent_adsb_data_{timestamp}.csv"
        
        self.csv_file = open(filename, 'w', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        
        headers = ['timestamp', 'raw_message']
        self.csv_writer.writerow(headers)
        self.csv_file.flush()
        
        self.logger.info(f"Created output file: {filename}")
        self.output_filename = filename
    
    def connect(self) -> bool:
        try:
            if self.socket:
                self.socket.close()
            
            self.connection_attempts += 1
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(15)
            self.socket.connect((self.host, self.port))
            self.logger.info(f"Connected to {self.host}:{self.port} (attempt #{self.connection_attempts})")
            return True
        except Exception as e:
            self.logger.error(f"Connection attempt #{self.connection_attempts} failed: {e}")
            return False
    
    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.socket = None
    
    def save_raw_message(self, message: str):
        timestamp = datetime.now().isoformat()
        self.csv_writer.writerow([timestamp, message.strip()])
        self.csv_file.flush()
        self.message_count += 1
    
    def run_persistent(self, duration_minutes: int = 5):
        self.running = True
        end_time = time.time() + (duration_minutes * 60)
        buffer = ""
        
        self.logger.info(f"Starting persistent collection for {duration_minutes} minutes...")
        
        while self.running and time.time() < end_time:
            if not self.connect():
                self.logger.info("Connection failed, waiting 30 seconds before retry...")
                time.sleep(30)
                continue
            
            try:
                # Set a longer timeout for data reception
                self.socket.settimeout(120)  # 2 minutes
                
                connection_start = time.time()
                no_data_timeout = 60  # Give up on this connection after 60s of no data
                
                while time.time() < end_time and (time.time() - connection_start) < no_data_timeout:
                    try:
                        data = self.socket.recv(4096).decode('utf-8', errors='ignore')
                        if not data:
                            self.logger.info("Connection closed by server")
                            break
                        
                        # Reset connection timer since we got data
                        connection_start = time.time()
                        buffer += data
                        
                        # Process complete lines
                        while '\n' in buffer:
                            line, buffer = buffer.split('\n', 1)
                            line = line.rstrip('\r')
                            
                            if line.strip():
                                self.save_raw_message(line)
                                
                                if self.message_count == 1:
                                    self.logger.info(f"First message received: {line.strip()}")
                                
                                if self.message_count % 100 == 0:
                                    elapsed = time.time() - connection_start
                                    self.logger.info(f"Received {self.message_count} messages")
                    
                    except socket.timeout:
                        self.logger.info("Socket timeout, will retry connection...")
                        break
                    except Exception as e:
                        self.logger.error(f"Error receiving data: {e}")
                        break
                
                self.disconnect()
                
                if time.time() < end_time:
                    self.logger.info("Waiting 10 seconds before reconnection...")
                    time.sleep(10)
            
            except KeyboardInterrupt:
                self.logger.info("Interrupted by user")
                break
        
        if self.csv_file:
            self.csv_file.close()
        
        self.logger.info(f"Collection completed: {self.message_count} messages, {self.connection_attempts} connection attempts")
        return self.message_count > 0

def main():
    print("Persistent ADS-B Data Client")
    print("Will attempt multiple connections over 5 minutes")
    print("Press Ctrl+C to stop early")
    
    client = PersistentADSBClient()
    success = client.run_persistent(duration_minutes=5)
    
    if success:
        print(f"\nSuccess! Collected {client.message_count} messages")
        print(f"Output file: {client.output_filename}")
    else:
        print("No data collected - check logs for details")

if __name__ == "__main__":
    main()