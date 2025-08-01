#!/usr/bin/env python3

import socket
import time
import csv
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass
import signal
import sys

@dataclass
class ADSBMessage:
    message_type: str
    transmission_type: Optional[int]
    session_id: Optional[int]
    aircraft_id: Optional[int]
    hex_ident: Optional[str]
    flight_id: Optional[int]
    date_generated: Optional[str]
    time_generated: Optional[str]
    date_logged: Optional[str]
    time_logged: Optional[str]
    callsign: Optional[str]
    altitude: Optional[int]
    ground_speed: Optional[int]
    track: Optional[int]
    latitude: Optional[float]
    longitude: Optional[float]
    vertical_rate: Optional[int]
    squawk: Optional[str]
    alert: Optional[bool]
    emergency: Optional[bool]
    spi: Optional[bool]
    is_on_ground: Optional[bool]

class TestADSBClient:
    def __init__(self, host: str = "data.adsbhub.org", port: int = 5002, test_duration: int = 30):
        self.host = host
        self.port = port
        self.test_duration = test_duration
        self.socket = None
        self.running = False
        self.start_time = None
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('test_adsb_client.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.csv_file = None
        self.csv_writer = None
        self.message_count = 0
        self.position_messages = 0
        self._setup_csv_output()
    
    def _setup_csv_output(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"test_adsb_data_{timestamp}.csv"
        
        self.csv_file = open(filename, 'w', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        
        headers = [
            'timestamp', 'message_type', 'transmission_type', 'session_id', 
            'aircraft_id', 'hex_ident', 'flight_id', 'date_generated', 
            'time_generated', 'date_logged', 'time_logged', 'callsign', 
            'altitude', 'ground_speed', 'track', 'latitude', 'longitude', 
            'vertical_rate', 'squawk', 'alert', 'emergency', 'spi', 'is_on_ground'
        ]
        self.csv_writer.writerow(headers)
        self.csv_file.flush()
        
        self.logger.info(f"Created test output file: {filename}")
        self.output_filename = filename
    
    def connect(self) -> bool:
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(10)
            self.socket.connect((self.host, self.port))
            self.logger.info(f"Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.socket = None
        if self.csv_file:
            self.csv_file.close()
        self.logger.info("Disconnected")
    
    def parse_sbs_message(self, line: str) -> Optional[ADSBMessage]:
        if not line.strip() or line.strip() == "":
            return None
        
        try:
            fields = line.strip().split(',')
            
            if len(fields) < 22 or fields[0] != "MSG":
                return None
            
            def safe_int(value: str) -> Optional[int]:
                try:
                    return int(value) if value.strip() else None
                except ValueError:
                    return None
            
            def safe_float(value: str) -> Optional[float]:
                try:
                    return float(value) if value.strip() else None
                except ValueError:
                    return None
            
            def safe_bool(value: str) -> Optional[bool]:
                if value.strip() == "1":
                    return True
                elif value.strip() == "0":
                    return False
                return None
            
            return ADSBMessage(
                message_type=fields[0],
                transmission_type=safe_int(fields[1]),
                session_id=safe_int(fields[2]),
                aircraft_id=safe_int(fields[3]),
                hex_ident=fields[4] if fields[4].strip() else None,
                flight_id=safe_int(fields[5]),
                date_generated=fields[6] if fields[6].strip() else None,
                time_generated=fields[7] if fields[7].strip() else None,
                date_logged=fields[8] if fields[8].strip() else None,
                time_logged=fields[9] if fields[9].strip() else None,
                callsign=fields[10].strip() if fields[10].strip() else None,
                altitude=safe_int(fields[11]),
                ground_speed=safe_int(fields[12]),
                track=safe_int(fields[13]),
                latitude=safe_float(fields[14]),
                longitude=safe_float(fields[15]),
                vertical_rate=safe_int(fields[16]),
                squawk=fields[17] if fields[17].strip() else None,
                alert=safe_bool(fields[18]),
                emergency=safe_bool(fields[19]),
                spi=safe_bool(fields[20]),
                is_on_ground=safe_bool(fields[21])
            )
        except Exception as e:
            self.logger.warning(f"Failed to parse message: {line.strip()} - {e}")
            return None
    
    def save_message(self, message: ADSBMessage):
        timestamp = datetime.now().isoformat()
        row = [
            timestamp, message.message_type, message.transmission_type,
            message.session_id, message.aircraft_id, message.hex_ident,
            message.flight_id, message.date_generated, message.time_generated,
            message.date_logged, message.time_logged, message.callsign,
            message.altitude, message.ground_speed, message.track,
            message.latitude, message.longitude, message.vertical_rate,
            message.squawk, message.alert, message.emergency,
            message.spi, message.is_on_ground
        ]
        self.csv_writer.writerow(row)
        self.csv_file.flush()
        
        # Track position messages
        if message.latitude is not None and message.longitude is not None:
            self.position_messages += 1
    
    def run_test(self):
        if not self.connect():
            return False
        
        self.running = True
        self.start_time = time.time()
        buffer = ""
        
        self.logger.info(f"Starting {self.test_duration} second test...")
        
        try:
            while self.running:
                # Check if test duration has elapsed
                elapsed = time.time() - self.start_time
                if elapsed >= self.test_duration:
                    self.logger.info(f"Test duration ({self.test_duration}s) completed")
                    break
                
                try:
                    self.socket.settimeout(1)  # Short timeout for test
                    data = self.socket.recv(1024).decode('utf-8', errors='ignore')
                    if not data:
                        self.logger.warning("No data received")
                        break
                    
                    buffer += data
                    
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        line = line.rstrip('\r')
                        
                        if line.strip():
                            message = self.parse_sbs_message(line)
                            if message:
                                self.save_message(message)
                                self.message_count += 1
                                
                                if self.message_count % 50 == 0:
                                    elapsed = time.time() - self.start_time
                                    self.logger.info(f"Processed {self.message_count} messages in {elapsed:.1f}s")
                
                except socket.timeout:
                    continue
                except Exception as e:
                    self.logger.error(f"Error receiving data: {e}")
                    break
        
        except KeyboardInterrupt:
            self.logger.info("Test interrupted by user")
        finally:
            elapsed = time.time() - self.start_time
            self.disconnect()
            self.logger.info(f"Test completed: {self.message_count} total messages, {self.position_messages} with position data in {elapsed:.1f}s")
            return True

def main():
    print("ADS-B Test Client - 30 second data collection test")
    print("Connecting to data.adsbhub.org:5002...")
    
    client = TestADSBClient(test_duration=30)
    success = client.run_test()
    
    if success:
        print(f"\nTest completed successfully!")
        print(f"Messages collected: {client.message_count}")
        print(f"Position messages: {client.position_messages}")
        print(f"Output file: {client.output_filename}")
    else:
        print("Test failed - check logs for details")

if __name__ == "__main__":
    main()