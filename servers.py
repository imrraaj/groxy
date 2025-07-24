#!/usr/bin/env python3

import http.server
import socketserver
import json
import threading
import signal
import sys
import socket

class TestServerHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, server_id, *args, **kwargs):
        self.server_id = server_id
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'status': 'healthy', 'server': f'Server {self.server_id}'}
            self.wfile.write(json.dumps(response).encode())
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Server', f'TestServer-{self.server_id}')
        self.end_headers()
        
        headers = dict(self.headers)
        
        response = {
            'server': f'Server {self.server_id}',
            'message': f'Hello from server {self.server_id}',
            'path': self.path,
            'method': self.command,
            'headers': headers,
            'client_address': f'{self.client_address[0]}:{self.client_address[1]}'
        }
        
        self.wfile.write(json.dumps(response, indent=2).encode())

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else ''
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Server', f'TestServer-{self.server_id}')
        self.end_headers()
        
        headers = dict(self.headers)
        
        response = {
            'server': f'Server {self.server_id}',
            'message': f'Hello from server {self.server_id}',
            'path': self.path,
            'method': self.command,
            'headers': headers,
            'body': post_data,
            'client_address': f'{self.client_address[0]}:{self.client_address[1]}'
        }
        
        self.wfile.write(json.dumps(response, indent=2).encode())

    def log_message(self, format, *args):
        pass  # Disable default logging

class ReuseAddrTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

def create_handler(server_id):
    def handler(*args, **kwargs):
        return TestServerHandler(server_id, *args, **kwargs)
    return handler

def start_server(port, server_id):
    handler = create_handler(server_id)
    
    try:
        with ReuseAddrTCPServer(('', port), handler) as httpd:
            print(f"Server {server_id} running on port {port}")
            httpd.serve_forever()
    except Exception as e:
        print(f"Failed to start server {server_id} on port {port}: {e}")

def signal_handler(sig, frame):
    print("\nShutting down servers...")
    sys.exit(0)

def main():
    ports = [8001, 8002]
    
    # Set up signal handler for clean shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    threads = []
    
    for i, port in enumerate(ports, 1):
        thread = threading.Thread(target=start_server, args=(port, i))
        thread.daemon = True
        thread.start()
        threads.append(thread)
        
    # Keep main thread alive
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nShutting down servers...")

if __name__ == "__main__":
    main()
