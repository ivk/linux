import socket
import http
from urllib.parse import urlparse, parse_qs

from socket_config import HOST, PORT


def parse_query_string(query):
    parsed_query = parse_qs(urlparse(query).query)

    if 'status' in parsed_query:
        try:
            status_code = int(parsed_query['status'][0])
            return status_code, http.HTTPStatus(status_code).phrase
        except ValueError:
            print(f"Wrong status code requested: {parsed_query['status'][0]}")
            pass

    return 200, 'OK'


def handle_client(conn):
    request = conn.recv(1024).decode('utf-8')

    if not request:
        return

    # Parse the request
    headers = request.split('\r\n')
    method, query, args = headers[0].split()

    # Get status from query
    status_code, status_text = parse_query_string(query)

    # Prepare response
    response = f"HTTP/1.1 {status_code} {status_text}\r\n"
    response += "Content-Type: text/plain\r\n\r\n"
    response += f"Request Method: {method}\r\n"
    response += f"Request Source: {conn.getpeername()}\r\n"
    response += f"Response Status: {status_code} {status_text}\r\n"

    # And add request as echo server
    response += request

    conn.send(response.encode('utf-8'))
    conn.close()


def main(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(1)

    print(f"Echo server is listening on {host}:{port}")

    while True:
        conn, addr = server_socket.accept()
        print(f"Connected by {addr}")
        handle_client(conn)


if __name__ == "__main__":
    main(HOST, PORT)