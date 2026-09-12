import time
import socket

target_ip = "127.0.0.1"
target_port = 2511

list_of_text_to_send_in_loop = [
    "P0", "p0", "P1", "p1", "P2", "p2", "P8", "p8",
    "P12", "p12", "P13", "p13", "P14", "p14", "P15", "p15",
    "P16", "p16", "PA", "pa",
]

list_of_text_to_send_in_loop = [
    "p0", "P0", "p1", "P1", "p2", "P2", "p8", "P8",
    "p12", "P12", "p13", "P13", "p14", "P14", "p15", "P15",
    "p16", "P16", "pa", "PA",
]

while True:
    for message in list_of_text_to_send_in_loop:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto((message+"\r\n").encode(), (target_ip, target_port))
        time.sleep(1)
        sock.close()

