
import asyncio
import argparse
from bleak import BleakScanner, BleakClient


micro_bit_address = "C8:B0:9F:8B:37:E5"
micro_bit_address = "ED:C0:2D:0F:67:4C"
micro_bit_address = "DF:C4:94:6F:5D:2B"
UDP_HOST = "0.0.0.0"
UDP_PORT = 2511
## BLE write characteristic UUID for the micro:bit USE ON RELY
WRITE_UUID = "e97d3b10-251d-470a-a062-fa1922dfa9a8"
WRITE_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"




def read_params():
    parser = argparse.ArgumentParser(
        description="Forward UDP messages to a micro:bit over BLE."
    )
    parser.add_argument(
        "--mask",
        default=UDP_HOST,
        help=f"UDP bind address (default: {UDP_HOST})",
    )
    parser.add_argument(
        "--microbit",
        default=micro_bit_address,
        help=f"micro:bit Bluetooth address (default: {micro_bit_address})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=UDP_PORT,
        help=f"UDP listening port (default: {UDP_PORT})",
    )
    return parser.parse_args()


async def find_device():
    print("Searching...")

    device = await BleakScanner.find_device_by_address(
        micro_bit_address,
        timeout=10,
    )

    if device is None:
        print("Not found by address, trying by name...")

        device = await BleakScanner.find_device_by_name(
            "BBC micro:bit",
            timeout=10,
        )

    return device


class UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, message_queue):
        self.message_queue = message_queue

    def connection_made(self, transport):
        self.transport = transport

        sock = transport.get_extra_info("socket")
        print(f"UDP listening on {UDP_HOST}:{UDP_PORT}")

    def datagram_received(self, data, addr):
        try:
            message = data.decode("utf-8").strip()
        except UnicodeDecodeError:
            print("Received invalid UTF-8 from", addr)
            return

        print(f"UDP RX <- {addr}: {message!r}")

        # Put the message into the async queue.
        self.message_queue.put_nowait(message)

    def error_received(self, exc):
        print("UDP error:", exc)

    def connection_lost(self, exc):
        print("UDP socket closed")


async def send_udp_messages(client, write_characteristic, message_queue):
    """
    Wait for UDP messages and forward them to the micro:bit.
    """

    # Windows + Nordic UART:
    # write-with-response can cause problems.
    use_response = False

    while True:
        if not client.is_connected:
            print("Client disconnected!")
            return

        # Wait until a UDP packet arrives.
        message = await message_queue.get()

        # Add newline because your previous messages used "\n".
        message = message + "\n"
        data = message.encode("utf-8")

        try:
            await client.write_gatt_char(
                write_characteristic,
                data,
                response=use_response
            )

            print("BLE TX ->", repr(message))

        except Exception as e:
            print("Write failed:", e)
            return


async def find_write_characteristic(client):
    write_characteristic = None

    for service in client.services:
        for characteristic in service.characteristics:
            print(
                "Service:", service.uuid,
                "Characteristic:", characteristic.uuid,
                "Properties:", characteristic.properties,
            )

            if characteristic.uuid.lower() == WRITE_UUID.lower():
                properties = {property.lower() for property in characteristic.properties}
                if "write" in properties or "write-without-response" in properties:
                    write_characteristic = characteristic
                else:
                    print("Configured characteristic does not support writing.")

    return write_characteristic


async def main():
    # Queue shared between UDP listener and BLE sender.
    message_queue = asyncio.Queue()

    # Start UDP listener.
    loop = asyncio.get_running_loop()

    udp_transport, udp_protocol = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(message_queue),
        local_addr=(UDP_HOST, UDP_PORT),
    )

    try:
        while True:
            device = await find_device()

            if device is None:
                print("Micro:bit not found!")
                await asyncio.sleep(2)
                continue

            print("Found:", device)

            try:
                async with BleakClient(device) as client:
                    print("CONNECTED!", client.is_connected)

                    write_characteristic = await find_write_characteristic(client)

                    if write_characteristic is None:
                        print("Write characteristic not found!")
                        await asyncio.sleep(2)
                        continue

                    print(
                        "Using:",
                        write_characteristic.uuid,
                        write_characteristic.properties,
                    )

                    # Forward UDP -> BLE until BLE disconnects.
                    await send_udp_messages(
                        client,
                        write_characteristic,
                        message_queue,
                    )

            except Exception as e:
                print("Connection error:", e)

            print("Reconnecting...")
            await asyncio.sleep(1)

    finally:
        udp_transport.close()


if __name__ == "__main__":
    params = read_params()
    UDP_HOST = params.mask
    micro_bit_address = params.microbit
    UDP_PORT = params.port
    asyncio.run(main())