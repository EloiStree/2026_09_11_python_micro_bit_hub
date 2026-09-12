import socket
import asyncio
from bleak import BleakScanner, BleakClient


ipv4_address_to_relay_to = "127.0.0.1"
port_to_relay_to = 2510

# Micro:bit Bluetooth address
uart_address = "C5:3D:C3:B2:5F:57"

# Micro:bit UART receive-from-device characteristic
UART_RX_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"


prefix=f"uart|{uart_address}|"
prefix_as_bytes = prefix.encode("utf-8")


def push_read_byte_data_to_target(byte_data: bytes):
    # cSpell:ignore DGRAM
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    byte_data_with_prefix = prefix_as_bytes + byte_data
    sock.sendto(
        byte_data_with_prefix,
        (ipv4_address_to_relay_to, port_to_relay_to)
    )
    sock.close()


use_debug_print = False
use_debug_print = True

def handle_data(sender, data):
    queue_between_threads.put_nowait(data)
    if use_debug_print:
        print("RAW:", data)
    try:

        text = data.decode("utf-8")
        if use_debug_print:
            print("RX <-", repr(text))
    except UnicodeDecodeError:
        if use_debug_print:
            print("RX <-", data.hex())


queue_between_threads = asyncio.Queue()


async def main():

    print(f"Searching for micro:bit at {uart_address}...")

    device = await BleakScanner.find_device_by_address(
        uart_address,
        timeout=10
    )

    if device is None:
        print("Micro:bit not found!")
        return

    print("Found:", device)

    async with BleakClient(device) as client:

        print("CONNECTED!")

        characteristic = client.services.get_characteristic(
            UART_RX_UUID
        )

        if characteristic is None:
            print("UART characteristic not found!")
            return

        print("Using:", characteristic.uuid)
        print("Properties:", characteristic.properties)

        # This characteristic uses "indicate"
        await client.start_notify(
            characteristic,
            handle_data
        )

        print()
        print("Listening for Bluetooth data...")
        print("Press Ctrl+C to stop.")
        print()

        try:
            while True:
                if not queue_between_threads.empty():
                    data = await queue_between_threads.get()
                    if use_debug_print:
                        print("Pushing to target:", data)
                    push_read_byte_data_to_target(data)
                await asyncio.sleep(0.001)

        except asyncio.CancelledError:
            pass

        finally:
            await client.stop_notify(characteristic)


try:
    asyncio.run(main())

except KeyboardInterrupt:
    print("\nStopped.")