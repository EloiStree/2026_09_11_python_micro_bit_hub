import asyncio
from bleak import BleakScanner, BleakClient


WRITE_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"
micro_bit_address = "C8:B0:9F:8B:37:E5"

list_of_text_to_send_in_loop = [
    "P0", "p0", "P1", "p1", "P2", "p2", "P8", "p8",
    "P12", "p12", "P13", "p13", "P14", "p14", "P15", "p15",
    "P16", "p16", "PA", "pa",
]


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


async def send_loop(client):
    write_characteristic = None
    for service in client.services:
        for characteristic in service.characteristics:
            print(
                "Service:", service.uuid,
                "Characteristic:", characteristic.uuid,
                "Properties:", characteristic.properties,
            )
            if characteristic.uuid.lower() == WRITE_UUID.lower():
                write_characteristic = characteristic

    if write_characteristic is None:
        print("Write characteristic not found!")
        return

    print("Using:", write_characteristic.uuid, write_characteristic.properties)

    # Windows + Nordic UART: write-with-response often cancels and closes the GATT object.
    use_response = False

    while True:
        for letter in list_of_text_to_send_in_loop:
            if not client.is_connected:
                print("Client disconnected!")
                return

            message = letter + "\n"
            data = message.encode("utf-8")

            try:
                await client.write_gatt_char(
                    write_characteristic,
                    data,
                    response=use_response,
                )
                print("TX ->", repr(message))
            except Exception as e:
                print("Write failed:", e)
                return

            await asyncio.sleep(0.2)


async def main():
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
                await send_loop(client)
        except Exception as e:
            print("Connection error:", e)

        print("Reconnecting...")
        await asyncio.sleep(1)


asyncio.run(main())