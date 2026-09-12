import asyncio
from bleak import BleakScanner, BleakClient


async def main():

    print("Searching...")

    device = await BleakScanner.find_device_by_name(
        "BBC micro:bit",
        timeout=10
    )

    if device is None:
        print("Micro:bit not found!")
        return

    print("Found:", device)
    print("Address:", device.address)

    print("Connecting...")

    async with BleakClient(device) as client:
        print("CONNECTED!")
        print("Services:")

        for service in client.services:
            print(service)


asyncio.run(main())