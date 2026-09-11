import asyncio
from bleak import BleakScanner


async def main():
    print("Scanning...")

    devices = await BleakScanner.discover(timeout=10)

    for device in devices:
        print(device)


asyncio.run(main())