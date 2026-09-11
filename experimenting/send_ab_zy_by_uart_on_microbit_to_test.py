import asyncio
from bleak import BleakScanner, BleakClient


WRITE_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"


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

    async with BleakClient(device) as client:

        print("CONNECTED!")

        write_characteristic = None

        for service in client.services:
            for characteristic in service.characteristics:

                if characteristic.uuid.lower() == WRITE_UUID.lower():
                    write_characteristic = characteristic
                    break

            if write_characteristic:
                break

        if write_characteristic is None:
            print("Write characteristic not found!")
            return

        print(
            "Using:",
            write_characteristic.uuid,
            write_characteristic.properties
        )

        for letter in "abcdefghijklmnopqrstuvwxyz":

            message = letter + "\n"
            data = message.encode("utf-8")

            await client.write_gatt_char(
                write_characteristic,
                data,
                response=False
            )

            print("TX ->", repr(message))

            await asyncio.sleep(1)


asyncio.run(main())