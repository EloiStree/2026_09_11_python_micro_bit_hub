import asyncio
from bleak import BleakScanner, BleakClient


UART_SERVICE_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"


def on_receive(sender, data):
    print("RX <-", data)
    print("Text:", data.decode("utf-8", errors="replace"))


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

        # Get the UART service
        uart_service = client.services.get_service(
            UART_SERVICE_UUID
        )

        if uart_service is None:
            print("UART service not found!")
            return

        print("UART service found!")

        # Print its characteristics
        for characteristic in uart_service.characteristics:
            print(
                characteristic.uuid,
                characteristic.properties
            )

        # Find the characteristic that supports NOTIFY
        tx_characteristic = None

        # Find the characteristic that supports WRITE
        rx_characteristic = None

        for characteristic in uart_service.characteristics:

            if "notify" in characteristic.properties:
                tx_characteristic = characteristic

            if (
                "write" in characteristic.properties
                or "write-without-response" in characteristic.properties
            ):
                rx_characteristic = characteristic

        if tx_characteristic is None:
            print("No NOTIFY characteristic found!")
            return

        if rx_characteristic is None:
            print("No WRITE characteristic found!")
            return

        print("TX characteristic:", tx_characteristic.uuid)
        print("RX characteristic:", rx_characteristic.uuid)

        # Listen
        await client.start_notify(
            tx_characteristic,
            on_receive
        )

        # Send
        message = "Hello World!\n"

        await client.write_gatt_char(
            rx_characteristic,
            message.encode("utf-8")
        )

        print("TX ->", repr(message))

        await asyncio.sleep(10)

        await client.stop_notify(tx_characteristic)


asyncio.run(main())