import argparse
import asyncio
import signal
from typing import Optional

from bleak import BleakClient, BleakScanner


# ============================================================
# CONFIGURATION
# ============================================================

# micro:bit BLE address
MICRO_BIT_ADDRESS = "DF:C4:94:6F:5D:2B"

# UDP server
UDP_LISTENER_MASK = "0.0.0.0"
UDP_LISTENER_PORT = 2511

# UDP destinations for micro:bit -> UDP
UDP_TARGETS = [
    ("127.0.0.1", 2510),
]

# Default ATT MTU to assume before negotiation (BLE minimum).
# The usable payload per write is MTU - 3 (ATT header overhead).
DEFAULT_ATT_MTU = 23

# ============================================================
# MICRO:BIT NORDIC UART SERVICE
# ============================================================

NUS_SERVICE_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"

# Python -> micro:bit (Nordic UART RX characteristic)
## DONT TOUCH THAT IT IS THE GOOD ONE
WRITE_CHARACTERISTIC_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"

# micro:bit -> Python (Nordic UART TX characteristic, notify)
## DONT TOUCH THAT IT IS THE GOOD ONE
READ_CHARACTERISTIC_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"

# On Windows, a GATT operation issued immediately after connect() can be
# cancelled by the OS with OSError(22, 'The operation was canceled by the
# user.', ..., -2147023673) while the BLE stack is still finishing
# connection-parameter negotiation. This is transient WinRT/Bleak flakiness,
# not a logic error. We wait briefly after connecting and retry the
# subscribe step a few times before treating it as a real failure.
POST_CONNECT_SETTLE_SECONDS = 2.0
START_NOTIFY_MAX_ATTEMPTS = 4
START_NOTIFY_RETRY_DELAY_SECONDS = 1.5

# Windows' WinRT BLE backend caches GATT services/CCCD handles per
# device address. If a previous run left a stale cache (or the
# micro:bit's GATT table changed), operations like start_notify() get
# cancelled by the OS every single time, immediately followed by a
# disconnect -- retrying doesn't help because the cache never gets
# refreshed. Passing use_cached_services=False forces Windows to
# re-discover services on every connection instead of trusting the
# cache. This kwarg is WinRT-specific; other backends (Linux/macOS)
# simply ignore it.
BLEAK_CLIENT_KWARGS = {
    "winrt": {"use_cached_services": False},
}


# ============================================================
# DEBUG
# ============================================================

USE_DEBUG_PRINT = True
USE_DEBUG_PRINT = False


def debug(*args):
    if USE_DEBUG_PRINT:
        print(*args)


# ============================================================
# QUEUES
# ============================================================

udp_to_uart_queue: asyncio.Queue = asyncio.Queue()
uart_to_udp_queue: asyncio.Queue = asyncio.Queue()


# ============================================================
# BLE / UDP GLOBAL STATE
# ============================================================

ble_client: Optional[BleakClient] = None
ble_write_characteristic = None
ble_receive_characteristic = None

# The asyncio UDP transport created in main(); reused for sending
# so we don't need a second, separately-managed socket.
udp_transport: Optional[asyncio.DatagramTransport] = None


# ============================================================
# PREFIX
# ============================================================

def get_udp_prefix() -> bytes:
    return f"uart|{MICRO_BIT_ADDRESS}|".encode("utf-8")


# ============================================================
# UDP RECEIVER
# ============================================================

class UDPProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport

        print()
        print("==========================================")
        print("UDP LISTENER ACTIVE")
        print(f"Listening on {UDP_LISTENER_MASK}:{UDP_LISTENER_PORT}")
        print("==========================================")
        print()

    def datagram_received(self, data, addr):
        data = bytes(data)
        print(f"UDP RX <- {addr}: {data!r}")
        udp_to_uart_queue.put_nowait(data)

    def error_received(self, exc):
        print("UDP error:", repr(exc))

    def connection_lost(self, exc):
        debug("UDP socket closed:", repr(exc))


# ============================================================
# UDP -> BLE
# ============================================================

def chunk_payload(payload: bytes, chunk_size: int):
    """Split payload into chunk_size-sized pieces (at least 1 byte)."""
    chunk_size = max(1, chunk_size)
    for i in range(0, len(payload), chunk_size):
        yield payload[i:i + chunk_size]


async def ble_sender():

    global ble_client
    global ble_write_characteristic

    while True:

        data = await udp_to_uart_queue.get()

        # Treat the payload as text and frame it with a trailing
        # newline, since the micro:bit side reads UART data line by
        # line. Bytes that aren't valid UTF-8 are dropped rather than
        # raising, so a stray binary packet can't crash the sender.
        text = data.decode("utf-8", errors="ignore")
        text = text + "\n"
        data = text.encode("utf-8")

        try:

            if (
                ble_client is None
                or not ble_client.is_connected
                or ble_write_characteristic is None
            ):
                print(
                    "BLE not connected."
                    f" UDP packet cannot be sent: {data!r}"
                )
                continue

            characteristic = ble_write_characteristic
            properties = {p.lower() for p in characteristic.properties}

            # ------------------------------------------------
            # Send the UDP data exactly as received (no prefix
            # is added going towards the micro:bit).
            # ------------------------------------------------

            payload = data

            print(f"BLE TX REQUEST <- UDP: {payload!r}")
            print(f"BLE write properties: {characteristic.properties}")

            if "write-without-response" in properties:
                use_response = False
            elif "write" in properties:
                use_response = True
            else:
                raise RuntimeError(
                    "Characteristic does not support write or "
                    f"write-without-response: {characteristic.properties}"
                )

            # ------------------------------------------------
            # Respect the negotiated ATT MTU. BLE writes are
            # limited to (MTU - 3) usable bytes; larger UDP
            # payloads must be chunked or they will be silently
            # truncated / rejected by the peripheral.
            # ------------------------------------------------

            mtu = getattr(ble_client, "mtu_size", DEFAULT_ATT_MTU) or DEFAULT_ATT_MTU
            max_chunk = max(1, mtu - 3)

            mode_label = "write-with-response" if use_response else "write-without-response"

            for chunk in chunk_payload(payload, max_chunk):

                print(f"BLE write mode: {mode_label} ({len(chunk)} bytes)")

                await ble_client.write_gatt_char(
                    characteristic.uuid,
                    chunk,
                    response=use_response,
                )

            print(f"BLE TX OK -> {payload!r}")

        except Exception as exc:
            print()
            print("BLE TX FAILED")
            print("Data:", repr(data))
            print("Error:", repr(exc))
            print()

        finally:
            udp_to_uart_queue.task_done()


# ============================================================
# BLE RECEIVE CALLBACK
# ============================================================

def uart_notification_handler(sender, data):
    payload = bytes(data)
    print(f"BLE RX <- {payload!r}")
    uart_to_udp_queue.put_nowait(payload)


# ============================================================
# BLE -> UDP
# ============================================================

async def udp_sender():
    """
    Forward micro:bit -> UDP data using the same asyncio
    DatagramTransport created for listening, instead of a
    second raw socket. This avoids having to manage a
    non-blocking socket by hand for loop.sock_sendto().
    """

    while True:

        data = await uart_to_udp_queue.get()

        try:

            if udp_transport is None:
                print("UDP transport not ready; dropping BLE packet:", repr(data))
                continue

            payload = get_udp_prefix() + data

            for ip, port in UDP_TARGETS:
                udp_transport.sendto(payload, (ip, port))
                print(f"UDP TX -> {ip}:{port}: {payload!r}")

        except Exception as exc:
            print("UDP TX FAILED:", repr(exc))

        finally:
            uart_to_udp_queue.task_done()


# ============================================================
# FIND MICRO:BIT
# ============================================================

async def find_microbit():

    print(f"Searching for micro:bit {MICRO_BIT_ADDRESS} ...")

    try:
        return await BleakScanner.find_device_by_address(
            MICRO_BIT_ADDRESS,
            timeout=10,
        )
    except Exception as exc:
        print("BLE scan failed:", repr(exc))
        return None


# ============================================================
# FIND GATT CHARACTERISTICS
# ============================================================

def find_characteristics(client):

    write_characteristic = None
    receive_characteristic = None

    print()
    print("==========================================")
    print("GATT CHARACTERISTICS")
    print("==========================================")

    for service in client.services:

        print(f"Service: {service.uuid}")

        for characteristic in service.characteristics:

            properties = {p.lower() for p in characteristic.properties}

            print(f"  Characteristic: {characteristic.uuid}")
            print(f"  Properties: {characteristic.properties}")

            uuid = characteristic.uuid.lower()

            if uuid == WRITE_CHARACTERISTIC_UUID.lower():
                if "write" in properties or "write-without-response" in properties:
                    write_characteristic = characteristic
                    print("  >>> FOUND UART WRITE")

            if uuid == READ_CHARACTERISTIC_UUID.lower():
                if "notify" in properties or "indicate" in properties:
                    receive_characteristic = characteristic
                    print("  >>> FOUND UART RECEIVE")

    print("==========================================")
    print()

    return write_characteristic, receive_characteristic


# ============================================================
# BLE CONNECTION
# ============================================================

async def ble_connection_loop():

    global ble_client
    global ble_write_characteristic
    global ble_receive_characteristic

    # Growing delay between reconnect attempts. Resets to the base
    # value once a connection makes it all the way to "active". This
    # avoids hammering the adapter/device every 1s when something
    # (e.g. a stale Windows GATT cache, or the micro:bit still settling
    # after a forced disconnect) is causing every attempt to fail the
    # same way.
    base_retry_seconds = 1.0
    max_retry_seconds = 10.0
    retry_seconds = base_retry_seconds

    while True:

        device = await find_microbit()

        if device is None:
            print("micro:bit not found.")
            print(f"Retrying in {retry_seconds:.1f} seconds...")
            await asyncio.sleep(retry_seconds)
            retry_seconds = min(retry_seconds * 2, max_retry_seconds)
            continue

        print("Found micro:bit:", device)

        client = None
        # NOTE: these must be initialized *before* the try block.
        # If client.connect() (or anything before find_characteristics())
        # raises, the `finally` clause below still references these
        # names -- leaving them unassigned would raise an
        # UnboundLocalError on the very first failed connection attempt.
        write_characteristic = None
        receive_characteristic = None

        try:

            def disconnected_callback(_client):
                print()
                print("BLE DISCONNECTED")
                print()

            client = BleakClient(
                device,
                disconnected_callback=disconnected_callback,
                **BLEAK_CLIENT_KWARGS,
            )

            print("Connecting to micro:bit...")

            await asyncio.wait_for(client.connect(), timeout=15)

            if not client.is_connected:
                raise RuntimeError("Bleak did not establish a BLE connection.")

            ble_client = client

            print()
            print("==========================================")
            print("BLE CONNECTED")
            print(f"Negotiated MTU: {getattr(client, 'mtu_size', 'unknown')}")
            print("==========================================")
            print()

            write_characteristic, receive_characteristic = find_characteristics(client)

            if write_characteristic is None:
                raise RuntimeError(
                    "UART write characteristic was not found or is not "
                    f"writable.\nExpected UUID: {WRITE_CHARACTERISTIC_UUID}"
                )

            if receive_characteristic is None:
                raise RuntimeError(
                    "UART receive characteristic was not found or does "
                    f"not support notify/indicate.\nExpected UUID: {READ_CHARACTERISTIC_UUID}"
                )

            ble_write_characteristic = write_characteristic
            ble_receive_characteristic = receive_characteristic

            print()
            print("BLE WRITE:")
            print(f"  UUID: {write_characteristic.uuid}")
            print(f"  Properties: {write_characteristic.properties}")
            print()
            print("BLE RECEIVE:")
            print(f"  UUID: {receive_characteristic.uuid}")
            print(f"  Properties: {receive_characteristic.properties}")
            print()

            # Let Windows finish connection-parameter negotiation before
            # touching GATT, to avoid the transient "operation was
            # canceled by the user" cancellation described above.
            await asyncio.sleep(POST_CONNECT_SETTLE_SECONDS)

            print("Starting BLE UART receive subscription...")

            for attempt in range(1, START_NOTIFY_MAX_ATTEMPTS + 1):

                try:
                    await client.start_notify(
                        receive_characteristic.uuid,
                        uart_notification_handler,
                    )
                    break

                except OSError as exc:
                    # WinError -2147023673 / errno 22 "The operation was
                    # canceled by the user." -- transient, worth retrying.
                    if attempt == START_NOTIFY_MAX_ATTEMPTS:
                        raise

                    print(
                        f"start_notify attempt {attempt} failed "
                        f"({exc!r}); retrying in "
                        f"{START_NOTIFY_RETRY_DELAY_SECONDS}s..."
                    )

                    if not client.is_connected:
                        # The OS tore down the connection along with the
                        # cancelled operation; no point retrying, let the
                        # outer loop reconnect from scratch.
                        raise

                    await asyncio.sleep(START_NOTIFY_RETRY_DELAY_SECONDS)

            print()
            print("==========================================")
            print("UART BRIDGE ACTIVE")
            print("==========================================")
            print(f"UDP RX : {UDP_LISTENER_MASK}:{UDP_LISTENER_PORT}")
            print("BLE TX : micro:bit UART")
            print()
            print("BLE RX : micro:bit UART")
            print("UDP TX :")

            for target in UDP_TARGETS:
                print(f"  {target[0]}:{target[1]}")

            print("==========================================")
            print()

            # We made it all the way to a working, subscribed
            # connection -- reset the backoff so a later disconnect
            # starts retrying quickly again rather than inheriting a
            # long delay from earlier failures.
            retry_seconds = base_retry_seconds

            while client.is_connected:
                await asyncio.sleep(0.25)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            print()
            print("BLE CONNECTION ERROR:")
            print(repr(exc))
            print()

        finally:

            if client is not None and receive_characteristic is not None:
                try:
                    if client.is_connected:
                        await client.stop_notify(receive_characteristic.uuid)
                except Exception as exc:
                    debug("stop_notify failed:", repr(exc))

            if client is not None:
                try:
                    if client.is_connected:
                        await client.disconnect()
                except Exception as exc:
                    debug("disconnect failed:", repr(exc))

            ble_client = None
            ble_write_characteristic = None
            ble_receive_characteristic = None

        print(f"Reconnecting in {retry_seconds:.1f} seconds...")
        await asyncio.sleep(retry_seconds)
        retry_seconds = min(retry_seconds * 2, max_retry_seconds)


# ============================================================
# COMMAND LINE
# ============================================================

def parse_target(value: str):
    try:
        host, port_str = value.rsplit(":", 1)
        port = int(port_str)

        if not (1 <= port <= 65535):
            raise ValueError("port outside valid range")

        return host, port

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid UDP target {value!r}. Expected IP:PORT"
        ) from exc


def parse_args():

    parser = argparse.ArgumentParser(
        description="UDP <-> micro:bit BLE UART bridge"
    )

    parser.add_argument(
        "--microbit",
        default=MICRO_BIT_ADDRESS,
        help="micro:bit BLE address",
    )

    parser.add_argument(
        "--mask",
        default=UDP_LISTENER_MASK,
        help="UDP listen address",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=UDP_LISTENER_PORT,
        help="UDP listen port",
    )

    parser.add_argument(
        "--target",
        action="append",
        metavar="IP:PORT",
        help=(
            "UDP destination for micro:bit -> UDP. "
            "Can be specified multiple times."
        ),
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Disable debug-level print output.",
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

async def main():

    global MICRO_BIT_ADDRESS
    global UDP_LISTENER_MASK
    global UDP_LISTENER_PORT
    global UDP_TARGETS
    global USE_DEBUG_PRINT
    global udp_transport

    args = parse_args()

    MICRO_BIT_ADDRESS = args.microbit
    UDP_LISTENER_MASK = args.mask
    UDP_LISTENER_PORT = args.port

    if args.quiet:
        USE_DEBUG_PRINT = False

    if args.target:
        UDP_TARGETS = [parse_target(t) for t in args.target]

    loop = asyncio.get_running_loop()

    transport, _protocol = await loop.create_datagram_endpoint(
        UDPProtocol,
        local_addr=(UDP_LISTENER_MASK, UDP_LISTENER_PORT),
    )
    udp_transport = transport

    tasks = [
        asyncio.create_task(ble_sender(), name="udp-to-ble"),
        asyncio.create_task(udp_sender(), name="ble-to-udp"),
        asyncio.create_task(ble_connection_loop(), name="ble-connection"),
    ]

    def _handle_signal(sig: signal.Signals):
        # Cancelling the tasks here (rather than just letting the
        # process die) is what lets the `finally` block below run and
        # cleanly disconnect from the micro:bit instead of leaving the
        # BLE stack thinking a connection is still open.
        print(f"\nReceived {sig.name}, shutting down...")
        for task in tasks:
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except (NotImplementedError, RuntimeError):
            # add_signal_handler isn't available on some platforms
            # (e.g. Windows' default ProactorEventLoop). On those,
            # Ctrl+C surfaces as KeyboardInterrupt instead, which
            # asyncio.run()'s own shutdown sequence still routes back
            # through this task's cancellation -- so the `finally`
            # block below still runs and still disconnects cleanly.
            pass

    try:
        # Any unhandled exception raised inside one of the tasks (a
        # crash) propagates out of gather() and still hits `finally`
        # below, since Python always runs `finally` regardless of how
        # the `try` block exits.
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        pass

    finally:
        print("Stopping bridge...")

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        udp_transport.close()
        udp_transport = None

        # Safety net: ble_connection_loop() normally disconnects itself
        # on cancellation/crash, but if it was cancelled before it got
        # that far, or ble_client was left in a stale connected state,
        # make sure we don't leave the BLE link open on exit.
        if ble_client is not None and ble_client.is_connected:
            try:
                await ble_client.disconnect()
            except Exception as exc:
                debug("final disconnect failed:", repr(exc))


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print()
        print("Bridge stopped.")
