#!/usr/bin/env python3
"""
Reproduce the initialConnectionComplete / onClose race condition.

The race is a result of attempting to close a client connection to the
broker while the broker is still processing the initial connection in
TCPSessionFactory::initialConnectionComplete, which holds a mutex that
is also held by TCPSessionFactory::onClose. If the onClose call gets
the lock before the connection is added to the d_channels map, then the channel
will be orphaned and prevent the broker from shutting down cleanly.

How to reproduce:
1. Start the broker with a delay in TCPSessionFactory::initialConnectionComplete
(see the commented-out line in that file).
2. Send a negotiation packet to the broker and close the socket immediately after
receiving the response.
3. Stop the broker and observe that it does not shut down cleanly, with
an orphaned channel in d_channels preventing shutdown.
4. More specifically note that d_nbOpenClients is 1, which prevents the shut down.
"""

import argparse
import json
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

from blazingmq.dev.paths import paths
from blazingmq.dev.processtools import stop_broker

BROKER_PORT = 30114
STARTUP_TIMEOUT = 30
SHUTDOWN_TIMEOUT = 10

IDENTITY = json.dumps(
    {
        "clientIdentity": {
            "protocolVersion": 999999,
            "sdkVersion": 999999,
            "clientType": "E_TCPCLIENT",
            "processName": "fuzztest",
            "pid": 0,
            "sessionId": 1,
            "hostName": "localhost",
            "features": "PROTOCOL_ENCODING:JSON",
            "clusterName": "",
            "clusterNodeId": -1,
            "sdkLanguage": "E_CPP",
        }
    },
    separators=(",", ":"),
).encode()


def make_negotiation_packet(payload: bytes) -> bytes:
    header_flags = b"\x41\x02\x20\x00"
    length = len(payload) + 8
    return struct.pack(">I", length) + header_flags + payload


def wait_for_port(port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--broker-dir",
        type=Path,
        default=paths.broker.parent,
    )
    args = parser.parse_args()
    broker_dir = args.broker_dir.resolve()

    if not (broker_dir / "bmqbrkr.tsk").exists():
        sys.exit(f"bmqbrkr.tsk not found in {broker_dir}")

    print(f"Starting broker in {broker_dir} ...")
    subprocess.Popen(
        ["./run"], cwd=broker_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    if not wait_for_port(BROKER_PORT, STARTUP_TIMEOUT):
        stop_broker(broker_dir)
        sys.exit("Broker failed to start")
    print("Broker listening on port", BROKER_PORT)

    packet = make_negotiation_packet(IDENTITY)
    sock = socket.create_connection(("localhost", BROKER_PORT), timeout=5)
    sock.sendall(packet)
    sock.recv(4096)
    sock.close()
    print("Sent negotiation, got response, closed socket")

    rc = stop_broker(broker_dir, timeout=SHUTDOWN_TIMEOUT)
    if rc == 0:
        print("Broker exited cleanly — race NOT triggered")
    else:
        print(
            "BUG REPRODUCED: broker did not shut down cleanly "
            "(orphaned channel in d_channels prevents shutdown)"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
