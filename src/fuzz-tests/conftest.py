# Copyright 2026 Bloomberg Finance L.P.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Pytest fixtures for fuzz testing the BlazingMQ broker.
"""

import queue
import socket
import subprocess
import time
from dataclasses import dataclass
from threading import Thread

import pytest

from blazingmq.dev.paths import paths
from blazingmq.dev.processtools import stop_broker

BROKER_DEFAULT_PORT = 30114
BROKER_TERMINATE_TIMEOUT = 10
BROKER_TIME_LIMIT = 60 * 60  # 1 hour
BROKER_STARTUP_TIMEOUT = 30


@dataclass
class BrokerInfo:
    host: str
    port: int


def _run_broker(result_queue, broker_dir, broker_cmd, time_limit):
    """Run the broker and put the exit code on the result queue."""
    try:
        subprocess.run(
            broker_cmd.split(),
            cwd=broker_dir,
            timeout=time_limit,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        result_queue.put((0, ""))
    except subprocess.CalledProcessError as ex:
        result_queue.put((ex.returncode, (ex.stdout or b"").decode(errors="replace")))
    except subprocess.TimeoutExpired as ex:
        stop_broker(broker_dir, BROKER_TERMINATE_TIMEOUT)
        result_queue.put((-1, (ex.stdout or b"").decode(errors="replace")))


def _wait_for_port(host, port, timeout):
    """Wait until a TCP connection to host:port succeeds or timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


@pytest.fixture
def broker():
    """
    Start a BlazingMQ broker, yield connection info, then stop it.

    The broker directory is resolved from BLAZINGMQ_BUILD_DIR (set by cmake
    or the user) via blazingmq.dev.paths, falling back to build/blazingmq
    relative to the repository root.
    """
    broker_dir = paths.broker.parent

    if not broker_dir.exists():
        pytest.fail(
            f"Broker directory '{broker_dir}' does not exist. "
            "Set BLAZINGMQ_BUILD_DIR or build with: cmake --build --preset default"
        )

    if not (broker_dir / "bmqbrkr.tsk").exists():
        pytest.fail(
            f"'{broker_dir}' does not contain bmqbrkr.tsk. "
            "Build with: cmake --build --preset default"
        )

    result_queue = queue.Queue()
    broker_thread = Thread(
        target=_run_broker,
        args=(result_queue, broker_dir, "./run", BROKER_TIME_LIMIT),
    )
    broker_thread.start()

    if not _wait_for_port("localhost", BROKER_DEFAULT_PORT, BROKER_STARTUP_TIMEOUT):
        if not result_queue.empty():
            rc, output = result_queue.get_nowait()
            pytest.fail(
                f"Broker failed to start (exit code {rc}).\n"
                f"Output:\n{output[-2000:]}"
            )
        pytest.fail(
            f"Broker did not start listening on port {BROKER_DEFAULT_PORT} "
            f"within {BROKER_STARTUP_TIMEOUT} seconds"
        )

    yield BrokerInfo(host="localhost", port=BROKER_DEFAULT_PORT)

    stop_broker(broker_dir, BROKER_TERMINATE_TIMEOUT)
    broker_thread.join(timeout=BROKER_TERMINATE_TIMEOUT)

    if not result_queue.empty():
        rc, output = result_queue.get_nowait()
        if rc != 0:
            pytest.fail(
                f"Broker exited with non-zero return code: {rc}\n"
                f"Output:\n{output[-2000:]}"
            )
