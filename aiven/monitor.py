import pickle
import signal
import socket
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.client import HTTPConnection, HTTPSConnection
from logging import Logger, getLogger
from queue import Queue
from sys import stderr
from threading import Event, Thread
from typing import Any, Dict, Optional, Union
from uuid import UUID, uuid1

from kafka import KafkaProducer  # type: ignore

from aiven.config import HTTPMonitorConfig, KafkaBrokerConfig, MonitorConfig

NANO_TO_SEC_RATIO = 1_000_000_000
NANO_TO_MS_RATIO = 1_000_000


@dataclass
class HTTPMetrics:
    http_status_code: int
    http_fully_loaded_time_ms: int


@dataclass
class HTTPMeasurement:
    config: HTTPMonitorConfig
    measurement_started_at: datetime
    metrics: Optional[HTTPMetrics]
    uuid: UUID = field(default_factory=uuid1, init=False)


def http_metrics(
    config: HTTPMonitorConfig, url: str, connection: Union[HTTPSConnection, HTTPConnection]
) -> HTTPMetrics:
    """ Perform a single request and produce a metric for its result. """
    before_ns = time.perf_counter_ns()

    connection.request(config.http_verb, url)
    response = connection.getresponse()
    response.read()

    elapsed_ns = time.perf_counter_ns() - before_ns

    http_fully_loaded_time_ms = elapsed_ns // NANO_TO_MS_RATIO
    status = response.status

    return HTTPMetrics(status, http_fully_loaded_time_ms)


def http_connection_from_config(
    config: HTTPMonitorConfig,
) -> Union[HTTPSConnection, HTTPConnection]:
    # Open the connection before the measurement and keep them alive. This is
    # necesary to allow for comparison of HTTP with HTTPS since TLS adds a
    # non-neglible overhead.
    if config.parsed_url.scheme == "https":
        return HTTPSConnection(config.parsed_url.netloc, timeout=config.timeout_sec)

    assert config.parsed_url.scheme == "http", f"unexpect protocol {config.parsed_url.scheme}"
    return HTTPConnection(config.parsed_url.netloc, timeout=config.timeout_sec)


def http_monitor(
    stop_event: Event, config: HTTPMonitorConfig, queue: "Queue[HTTPMeasurement]", logger: Logger
) -> None:
    """ Infinite loop to monitor the HTTP described by `config`. """
    perf_timer_details = time.get_clock_info("perf_counter")
    msg = (
        "perf timer must be monotonic and non-adjustable, otherwise the "
        "measurements could be skewed."
    )
    assert perf_timer_details.adjustable is False, msg
    assert perf_timer_details.monotonic is True, msg

    parsed_url = config.parsed_url

    url = f"{parsed_url.path}"
    if parsed_url.query:
        url += f"?{parsed_url.query}"

    connection = http_connection_from_config(config)
    metrics: Optional[HTTPMetrics]

    while not stop_event.is_set():
        logger.debug("Starting new measurement")

        measurement_started_at = datetime.now(tz=timezone.utc)

        try:
            metrics = http_metrics(config, url, connection)

        # ConnectionRefusedError - server may not be online yet
        # ConnectionResetError - server quit and closed the connection
        # socket.timeout - if the timeout is reached
        except (ConnectionRefusedError, ConnectionResetError, socket.timeout):
            connection = http_connection_from_config(config)
            metrics = None

        measurement = HTTPMeasurement(config, measurement_started_at, metrics)

        queue.put(measurement)

        elapsed = datetime.now(tz=timezone.utc) - measurement_started_at
        sleep_for = config.measure_every_sec - elapsed.seconds

        if sleep_for > 0:
            stop_event.wait(timeout=sleep_for)


def install_stop_hooks(stop_event: Event) -> None:
    """ Install hooks to stop the process, either through a system signal or an
    unhandled exception.
    """

    def print_exception_and_stop(args):  # type: ignore
        if args.thread is not None:
            print(f"Thread {args.thread.name} stopped abruptly", file=stderr)

        traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback, file=stderr)
        stop_event.set()

    threading.excepthook = print_exception_and_stop

    def set_stop(_sig: int, _frame: Any = None) -> None:
        stop_event.set()

    signal.signal(signal.SIGQUIT, set_stop)
    signal.signal(signal.SIGTERM, set_stop)
    signal.signal(signal.SIGINT, set_stop)


def kafka_publish_measurements(
    stop_event: Event,
    kafka_config: KafkaBrokerConfig,
    kafka_producer: KafkaProducer,
    queue: "Queue[HTTPMeasurement]",
) -> None:
    while not stop_event.is_set():
        measurement = queue.get()
        data = pickle.dumps(measurement)
        kafka_producer.send(kafka_config.topic, data)


def run_monitoring_from_config(user_config: Dict[str, Any]) -> None:
    """ Run the monitoring as described in the user configuration.

    This will spawn one thread per monitored end-point. This prevents
    inteferrence of slow measurements with the faster ones.

    This will fail early if the kafka servers are not running *during startup*.
    However, the service will continue running if the connection is closed
    *after* initialization. For proper operational behavior this process has
    to be executed through a process manager.
    """
    valid_config = MonitorConfig.from_dict(user_config)
    stop_event = Event()
    install_stop_hooks(stop_event)

    broker = valid_config.broker
    args: Dict[str, Any] = {
        "bootstrap_servers": broker.bootstrap_servers,
    }

    if broker.ssl:
        args["ssl_cafile"] = broker.ssl.cafile
        args["ssl_certfile"] = broker.ssl.certfile
        args["ssl_keyfile"] = broker.ssl.keyfile
        args["security_protocol"] = "SSL"

    kafka_producer = KafkaProducer(**args)

    queue: "Queue[HTTPMeasurement]" = Queue()

    for measurement in valid_config.measurements:
        logger = getLogger(f"monitor.http.{measurement.parsed_url.netloc}")
        t = Thread(
            target=http_monitor,
            name=f"monitor-{measurement.parsed_url.netloc}",
            args=(stop_event, measurement, queue, logger),
            daemon=False,
        )
        t.start()

    t = Thread(
        target=kafka_publish_measurements,
        name="kafka",
        args=(stop_event, valid_config.broker, kafka_producer, queue),
        # The kafka_producer hangs on the `send` method, even if the producer
        # is stopped. Since there is no API to unblock the send, waiting for it
        # would hang the whole process.
        daemon=True,
    )
    t.start()

    stop_event.wait()

    kafka_producer.flush()
    kafka_producer.close()
