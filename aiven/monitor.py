import signal
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from http.client import HTTPConnection, HTTPSConnection
from logging import Logger, getLogger
from queue import Queue
from sys import stderr
from threading import Event, Thread
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

KAFKA = "kafka"
HTTP = "http"
NANO_TO_SEC_RATIO = 1_000_000_000
NANO_TO_MS_RATIO = 1_000_000


def verify_type(value: Any, types: Tuple[type, ...], errormsg: str,) -> None:
    """ Helper to improve type check readability. """
    if not isinstance(value, types):
        raise ValueError(errormsg)


def verify_type_of_list(values: Any, types: Tuple[type, ...], errormsg: str,) -> None:
    """ Helper to improve type check readability. """
    verify_type(values, (list,), errormsg)

    for value in values:
        verify_type(value, types, errormsg)


@dataclass
class KafkaSinkConfig:
    bootstrap_servers: List[str]

    def __post_init__(self) -> None:
        verify_type_of_list(
            self.bootstrap_servers, (str,), "bootstrap_servers should be a list of URLs"
        )

        if len(self.bootstrap_servers) == 0:
            raise ValueError("bootstrap_servers must not be an empty list")

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "KafkaSinkConfig":
        return KafkaSinkConfig(config.get("bootstrap_servers", list()))


@dataclass
class HTTPMonitorConfig:
    url: str
    http_verb: str
    measure_every_sec: float
    parsed_url: ParseResult = field(init=False)

    def __post_init__(self) -> None:
        verify_type(self.url, (str,), "Monitor URL must be valid")
        verify_type(
            self.measure_every_sec,
            (int, float),
            (
                "measure interval must be a number in seconds describing how "
                "frequent samples should be taken."
            ),
        )

        self.parsed_url = urlparse(self.url)
        if self.parsed_url.scheme not in ("http", "https"):
            raise ValueError("Only HTTP and HTTPS URLs are supported.")

        if self.measure_every_sec <= 0:
            raise ValueError("measure_every_sec must be a non-zero positive number.")

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "HTTPMonitorConfig":
        return HTTPMonitorConfig(
            config.get("url", None),
            config.get("http_verb", "GET"),
            config.get("measure_every_sec"),  # type:ignore
        )


@dataclass
class MonitorConfig:
    sinks: List[KafkaSinkConfig]
    targets: List["HTTPMonitorConfig"]

    SUPPORTED_SINKS = {
        KAFKA: KafkaSinkConfig,
    }
    SUPPORTED_MONITORS = {
        HTTP: HTTPMonitorConfig,
    }

    def __post_init__(self) -> None:
        sink_names = ", ".join(self.SUPPORTED_SINKS.keys())
        sink_types = tuple(self.SUPPORTED_SINKS.values())

        target_names = ", ".join(self.SUPPORTED_MONITORS.keys())
        target_types = tuple(self.SUPPORTED_MONITORS.values())

        verify_type_of_list(self.sinks, sink_types, f"sinks should be a list of {sink_names}")
        verify_type_of_list(
            self.targets, target_types, f"targets should be a list of {target_names}"
        )

        if len(self.targets) == 0:
            raise ValueError("At least one monitor entry is necessary.")

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "MonitorConfig":
        sinks = []
        for sink_config in config.get("sinks", []):
            sink_type = cls.SUPPORTED_SINKS.get(sink_config.get("type"))

            if sink_type:
                sinks.append(sink_type.from_dict(sink_config))

        targets = []
        for target in config.get("targets", []):
            monitor_type = cls.SUPPORTED_MONITORS.get(target.get("type"))

            if monitor_type:
                targets.append(monitor_type.from_dict(target))

        return MonitorConfig(sinks, targets)


@dataclass
class HTTPMetrics:
    http_status_code: int
    http_fully_loaded_time_ms: int


@dataclass
class HTTPMeasurement:
    config: HTTPMonitorConfig
    measurement_started_at: datetime
    metrics: Optional[HTTPMetrics]


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
        return HTTPSConnection(config.parsed_url.netloc)

    assert config.parsed_url.scheme == "http", f"unexpect protocol {config.parsed_url.scheme}"
    return HTTPConnection(config.parsed_url.netloc)


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
        measurement_started_at = datetime.now()

        try:
            metrics = http_metrics(config, url, connection)
        except ConnectionRefusedError:
            connection = http_connection_from_config(config)
            metrics = None

        measurement = HTTPMeasurement(config, measurement_started_at, metrics)

        logger.debug("New measurement")
        queue.put(measurement)

        elapsed = datetime.now() - measurement_started_at
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


def run_monitoring_from_config(user_config: Dict[str, Any]) -> None:
    """ Run the monitoring as described in the user configuration.

    This will spawn one thread per monitored end-point. This prevents
    inteferrence of slow measurements with the faster ones.
    """
    valid_config = MonitorConfig.from_dict(user_config)
    stop_event = Event()
    install_stop_hooks(stop_event)

    threads = list()
    for target in valid_config.targets:
        q: "Queue[HTTPMeasurement]" = Queue()
        logger = getLogger("monitor.http.target.parsed_url.netloc")
        t = Thread(
            target=http_monitor,
            name=f"monitor-{target.parsed_url.netloc}",
            args=(stop_event, target, q, logger),
            daemon=False,
        )
        t.start()
        threads.append(t)

    stop_event.wait()
