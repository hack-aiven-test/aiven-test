from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple
from urllib.parse import ParseResult, urlparse

KAFKA = "kafka"
HTTP = "http"


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
class KafkaBrokerConfig:
    bootstrap_servers: List[str]
    topic: str

    def __post_init__(self) -> None:
        verify_type(self.topic, (str,), "topic must be a string")
        verify_type_of_list(
            self.bootstrap_servers, (str,), "bootstrap_servers should be a list of URLs"
        )

        if len(self.bootstrap_servers) == 0:
            raise ValueError("bootstrap_servers must not be an empty list")

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "KafkaBrokerConfig":
        servers = config.get("bootstrap_servers", list())
        topic = config.get("topic")
        return KafkaBrokerConfig(servers, topic)  # type:ignore


@dataclass
class HTTPMonitorConfig:
    url: str
    http_verb: str
    measure_every_sec: float
    timeout_sec: int
    parsed_url: ParseResult = field(init=False)

    DEFAULT_TIMEOUT = 10

    def __post_init__(self) -> None:
        msg_invalid_measure_sec = (
            "measure interval must be a number in seconds describing how "
            "frequent samples should be taken."
        )

        verify_type(self.url, (str,), "Monitor URL must be valid")
        verify_type(self.http_verb, (str,), "http_verb be a string")
        verify_type(self.timeout_sec, (int, float), "timeout_sec must be a number")
        verify_type(self.measure_every_sec, (int, float), msg_invalid_measure_sec)

        self.parsed_url = urlparse(self.url)
        if self.parsed_url.scheme not in ("http", "https"):
            raise ValueError("Only HTTP and HTTPS URLs are supported.")

        if self.measure_every_sec <= 0:
            raise ValueError("measure_every_sec must be a non-zero positive number.")

        # Note: `timeout_sec` is used to configure the socket timeout, this is
        # not ideal since the overall request may take more time than the
        # setting.
        if self.timeout_sec > self.measure_every_sec:
            raise ValueError(
                "timeout_sec should be smaller than measure_every_sec, "
                "otherwise measuring unavailability will be compromised."
            )

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "HTTPMonitorConfig":
        return HTTPMonitorConfig(
            config.get("url", None),
            config.get("http_verb", "GET"),
            config.get("measure_every_sec"),  # type:ignore
            config.get("timeout_sec", HTTPMonitorConfig.DEFAULT_TIMEOUT),
        )


@dataclass
class MonitorConfig:
    broker: KafkaBrokerConfig
    measurements: List["HTTPMonitorConfig"]

    SUPPORTED_BROKERS = {
        KAFKA: KafkaBrokerConfig,
    }
    SUPPORTED_MONITORS = {
        HTTP: HTTPMonitorConfig,
    }

    def __post_init__(self) -> None:
        broker_names = ", ".join(self.SUPPORTED_BROKERS.keys())
        broker_types = tuple(self.SUPPORTED_BROKERS.values())

        measurement_names = ", ".join(self.SUPPORTED_MONITORS.keys())
        measurement_types = tuple(self.SUPPORTED_MONITORS.values())

        verify_type(
            self.broker,
            broker_types,
            f"broker must be one of the {broker_names}, found {self.broker}",
        )
        verify_type_of_list(
            self.measurements,
            measurement_types,
            f"measurements should be a list of {measurement_names}",
        )

        if len(self.measurements) == 0:
            raise ValueError("At least one monitor entry is necessary.")

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "MonitorConfig":
        broker_config = config.get("broker", {})
        broker_type = cls.SUPPORTED_BROKERS.get(broker_config.get("type"))

        broker = None
        if broker_type:
            broker = broker_type.from_dict(broker_config)

        measurements = []
        for measurement in config.get("measurements", []):
            monitor_type = cls.SUPPORTED_MONITORS.get(measurement.get("type"))

            if monitor_type:
                measurements.append(monitor_type.from_dict(measurement))

        return MonitorConfig(broker, measurements)  # type:ignore


@dataclass
class PostgreSQLConfig:
    dsn: str

    def __post_init__(self) -> None:
        verify_type(self.dsn, (str,), "dsn must be a string")

        if len(self.dsn) == 0:
            raise ValueError("dsn must be non-empty.")

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> "PostgreSQLConfig":
        dsn = config.get("dsn")
        return PostgreSQLConfig(dsn)  # type:ignore


@dataclass
class PublishConfig:
    broker: KafkaBrokerConfig
    store: PostgreSQLConfig

    SUPPORTED_BROKERS = {
        KAFKA: KafkaBrokerConfig,
    }

    def __post_init__(self) -> None:
        broker_names = ", ".join(self.SUPPORTED_BROKERS.keys())
        broker_types = tuple(self.SUPPORTED_BROKERS.values())

        verify_type(
            self.broker,
            broker_types,
            f"broker must be one of the {broker_names}, found {self.broker}",
        )

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "PublishConfig":
        broker_config = config.get("broker", {})
        broker_type = cls.SUPPORTED_BROKERS.get(broker_config.get("type"))

        broker = None
        if broker_type:
            broker = broker_type.from_dict(broker_config)

        store = PostgreSQLConfig.from_dict(config.get("store", {}))

        return PublishConfig(broker, store)  # type:ignore
