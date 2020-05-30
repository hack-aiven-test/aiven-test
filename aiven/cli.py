import argparse
import json
import logging

from aiven.monitor import run_monitoring_from_config

MONITOR_CMD = "monitor"
PUBLISH_CMD = "publish"


def configure_logging(logging_level: str) -> None:
    logging.basicConfig(level=logging_level, format="%(asctime)-15s %(threadName)s %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--logging-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
    )
    commands = parser.add_subparsers(dest="command")

    parser_monitor = commands.add_parser(
        MONITOR_CMD,
        help="Monitoring projects as defined per the configuration and publish metrics to Kafka.",
    )
    parser_monitor.add_argument("config", type=argparse.FileType("r"))

    parser_publish = commands.add_parser(
        PUBLISH_CMD, help="Fetch metrics from Kafka and save on a relation database."
    )
    parser_publish.add_argument("config", type=argparse.FileType("r"))

    args = parser.parse_args()

    configure_logging(args.logging_level)

    if args.command == MONITOR_CMD:
        config = json.load(args.config)
        args.config.close()
        run_monitoring_from_config(config)
    elif args.command == PUBLISH_CMD:
        config = json.load(args.config)
        args.config.close()
        print(PUBLISH_CMD)
    else:
        parser.print_help()
