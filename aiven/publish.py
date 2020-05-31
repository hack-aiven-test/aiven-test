import pickle
from contextlib import closing
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid1

import psycopg2  # type: ignore
from kafka import KafkaConsumer  # type: ignore

from aiven.config import PublishConfig
from aiven.monitor import HTTPMeasurement

# These queries assumes conflicts only happen if there is an id collision,
# since ids are UUID1 this should never happen. This allows messages to be
# processed more-than-once.
MAYBE_INSERT_MEASUREMENT = (
    "INSERT INTO monmon_website_measurement "
    "  (id, website_fk, measurement_started_at, http_status_code, http_fully_loaded_time_ms) "
    " VALUES (%s, %s, %s, %s, %s) "
    " ON CONFLICT DO NOTHING"
)

MAYBE_INSERT_WEBSITE = (
    "INSERT INTO monmon_website (id, website) VALUES (%s, %s) ON CONFLICT DO NOTHING"
)


def measurement_to_insert_tuple(
    measurement: HTTPMeasurement, website_to_id: Dict[str, str]
) -> Tuple[Any, ...]:
    """ Converts a `HTTPMeasurement` to a tuple to be used with an insert query. """
    http_status_code: Optional[int] = None
    http_fully_loaded_time_ms: Optional[int] = None
    if measurement.metrics:
        http_status_code = measurement.metrics.http_status_code
        http_fully_loaded_time_ms = measurement.metrics.http_fully_loaded_time_ms

    website_id = website_to_id[measurement.config.url]

    return (
        str(measurement.uuid),
        website_id,
        measurement.measurement_started_at,
        http_status_code,
        http_fully_loaded_time_ms,
    )


def update_website_id_mapping(  # type: ignore
    conn, website_to_id: Dict[str, str], msgs_batch: List[HTTPMeasurement]
) -> None:
    """ Generate IDs for unknown websites and *if necessary* insert them into
    the database. Update the mapping with the DB's id.
    """
    uncached_website_ids = set(
        (str(uuid1()), measurement.config.url)
        for measurement in msgs_batch
        if measurement.config.url not in website_to_id
    )

    with conn, closing(conn.cursor()) as cursor:
        cursor.executemany(MAYBE_INSERT_WEBSITE, uncached_website_ids)

    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT website, id FROM monmon_website")
        website_to_id.update(cursor.fetchall())


def run_publish_from_config(user_config: Dict[str, Any]) -> None:
    valid_config = PublishConfig.from_dict(user_config)

    kafka_config = valid_config.broker
    kafka_consumer = KafkaConsumer(
        kafka_config.topic, bootstrap_servers=kafka_config.bootstrap_servers
    )

    conn = psycopg2.connect(valid_config.store.dsn)
    website_to_id: Dict[str, str] = dict()

    while True:
        # TODO: Consider what to do if the datetime object is time zone
        # unaware. Solutions:
        # - Assume the monitoring and publishing servers are in the same
        # timezone, and use this server to convert the datetime to UTC (Maybe
        # mark the data as tainted).
        # - Drop the data.

        msgs_batch: List[HTTPMeasurement] = list()
        for topic_messages in kafka_consumer.poll(timeout_ms=1000).values():
            msgs_batch.extend(pickle.loads(msg.value) for msg in topic_messages)

        if msgs_batch:
            update_website_id_mapping(conn, website_to_id, msgs_batch)

            insert_batch = [
                measurement_to_insert_tuple(measurement, website_to_id)
                for measurement in msgs_batch
            ]

            # Here I would have used prepared statements, however, I'm sticking to
            # the DB API as specified in the test assignment.
            with conn, closing(conn.cursor()) as cursor:
                cursor.executemany(MAYBE_INSERT_MEASUREMENT, insert_batch)
