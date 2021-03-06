Running
=======

After installing the projectect with `poetry install`, the monitoring and
publishing of metrics need a configuration file:

```json
{
    "broker": {
        "type": "kafka",
        "topic": "aiven",
        "bootstrap_servers": ["<server>:<port>"],
        "ssl": {
            "cafile": "<path_to_cafile>",
            "certfile": "<path_to_certfile>",
            "keyfile": "<path_to_keyfile>"
        }
    },
    "store": {
        "type": "postgresql",
        "dsn": "host=<host> port=<port> user=<user> password=<password> dbname=aiven connect_timeout=2"
    },
    "measurements": [
        {
            "type": "http",
            "url": "<website>",
            "measure_every_sec": <frequency_of_measurements>,
            "timeout_sec": <time_to_wait_for_a_single_measurement>
        }
    ]
}
```

Note that secrets can be provided via [environment
vars](https://www.postgresql.org/docs/current/libpq-envars.html).

With this configuration the two utilities can be used. To monitor the
configured websites the following command can be used:

```sh
monmon monitor /path/to/config.json
```

and to consume the measurements and save to a database:

```sh
monmon publish /path/to/config.json
```

Testing
=======

To enable the project in a virtualenv:

```sh
poetry shell
poetry install
```

To start kafka and postgres:

```sh
cd tests
docker-compose up -d
```

```sh
# on one terminal
monmon monitor config.json
# on another terminal
monmon publish config.json
# entries can be fetched with, password is test
psql -h localhost -U postgres -d postgres
```

Database
========

- The tables are prefixed with `monmon_`.
- Database entries use UUIDs as primary keys. This prevents id collision and
  allows for merging of datasets.
- Every measurement is saved into the database, this allows the resolution of
  the measurement to be changed with time. IOW, the lack of data does imply the
  monitored service was offline.
  - ATM there is no policy for data retention in place. Data usage will
    increase with time and will either require manual cleaning or the addition
    of automated rolling / cleanup.

Exercise
========

Your task is to implement a system that monitors website availability over the
network, produces metrics about this and passes these events through an Aiven
Kafka instance into an Aiven PostgreSQL database.

For this, you need a Kafka producer which periodically checks the target
websites and sends the check results to a Kafka topic, and a Kafka consumer
storing the data to an Aiven PostgreSQL database. For practical reasons, these
components may run in the same machine (or container or whatever system you
choose), but in production use similar components would run in different
systems.

The website checker should perform the checks periodically and collect the
HTTP response time, error code returned, as well as optionally checking the
returned page contents for a regexp pattern that is expected to be found on the
page.

For the database writer we expect to see a solution that records the check
results into one or more database tables and could handle a reasonable amount
of checks performed over a longer period of time.

Even though this is a small concept program, returned homework should include
tests and proper packaging. If your tests require Kafka and PostgreSQL
services, for simplicity your tests can assume those are already running,
instead of integrating Aiven service creation and deleting.

Aiven is a Database as a Service vendor and the homework requires using our
services. Please register to Aiven at https://console.aiven.io/signup.html at
which point you'll automatically be given $300 worth of credits to play around
with. The credits should be enough for a few hours of use of our services. If
you need more credits to complete your homework, please contact us.

The solution should NOT include using any of the following:

- Database ORM libraries - use a Python DB API compliant library and raw SQL
  queries instead
- Extensive container build recipes - rather focus your effort on the Python
  code, tests, documentation, etc.

Criteria for evaluation
=======================

- Code formatting and clarity. We value readable code written for other
  developers, not for a tutorial, or as one-off hack.
- We appreciate demonstrating your experience and knowledge, but also utilizing
  existing libraries. There is no need to re-invent the wheel.
- Practicality of testing. 100% test coverage may not be practical, and also
  having 100% coverage but no validation is not very useful.
- Automation. We like having things work automatically, instead of multi-step
  instructions to run misc commands to set up things. Similarly, CI is a
  relevant thing for automation.
- Attribution. If you take code from Google results, examples etc., add
  attributions. We all know new things are often written based on search
  results.
- "Open source ready" repository. It's very often a good idea to pretend the
  homework assignment in Github is used by random people (in practice, if you
  want to, you can delete/hide the repository as soon as we have seen it).
