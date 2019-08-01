import os
import time

import mock
import requests_mock
import unittest

import asyncio
import simplejson
from singer import Schema

from tap_datadog import DatadogClient, DatadogAuthentication, DatadogSync, load_schemas

def load_schema(filename):
    myDir = os.path.dirname(os.path.abspath(''))
    path = os.path.join(myDir, "tap_datadog/schemas", filename)
    with open(path) as file:
        return simplejson.load(file)


# Our test case class
class MyGreatClassTestCase(unittest.TestCase):

    @requests_mock.mock()
    def test_hourly(self, m):
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/logs', json={'usage': [{'ingested_events_bytes': 0, 'indexed_events_count': 0, 'hour': '2019-07-22T18'}]})
        config = {"start_hour":"2019-08-07T12"}
        self.assertEqual(test_client.hourly_request({}, config, "logs", "logs"), {'usage': [{'ingested_events_bytes': 0, 'indexed_events_count': 0, 'hour': '2019-07-22T18'}]})

    def test_hourly_ratelimit(self):
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)
        time_to_reset = str(-30)
        with requests_mock.mock() as m:
            m.get('https://api.datadoghq.com/api/v1/usage/logs', json={'usage': [{'ingested_events_bytes': 0, 'indexed_events_count': 0, 'hour': '2019-07-22T18'}]}, status_code=429,  headers={'X-RateLimit-Reset': time_to_reset})
            config = {"start_hour":"2019-08-07T12"}
            test_client.hourly_request({}, config, "logs", "logs")
            count = m.call_count
            self.assertEqual(count, 3)

    def test_generate_start_date_without_state(self):
        config ={"start_month":"2019-07",
                 "start_hour":"2019-09-15T12"}
        state= {"bookmarks": {"top_average_metrics": {"since": "2019-08"}, "logs": {"since": "2019-11-22T18"}}}
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)
        self.assertEqual(test_client.generate_start_date(config, "top_average_metrics", "since", "start_month", {}), "2019-07")
        self.assertEqual(test_client.generate_start_date(config, "top_average_metrics", "since", "start_month", state), "2019-08")
        self.assertEqual(test_client.generate_start_date(config, "logs", "since", "start_hour", {}),  "2019-09-15T12")
        self.assertEqual(test_client.generate_start_date(config, "logs", "since", "start_hour", state),  "2019-11-22T18")

    @requests_mock.mock()
    def test_top_avg_metrics(self, m):
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/top_avg_metrics', json={'usage': [{"metric_category": "custom", "metric_name": "prod.events.producer.failed.messages.95percentile", "max_metric_hour": 932, "avg_metric_hour": 262}]})
        config = {"start_month":"2019-08-07T12"}
        self.assertEqual(test_client.top_avg_metrics({}, config), {'usage': [{"metric_category": "custom", "metric_name": "prod.events.producer.failed.messages.95percentile", "max_metric_hour": 932, "avg_metric_hour": 262}]})

    @requests_mock.mock()
    def test_hourly_logs(self, m):
        loop = asyncio.get_event_loop()
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/logs', json={'usage': [{'ingested_events_bytes': 0, 'indexed_events_count': 0, 'hour': '2019-07-22T18'}]})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = DatadogSync(test_client, config=config)
        schema = load_schema("logs.json")
        resp = dataSync.sync_logs(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('logs', {'ingested_events_bytes': 0, 'indexed_events_count': 0, 'hour': '2019-07-22T18'})

    @requests_mock.mock()
    def test_hourly_custom_usage(self, m):
        loop = asyncio.get_event_loop()
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/timeseries', json={'usage': [{"num_custom_timeseries": 31561, "hour": "2019-07-22T18"}]})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = DatadogSync(test_client, config=config)
        schema = load_schema("custom_usage.json")
        resp = dataSync.sync_custom_usage(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('custom_usage', {"num_custom_timeseries": 31561, "hour": "2019-07-22T18"})

    @requests_mock.mock()
    def test_hourly_fargate(self, m):
        loop = asyncio.get_event_loop()
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/fargate', json={'usage': [{"tasks_count": 0, "hour": "2019-06-17T12"}]})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = DatadogSync(test_client, config=config)
        schema = load_schema('fargate.json')
        resp = dataSync.sync_fargate(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('fargate', {"tasks_count": 0, "hour": "2019-06-17T12"})

    @requests_mock.mock()
    def test_hourly_hosts_and_containers(self, m):
        loop = asyncio.get_event_loop()
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/hosts', json={'usage': [{"host_count": 33, "container_count": None, "hour": "2019-07-22T19", "apm_host_count": None, "agent_host_count": 33, "gcp_host_count": 0, "aws_host_count": 0}]})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = DatadogSync(test_client, config=config)
        schema = load_schema('hosts_and_containers.json')
        resp = dataSync.sync_hosts_and_containers(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('hosts_and_containers', {"host_count": 33, "container_count": None, "hour": "2019-07-22T19", "apm_host_count": None, "agent_host_count": 33, "gcp_host_count": 0, "aws_host_count": 0})


    @requests_mock.mock()
    def test_hourly_synthetics(self, m):
        loop = asyncio.get_event_loop()
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/synthetics', json={'usage': [{"hour": "2019-07-22T20", "check_calls_count": 420}]})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = DatadogSync(test_client, config=config)
        schema = load_schema('synthetics.json')
        resp = dataSync.sync_synthetics(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('synthetics', {"hour": "2019-07-22T20", "check_calls_count": 420})

    @requests_mock.mock()
    def test_sync_top_average_metrics(self, m):
        loop = asyncio.get_event_loop()
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/top_avg_metrics', json={'usage': [{"metric_category": "custom", "metric_name": "prod.events.producer.failed.messages.95percentile", "max_metric_hour": 932, "avg_metric_hour": 262}]})
        config = {"start_month":"2019-08-07T12"}
        dataSync = DatadogSync(test_client, config=config)
        schema = load_schema('top_average_metrics.json')
        resp = dataSync.sync_top_average_metrics(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('top_average_metrics', {"metric_category": "custom", "metric_name": "prod.events.producer.failed.messages.95percentile", "max_metric_hour": 932, "avg_metric_hour": 262})

    @requests_mock.mock()
    def test_hourly_trace_search(self, m):
        loop = asyncio.get_event_loop()
        auth = DatadogAuthentication('111','222')
        test_client = DatadogClient(auth)

        m.get('https://api.datadoghq.com/api/v1/usage/traces', json={'usage': [{"indexed_events_count": 0, "hour": "2019-07-22T18"}]})
        config = {"start_hour":"2019-08-07T12"}
        dataSync = DatadogSync(test_client, config=config)
        schema = load_schema('trace_search.json')
        resp = dataSync.sync_trace_search(Schema(schema))
        with mock.patch('singer.write_record') as patching:
            task = asyncio.gather(resp)
            loop.run_until_complete(task)
            patching.assert_called_once_with('trace_search', {"indexed_events_count": 0, "hour": "2019-07-22T18"})


if __name__ == '__main__':
    unittest.main()