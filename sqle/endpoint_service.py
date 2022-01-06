import time
from dataclasses import dataclass
from typing import List

import requests.exceptions
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class SqlEndpoints:
    endpoints: List['SqlEndpoint']

    def to_pandas(self):
        return pd.DataFrame(self.endpoints)


from dataclasses import dataclass
from typing import List, Dict, Optional, Any

import pandas as pd
import requests.exceptions
from databricks import sql
from databricks_cli.sdk import ApiClient
from dataclasses_json import dataclass_json
from pandas import DataFrame
from pyspark.sql.types import StructType, StructField, BooleanType, LongType, StringType, MapType
from timeout_decorator import timeout_decorator


@dataclass_json
@dataclass
class SqlEndpoints:
    endpoints: List['SqlEndpoint']

    def to_pandas(self):
        return pd.DataFrame(self.endpoints)


SQL_ENDPOINT_STRUCT_TYPE = StructType(
    [StructField("auto_resume", BooleanType(), True), StructField("auto_stop_mins", LongType(), True),
     StructField("channel", StringType(), True), StructField("cluster_size", StringType(), True),
     StructField("conf_pairs", MapType(StringType(), StringType(), True), True), StructField("creator_id", LongType(), True),
     StructField("creator_name", StringType(), True), StructField("enable_photon", BooleanType(), True),
     StructField("enable_serverless_compute", BooleanType(), True), StructField("id", StringType(), True),
     StructField("jdbc_url", StringType(), True), StructField("max_num_clusters", LongType(), True),
     StructField("min_num_clusters", LongType(), True), StructField("name", StringType(), True),
     StructField("num_active_sessions", LongType(), True), StructField("num_clusters", LongType(), True),
     StructField("odbc_params", MapType(StringType(), StringType(), True), True), StructField("size", StringType(), True),
     StructField("spot_instance_policy", StringType(), True), StructField("state", StringType(), True),
     StructField("tags", MapType(StringType(), StringType(), True), True)])

@dataclass_json
@dataclass
class SqlEndpoint:
    id: str
    size: str
    name: str
    cluster_size: str
    min_num_clusters: int
    max_num_clusters: int
    auto_stop_mins: int
    auto_resume: bool
    creator_name: str
    creator_id: int
    spot_instance_policy: str
    enable_photon: bool
    enable_serverless_compute: bool
    num_clusters: int
    num_active_sessions: int
    state: str
    jdbc_url: str
    odbc_params: 'OdbcParam'
    conf_pairs: Optional[Dict[str, Any]] = None
    tags: Optional[Dict[str, Any]] = None
    channel: Optional['Channel'] = None


@dataclass_json
@dataclass
class Channel:
    name: Optional[str] = None


@dataclass_json
@dataclass
class OdbcParam:
    hostname: str
    path: str
    protocol: str
    port: int

class EndpointError(Exception):
    pass

class SqlEndpointsService(object):
    def __init__(self, client: ApiClient):
        self.client: ApiClient = client
        self.endpoint_name = "/sql/endpoints"

    def create(self, name, data, or_replace=False, if_not_exists=False, headers=None) -> SqlEndpoint:
        cluster_if_exists = self.get_by_name(name)
        if (or_replace is True or if_not_exists is True) and cluster_if_exists is not None:
            if cluster_if_exists.state == "RUNNING":
                return cluster_if_exists
            state = cluster_if_exists.state
            if or_replace is True and state == "STOPPED":
                self.start(name)
            while state in ["STARTING"]:
                time.sleep(2)
                state = self.get(cluster_if_exists.id).state
            state = self.get_by_name(name).state
            if state != "RUNNING":
                raise EndpointError(f"Requested endpoint to be running but not running instead moved to: {state}")
            return cluster_if_exists
        self.client.perform_query('POST', self.endpoint_name, data=data, headers=headers)
        time.sleep(1)
        ep = self.get_by_name(name)
        state = ep.state
        while state in ["STARTING"]:
            time.sleep(2)
            state = self.get(ep.id).state
        if state != "RUNNING":
            raise EndpointError(f"Requested endpoint to be running but not running instead moved to: {state}")
        return ep

    def list(self, headers=None) -> SqlEndpoints:
        _data = {}

        return SqlEndpoints.from_dict(self.client.perform_query('GET', self.endpoint_name, data=_data, headers=headers))

    def get(self, id_, headers=None) -> SqlEndpoint:
        _data = {}
        return SqlEndpoint.from_dict(self.client.perform_query('GET', f'{self.endpoint_name}/{id_}', data=_data, headers=headers))

    def get_by_name(self, name, headers=None) -> Optional[SqlEndpoint]:
        _data = {}
        for e in self.list(headers).endpoints:
            if e.name == name:
                return e
        return None

    def update(self, name, data, exists=False, headers=None):
        ep = self.get_by_name(name)
        if exists is True and ep is None:
            return
        return self.client.perform_query('POST', f'{self.endpoint_name}/{ep.id}/edit', data=data, headers=headers)

    @timeout_decorator.timeout(60)
    def delete(self, name, exists=False, headers=None):
        ep = self.get_by_name(name)
        if exists is True and ep is None:
            return
        if ep is None:
            raise EndpointError(f"Endpoint with name: {name} does not exist.")
        self.client.perform_query('DELETE', f'{self.endpoint_name}/{ep.id}', headers=headers)
        try:
            state = self.get(ep.id).state
            while state in ["DELETING"]:
                time.sleep(2)
                state = self.get(ep.id).state
        except requests.exceptions.HTTPError as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e):
                return
            else:
                raise e
        if state != "DELETED":
            raise EndpointError(f"Requested endpoint to be deleted but not deleted instead moved to: {state}")
        return

    def start(self, name, exists=False, headers=None):
        ep = self.get_by_name(name)
        if exists is True and ep is None:
            return
        if ep is None:
            raise EndpointError(f"Endpoint with name: {name} does not exist.")
        self.client.perform_query('POST', f'{self.endpoint_name}/{ep.id}/start', headers=headers)
        state = self.get(ep.id).state
        while state in ["STARTING"]:
            time.sleep(2)
            state = self.get(ep.id).state
        if state != "RUNNING":
            raise EndpointError(f"Requested endpoint to be started but not started instead moved to: {state}")
        return


    def stop(self, name,exists=False, headers=None):
        ep = self.get_by_name(name)
        if exists is True and ep is None:
            return
        if ep is None:
            raise EndpointError(f"Endpoint with name: {name} does not exist.")
        self.client.perform_query('POST', f'{self.endpoint_name}/{ep.id}/stop', headers=headers)
        state = self.get(ep.id).state
        while state in ["STOPPING"]:
            time.sleep(2)
            state = self.get(ep.id).state
        if state != "STOPPED":
            raise EndpointError(f"Requested endpoint to be started but not started instead moved to: {state}")
        return

    # MAYBE GLOBAL CONTEXT CAN HAVE CONNECTION Object and cursor is generated on query call
    def run_sql(self, name, sql_stmt, access_token,limit=1000):
        ep = self.get_by_name(name)
        if ep == None:
            raise EndpointError(f"No endpoint with name {name} exists!")
        connection = sql.connect(
            server_hostname=ep.odbc_params.hostname,
            http_path=ep.odbc_params.path,
            access_token=access_token
        )
        cursor = connection.cursor()

        cursor.execute(sql_stmt)
        def get_columns(desc):
            return [i[0] for i in desc]
        df = DataFrame(cursor.fetchmany(size=limit))
        df.columns = get_columns(cursor.description)
        cursor.close()
        connection.close()

@dataclass_json
@dataclass
class SqlEndpoint:
    id: str
    size: str
    name: str
    cluster_size: str
    min_num_clusters: int
    max_num_clusters: int
    auto_stop_mins: int
    auto_resume: bool
    creator_name: str
    creator_id: int
    spot_instance_policy: str
    enable_photon: bool
    enable_serverless_compute: bool
    num_clusters: int
    num_active_sessions: int
    state: str
    jdbc_url: str
    odbc_params: 'OdbcParam'
    conf_pairs: Optional[Dict[str, Any]] = None
    tags: Optional[Dict[str, Any]] = None
    channel: Optional['Channel'] = None


@dataclass_json
@dataclass
class Channel:
    name: Optional[str] = None


@dataclass_json
@dataclass
class OdbcParam:
    hostname: str
    path: str
    protocol: str
    port: int

class EndpointError(Exception):
    pass

class SqlEndpointsService(object):
    def __init__(self, client: ApiClient):
        self.client: ApiClient = client
        self.endpoint_name = "/sql/endpoints"

    def create(self, name, data, or_replace=False, if_not_exists=False, headers=None) -> SqlEndpoint:
        cluster_if_exists = self.get_by_name(name)
        if (or_replace is True or if_not_exists is True) and cluster_if_exists is not None:
            if cluster_if_exists.state == "RUNNING":
                return cluster_if_exists
            state = cluster_if_exists.state
            if or_replace is True and state == "STOPPED":
                self.start(name)
            while state in ["STARTING"]:
                time.sleep(2)
                state = self.get(cluster_if_exists.id).state
            state = self.get_by_name(name).state
            if state != "RUNNING":
                raise EndpointError(f"Requested endpoint to be running but not running instead moved to: {state}")
            return cluster_if_exists
        self.client.perform_query('POST', self.endpoint_name, data=data, headers=headers)
        time.sleep(1)
        ep = self.get_by_name(name)
        state = ep.state
        while state in ["STARTING"]:
            time.sleep(2)
            state = self.get(ep.id).state
        if state != "RUNNING":
            raise EndpointError(f"Requested endpoint to be running but not running instead moved to: {state}")
        return ep

    def list(self, headers=None) -> SqlEndpoints:
        _data = {}

        return SqlEndpoints.from_dict(self.client.perform_query('GET', self.endpoint_name, data=_data, headers=headers))

    def get(self, id_, headers=None) -> SqlEndpoint:
        _data = {}
        return SqlEndpoint.from_dict(self.client.perform_query('GET', f'{self.endpoint_name}/{id_}', data=_data, headers=headers))

    def get_by_name(self, name, headers=None) -> Optional[SqlEndpoint]:
        _data = {}
        for e in self.list(headers).endpoints:
            if e.name == name:
                return e
        return None

    def update(self, name, data, exists=False, headers=None):
        ep = self.get_by_name(name)
        if exists is True and ep is None:
            return
        return self.client.perform_query('POST', f'{self.endpoint_name}/{ep.id}/edit', data=data, headers=headers)

    @timeout_decorator.timeout(60)
    def delete(self, name, exists=False, headers=None):
        ep = self.get_by_name(name)
        if exists is True and ep is None:
            return
        if ep is None:
            raise EndpointError(f"Endpoint with name: {name} does not exist.")
        self.client.perform_query('DELETE', f'{self.endpoint_name}/{ep.id}', headers=headers)
        try:
            state = self.get(ep.id).state
            while state in ["DELETING"]:
                time.sleep(2)
                state = self.get(ep.id).state
        except requests.exceptions.HTTPError as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e):
                return
            else:
                raise e
        if state != "DELETED":
            raise EndpointError(f"Requested endpoint to be deleted but not deleted instead moved to: {state}")
        return

    def start(self, name, exists=False, headers=None):
        ep = self.get_by_name(name)
        if exists is True and ep is None:
            return
        if ep is None:
            raise EndpointError(f"Endpoint with name: {name} does not exist.")
        self.client.perform_query('POST', f'{self.endpoint_name}/{ep.id}/start', headers=headers)
        state = self.get(ep.id).state
        while state in ["STARTING"]:
            time.sleep(2)
            state = self.get(ep.id).state
        if state != "RUNNING":
            raise EndpointError(f"Requested endpoint to be started but not started instead moved to: {state}")
        return


    def stop(self, name,exists=False, headers=None):
        ep = self.get_by_name(name)
        if exists is True and ep is None:
            return
        if ep is None:
            raise EndpointError(f"Endpoint with name: {name} does not exist.")
        self.client.perform_query('POST', f'{self.endpoint_name}/{ep.id}/stop', headers=headers)
        state = self.get(ep.id).state
        while state in ["STOPPING"]:
            time.sleep(2)
            state = self.get(ep.id).state
        if state != "STOPPED":
            raise EndpointError(f"Requested endpoint to be started but not started instead moved to: {state}")
        return

    # MAYBE GLOBAL CONTEXT CAN HAVE CONNECTION Object and cursor is generated on query call
    def run_sql(self, name, sql_stmt, access_token,limit=1000):
        ep = self.get_by_name(name)
        if ep == None:
            raise EndpointError(f"No endpoint with name {name} exists!")
        connection = sql.connect(
            server_hostname=ep.odbc_params.hostname,
            http_path=ep.odbc_params.path,
            access_token=access_token
        )
        cursor = connection.cursor()

        cursor.execute(sql_stmt)
        def get_columns(desc):
            return [i[0] for i in desc]
        df = DataFrame(cursor.fetchmany(size=limit))
        df.columns = get_columns(cursor.description)
        cursor.close()
        connection.close()
