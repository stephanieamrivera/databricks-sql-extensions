import ctypes
import functools
import json
import os
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Union, List, Optional, Any, Dict

import pandas as pd
from databricks_cli.sdk import ApiClient
from dataclasses_json import dataclass_json
from pyspark.sql.types import StructType

from sqle.endpoint_service import SqlEndpointsService, SqlEndpoint, EndpointError, SQL_ENDPOINT_STRUCT_TYPE


@functools.lru_cache(maxsize=None)
def get_so():
    file_name = "ddl2json_linux.so" if str(platform.platform()).lower().startswith("linux") else "ddl2json_darwin.so"
    path_to_so = os.getenv("SO_BUILD_PATH", str(Path(__file__).parent.absolute() / file_name))
    return ctypes.cdll.LoadLibrary(path_to_so)


class ParsedJsonResp(ctypes.Structure):
    _fields_ = [('parsed_json', ctypes.c_char_p),
                ('err', ctypes.c_char_p)]


def bool_c_char(i: bool):
    return str(i).encode("utf-8")


def bind_parser_method(resp_key, method_name: str = None):
    def wrapper(f):
        @functools.wraps(f)
        def func(sql: Union[str, bytes]) -> list:
            f_name = f.__name__ if method_name is None else method_name
            sql_statement = sql if isinstance(sql, bytes) else sql.encode("utf-8")
            so = get_so()
            parse_sql_external_location = getattr(so, f_name)
            parse_sql_external_location.argtypes = [ctypes.c_char_p]
            parse_sql_external_location.restype = ParsedJsonResp
            resp = parse_sql_external_location(sql_statement)
            free = so.free
            parsed_resp, err = resp.parsed_json, resp.err
            free(resp)
            if err is not None:
                raise Exception(err.decode("utf-8"))
            return json.loads(parsed_resp).get(resp_key, [])

        return func

    return wrapper


@dataclass_json
@dataclass
class Identifier:
    value: str


@dataclass_json
@dataclass
class Setting:
    key: str
    value: str


@dataclass
class Context:
    client: ApiClient
    session_token: str
    current_endpoint: Optional[str] = None
    current_endpoint_obj: Optional[SqlEndpoint] = None
    resp: Optional[Union[pd.DataFrame, List[Dict[str, Any]]]] = None
    resp_type: Optional[StructType] = None


@dataclass_json
@dataclass
class EndpointCreate:
    name: Identifier
    options: List[Setting]
    if_not_exists: Optional[bool] = False
    is_classic: Optional[bool] = False
    with_replacement: Optional[bool] = False

    def execute(self, ctx: Context) -> Context:
        name = self.name.value
        client = ctx.client
        base_json = {"name": name, "cluster_size": "2X-Small", "auto_stop_mins": "10",
                     "min_num_clusters": 1, "max_num_clusters": 1}
        options = {i.key: i.value for i in self.options}
        ctx.resp = [SqlEndpointsService(client).create(name,
                                                      {**base_json, **options},
                                                      if_not_exists=self.if_not_exists,
                                                      or_replace=self.with_replacement).to_dict()]
        ctx.resp_type = SQL_ENDPOINT_STRUCT_TYPE
        return ctx


@dataclass_json
@dataclass
class EndpointAlter:
    name: Identifier
    if_exists: Optional[bool] = False
    suspend: Optional[bool] = False
    resume: Optional[bool] = False

    def execute(self, ctx: Context) -> Context:
        name = self.name.value
        client = ctx.client
        if self.suspend is True:
            ctx.resp = SqlEndpointsService(client).stop(name, exists=self.if_exists)
        if self.resume is True:
            ctx.resp = SqlEndpointsService(client).start(name, exists=self.if_exists)
        ctx.resp = [{"endpoint": name, "operation": "suspend" if self.suspend is True else "resume"}]
        return ctx


@dataclass_json
@dataclass
class EndpointShow:
    endpoint_filter: Optional[str] = None

    def execute(self, ctx: Context) -> Context:
        client = ctx.client
        ctx.resp = [s.to_dict() for s in SqlEndpointsService(client).list().endpoints]
        ctx.resp_type = SQL_ENDPOINT_STRUCT_TYPE
        return ctx


@dataclass_json
@dataclass
class EndpointDrop:
    name: Identifier
    if_exists: Optional[bool] = False

    def execute(self, ctx: Context) -> Context:
        name = self.name.value
        client = ctx.client
        SqlEndpointsService(client).delete(name)
        if ctx.current_endpoint == name:
            ctx.current_endpoint = None
            ctx.current_endpoint_obj = None
        ctx.resp = [{"DELETED ENDPOINT": name}]
        return ctx


@dataclass_json
@dataclass
class EndpointUse:
    name: Identifier

    def execute(self, ctx: Context) -> Context:
        name = self.name.value
        client = ctx.client
        ep = SqlEndpointsService(client).get_by_name(name)
        if ep is None:
            raise EndpointError(f"No endpoint with name {name} exists! Please create one using 'CREATE ENDPOINT {name};'")
        ctx.current_endpoint = name
        ctx.current_endpoint_obj = ep
        ctx.resp = [{"current_endpoint": name}]
        return ctx


@dataclass_json
@dataclass
class Endpoint:
    create: Optional[EndpointCreate] = None
    alter: Optional[EndpointAlter] = None
    show: Optional[EndpointShow] = None
    drop: Optional[EndpointDrop] = None
    use: Optional[EndpointUse] = None

    def execute(self, ctx: Context) -> Context:
        if self.create is not None:
            return self.create.execute(ctx)
        if self.alter is not None:
            return self.alter.execute(ctx)
        if self.show is not None:
            return self.show.execute(ctx)
        if self.drop is not None:
            return self.drop.execute(ctx)
        if self.use is not None:
            return self.use.execute(ctx)


@dataclass_json
@dataclass
class Statement:
    endpoint: Optional[Endpoint] = None
    passthrough: Optional[str] = None

    def execute(self, ctx: Context) -> Context:
        client = ctx.client
        if self.endpoint is not None:
            return self.endpoint.execute(ctx)
        if self.passthrough is not None:
            if ctx.current_endpoint is None:
                raise EndpointError("No endpoint is currently selected; Please use the `USE ENDPOINT <name>` query!")
            ctx.resp = SqlEndpointsService(client).run_sql(ctx.current_endpoint, self.passthrough, ctx.session_token)
            return ctx

        # Do nothing catch any statement
        ctx.resp = None
        return ctx


@bind_parser_method("parsed_statements_list")
def parseSql(sql: Union[str, bytes]):
    pass
