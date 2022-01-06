from __future__ import print_function

import pandas as pd
from databricks_cli.sdk import ApiClient

from sqle.execution import parseSql, Context, Statement


def patch_sqle(host, token, spark, display, debug=False):
    from IPython.core.magic import Magics, magics_class, cell_magic

    @magics_class
    class SqlExtensions(Magics):

        client = ApiClient(host=host, token=token)
        ctx = Context(client=client, session_token=token)
        debug_stmt = debug

        @cell_magic
        def sqle(self, *args):
            "Replace current line with new output"
            stmt = "\n".join(args[1:])
            data = parseSql(stmt)
            for i in data:
                stmt: Statement = Statement.from_dict(i)
                if self.debug_stmt is True:
                    print(i)
                self.ctx = stmt.execute(self.ctx)
                if self.ctx.resp is not None and isinstance(self.ctx.resp, pd.DataFrame):
                    display(self.ctx.resp)
                if self.ctx.resp is not None and isinstance(self.ctx.resp, list):
                    df = spark.createDataFrame(self.ctx.resp,
                                               schema=self.ctx.resp_type if self.ctx.resp_type is not None else None)
                    display(df)
                # flush responses after displaying
                self.ctx.resp = None
                self.ctx.resp_type = None

    ip = get_ipython()
    print("PATCHING SQL EXTENSIONS (currently only supports nephos endpoints)")
    ip.register_magics(SqlExtensions)
