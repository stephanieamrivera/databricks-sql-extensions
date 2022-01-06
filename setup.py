from setuptools import setup, find_packages

setup(
    name="databricks-sql-extensions",
    version="0.1.0",
    author="Sriharsha Tikkireddy",
    author_email="sri.tikkireddy@databricks.com",
    description="Databricks Sql Extensions for Notebooks",
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=['tests', 'tests.*', ]),
    install_requires=[
        "dataclasses-json",
        "databricks-sql-connector",
        "pandas",
         "timeout-decorator",
        "databricks-cli"
    ],
    package_data={'': ['ddl2json_darwin.so', 'ddl2json_linux.so','ddl2json_darwin.h','ddl2json_linux.h']},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
