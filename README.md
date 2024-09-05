# ASAM ODSBox

The [*ODSBox*](https://peak-solution.github.io/odsbox) is a lightweight Python wrapper on the standardized [ASAM ODS REST API](https://www.asam.net/standards/detail/ods/wiki/).
Using intuitive JAQuel queries and [pandas.DataFrames](https://pandas.pydata.org/) the *ODSBox* makes dealing with ASAM ODS
data in Python more fun.

``` python
from odsbox.con_i import ConI

with ConI(url="https://MY_SERVER/api", auth=("sa", "sa")) as con_i:
    measurements = con_i.query_data(
        {
            "AoMeasurement": {"name": {"$like": "*"}},
            "$attributes": {"name": 1, "id": 1},
            "$options": {"$rowlimit": 50},
        }
    )
    print(measurements)
```

See [documentation](https://peak-solution.github.io/odsbox) for overview.

## Communication

The ASAM ODS server is called using HTTP calls defined by the [ASAM ODS standard](https://www.asam.net/standards/detail/ods/wiki/#TechnicalContent).
The messages are encode/decoded using ASAM ODS [protobuf](https://protobuf.dev/programming-guides/proto3/) message definitions provided at
[ASAM-ODS-Interfaces](https://github.com/asam-ev/ASAM-ODS-Interfaces). As content type `application/x-asamods+protobuf` is used.

## JAQuel Queries

JAQuel allows you to query your data in a simple and intuitive way inspired
by [MongoDB Query Language (MQL)](https://www.mongodb.com/docs/manual/tutorial/query-documents/).
The definition of query expression as JSON easily integrates with the Python language – a win-win situation.

## Installation

*ODSBox* is available on.

|               |                                                                                      |
| :------------ | :----------------------------------------------------------------------------------- |
| github        | [https://github.com/peak-solution/odsbox/](https://github.com/peak-solution/odsbox/) |
| PyPI          | [https://pypi.org/project/odsbox/](https://pypi.org/project/odsbox/)                 |
| github docs   | [https://peak-solution.github.io/odsbox](https://peak-solution.github.io/odsbox)     |


```shell
# access ASAM ODS server
pip install odsbox
# access ASAM ODS EXD-API plugin
pip install odsbox[exd-data]
```
