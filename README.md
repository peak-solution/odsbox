# ASAM ODSBox

The odsbox is a lightweight Python wrapper on the standardized [ASAM ODS REST API](https://www.asam.net/standards/detail/ods/wiki/).
Using intuitive JAQuel Queries and [pandas.DataFrames](https://pandas.pydata.org/) the `odsbox` makes dealing with ASAM ODS
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

## Messages send via HTTP

The API makes use of the ASAM ODS protobuf message definitions provided at
[ASAM-ODS-Interfaces](https://github.com/asam-ev/ASAM-ODS-Interfaces)

## JAQuel Queries

JAQuel allows you to query your data in a simple and intuitive way following the concepts of the [MongoDB Query Language (MQL)](https://www.mongodb.com/docs/manual/tutorial/query-documents/). The definition of query expression as JSON easily integrates with the Python language – a win-win situation.
