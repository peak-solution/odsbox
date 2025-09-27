# ASAM ODSBox

![PyPI Version](https://img.shields.io/pypi/v/odsbox)
![Python Versions](https://img.shields.io/pypi/pyversions/odsbox)
![License](https://img.shields.io/github/license/peak-solution/odsbox)

The [*ODSBox*](https://peak-solution.github.io/odsbox) is a lightweight Python wrapper on the standardized
[ASAM ODS REST API](https://www.asam.net/standards/detail/ods/wiki/). It simplifies the handling of ASAM ODS data
in Python by utilizing intuitive [JAQuel](https://peak-solution.github.io/odsbox/jaquel.html#examples) queries
and [pandas DataFrames](https://pandas.pydata.org/).

## Features

- Lightweight Python wrapper for the [ASAM ODS HTTP API](https://www.asam.net/standards/detail/ods/wiki/#TechnicalContent).
- Intuitive querying using [JAQuel](https://peak-solution.github.io/odsbox/jaquel.html#examples) Query Language.
- Seamless integration with pandas DataFrames for data analysis.
- Includes generated [ASAM ODS protobuf](https://github.com/asam-ev/ASAM-ODS-Interfaces) stubs.
- Supports authentication mechanisms via [requests](https://pypi.org/project/requests/).
- [Bulk helper](https://peak-solution.github.io/odsbox/odsbox.html#odsbox.con_i.ConI.bulk) for quantity data access as
  pandas DataFrames.

## Quick Start

1. Install ODSBox: `pip install odsbox`
2. Connect to an ASAM ODS server:
    ```python
    from odsbox.con_i import ConI

    with ConI(url="https://MY_SERVER/api", auth=("USERNAME", "PASSWORD")) as con_i:
        measurements = con_i.query_data(
            {
                "AoMeasurement": {"name": {"$like": "*"}},
                "$attributes": {"name": 1, "id": 1},
                "$options": {"$rowlimit": 50},
            }
        )
        print(measurements) # print pandas DataFrame
    ```

## Documentation

* [*ODSBox* documentation](https://peak-solution.github.io/odsbox)
* [Work with ASAM ODS server](https://peak-solution.github.io/data_management_learning_path/ods/query-asam-server.html)

## Communication

The ASAM ODS server communicates via HTTP calls as specified in the [ASAM ODS standard](https://www.asam.net/standards/detail/ods/wiki/#TechnicalContent), which defines a RESTful API for accessing and manipulating measurement data. This allows for standardized interactions, including operations like querying, creating, updating, and deleting entities such as measurements, tests, and results.

Data exchanged between the client (e.g., ODSBox) and the server is encoded and decoded using [Protocol Buffers (protobuf)](https://protobuf.dev/programming-guides/proto3/), a binary serialization format developed by Google. Protobuf ensures efficient, compact, and fast data transfer compared to text-based formats like JSON, making it ideal for handling large datasets in technical measurement scenarios. The official protobuf message definitions are available in the [ASAM-ODS-Interfaces repository](https://github.com/asam-ev/ASAM-ODS-Interfaces).

Requests use the custom content type `application/x-asamods+protobuf` to indicate protobuf-encoded payloads. ODSBox abstracts these low-level details, allowing developers to interact with the API using intuitive Python dictionaries and methods, while handling authentication (e.g., via username/password or tokens) and connection management seamlessly.

## JAQuel Queries

[JAQuel](https://peak-solution.github.io/odsbox/jaquel.html#examples) enables intuitive data querying, drawing inspiration from the [MongoDB Query Language (MQL)](https://www.mongodb.com/docs/manual/tutorial/query-documents/). Its JSON-based query expressions seamlessly integrate with Python as dictionaries, offering a powerful and user-friendly approach to data retrieval.

## Installation

*ODSBox* is available on PyPI and GitHub.

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

## Contributing

We welcome contributions! See CONTRIBUTING.md for guidelines.

## License

This project is licensed under the MIT License.
