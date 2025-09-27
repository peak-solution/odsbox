# JAQuel Query Examples

This document contains examples of JAQuel queries and their corresponding ASAM ODS SelectStatement protobuf message serialized as JSON.


## Access Instances


### Basic access


#### Add some of the related physical dimension

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "Unit": {},
    "$attributes": {
        "name": 1,
        "factor": 1,
        "offset": 1,
        "phys_dimension.name": 1,
        "phys_dimension.length_exp": 1,
        "phys_dimension.mass_exp": 1
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Factor"
    },
    {
      "aid": "54",
      "attribute": "Offset"
    },
    {
      "aid": "47",
      "attribute": "Name"
    },
    {
      "aid": "47",
      "attribute": "Length"
    },
    {
      "aid": "47",
      "attribute": "Mass"
    }
  ],
  "joins": [
    {
      "aidFrom": "54",
      "aidTo": "47",
      "relation": "PhysDimension"
    }
  ]
}
```

````


#### Do access only few attributes

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "Unit": {},
    "$attributes": {
        "name": 1,
        "factor": 1,
        "offset": 1
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Factor"
    },
    {
      "aid": "54",
      "attribute": "Offset"
    }
  ]
}
```

````


#### Lets query for units with base entity name

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {}
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ]
}
```

````


#### Lets query for units with entity name

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "Unit": {}
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ]
}
```

````


#### Retrieve all attributes using asterisk

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "Unit": {},
    "$attributes": {
        "name": 1,
        "factor": 1,
        "offset": 1,
        "phys_dimension.*": 1
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Factor"
    },
    {
      "aid": "54",
      "attribute": "Offset"
    },
    {
      "aid": "47",
      "attribute": "*"
    }
  ],
  "joins": [
    {
      "aidFrom": "54",
      "aidTo": "47",
      "relation": "PhysDimension"
    }
  ]
}
```

````


#### Simplify attribute definition

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "Unit": {},
    "$attributes": {
        "name": 1,
        "factor": 1,
        "offset": 1,
        "phys_dimension": {
            "name": 1,
            "length_exp": 1,
            "mass_exp": 1
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Factor"
    },
    {
      "aid": "54",
      "attribute": "Offset"
    },
    {
      "aid": "47",
      "attribute": "Name"
    },
    {
      "aid": "47",
      "attribute": "Length"
    },
    {
      "aid": "47",
      "attribute": "Mass"
    }
  ],
  "joins": [
    {
      "aidFrom": "54",
      "aidTo": "47",
      "relation": "PhysDimension"
    }
  ]
}
```

````


### Order results by an attribute


#### Order results by  name

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {},
    "$attributes": {
        "id": 1,
        "name": 1
    },
    "$orderby": {
        "name": 1
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Id"
    },
    {
      "aid": "54",
      "attribute": "Name"
    }
  ],
  "orderBy": [
    {
      "aid": "54",
      "attribute": "Name"
    }
  ]
}
```

````


### Limit the amounts of results


#### Retrieve only 5 result rows

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {},
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "rowLimit": "5"
}
```

````


### Query Instance by  id


#### Query the unit with the id 3

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "id": 3
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Id",
        "longlongArray": {
          "values": [
            "3"
          ]
        }
      }
    }
  ]
}
```

````


#### Query the unit with the id 3 full

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "id": {
            "$eq": 3
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Id",
        "longlongArray": {
          "values": [
            "3"
          ]
        }
      }
    }
  ]
}
```

````


#### Query the unit with the id 3 simplified

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": 3
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Id",
        "longlongArray": {
          "values": [
            "3"
          ]
        }
      }
    }
  ]
}
```

````


#### Query the units with the ids in 1 , 2 and 3

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "id": {
            "$in": [
                1,
                2,
                3
            ]
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Id",
        "operator": "OP_INSET",
        "longlongArray": {
          "values": [
            "1",
            "2",
            "3"
          ]
        }
      }
    }
  ]
}
```

````


### Query Instance by  name


#### Query the unit with the name equal  s

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "name": "s"
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Name",
        "stringArray": {
          "values": [
            "s"
          ]
        }
      }
    }
  ]
}
```

````


#### Query the unit with the name equal  s  case insensitive

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "name": "s",
        "$options": "i"
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Name",
        "operator": "OP_CI_EQ",
        "stringArray": {
          "values": [
            "s"
          ]
        }
      }
    }
  ]
}
```

````


#### Query the unit with the name equal  s  case insensitive full

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "name": {
            "$eq": "s"
        },
        "$options": "i"
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Name",
        "operator": "OP_CI_EQ",
        "stringArray": {
          "values": [
            "s"
          ]
        }
      }
    }
  ]
}
```

````


#### Query the unit with the name like  k

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "name": {
            "$like": "k*"
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Name",
        "operator": "OP_LIKE",
        "stringArray": {
          "values": [
            "k*"
          ]
        }
      }
    }
  ]
}
```

````


#### Query the unit with the name like  k   case insensitive

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "name": {
            "$like": "k*"
        },
        "$options": "i"
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "54",
        "attribute": "Name",
        "operator": "OP_CI_LIKE",
        "stringArray": {
          "values": [
            "k*"
          ]
        }
      }
    }
  ]
}
```

````


### And conjunctions  $and  and  $or


#### Search speed based Units

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "phys_dimension": {
            "length_exp": 1,
            "mass_exp": 0,
            "time_exp": -1,
            "current_exp": 0,
            "temperature_exp": 0,
            "molar_amount_exp": 0,
            "luminous_intensity_exp": 0
        }
    },
    "$attributes": {
        "name": 1,
        "factor": 1,
        "offset": 1,
        "phys_dimension.name": 1
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Factor"
    },
    {
      "aid": "54",
      "attribute": "Offset"
    },
    {
      "aid": "47",
      "attribute": "Name"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "47",
        "attribute": "Length",
        "longArray": {
          "values": [
            1
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Mass",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Time",
        "longArray": {
          "values": [
            -1
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Current",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Temperature",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "MolarAmount",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "LuminousIntensity",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    }
  ],
  "joins": [
    {
      "aidFrom": "54",
      "aidTo": "47",
      "relation": "PhysDimension"
    }
  ]
}
```

````


#### Search speed based Units with explicit  $and  conjunction

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "phys_dimension": {
            "$and": [
                {
                    "length_exp": 1
                },
                {
                    "mass_exp": 0
                },
                {
                    "time_exp": -1
                },
                {
                    "current_exp": 0
                },
                {
                    "temperature_exp": 0
                },
                {
                    "molar_amount_exp": 0
                },
                {
                    "luminous_intensity_exp": 0
                }
            ]
        }
    },
    "$attributes": {
        "name": 1,
        "factor": 1,
        "offset": 1,
        "phys_dimension.name": 1
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Factor"
    },
    {
      "aid": "54",
      "attribute": "Offset"
    },
    {
      "aid": "47",
      "attribute": "Name"
    }
  ],
  "where": [
    {
      "conjunction": "CO_OPEN"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Length",
        "longArray": {
          "values": [
            1
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Mass",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Time",
        "longArray": {
          "values": [
            -1
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Current",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Temperature",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "MolarAmount",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "LuminousIntensity",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_CLOSE"
    }
  ],
  "joins": [
    {
      "aidFrom": "54",
      "aidTo": "47",
      "relation": "PhysDimension"
    }
  ]
}
```

````


#### Search speed or time based Units

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "phys_dimension": {
            "$or": [
                {
                    "length_exp": 1,
                    "mass_exp": 0,
                    "time_exp": -1,
                    "current_exp": 0,
                    "temperature_exp": 0,
                    "molar_amount_exp": 0,
                    "luminous_intensity_exp": 0
                },
                {
                    "length_exp": 0,
                    "mass_exp": 0,
                    "time_exp": 1,
                    "current_exp": 0,
                    "temperature_exp": 0,
                    "molar_amount_exp": 0,
                    "luminous_intensity_exp": 0
                }
            ]
        }
    },
    "$attributes": {
        "name": 1,
        "factor": 1,
        "offset": 1,
        "phys_dimension.name": 1
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Factor"
    },
    {
      "aid": "54",
      "attribute": "Offset"
    },
    {
      "aid": "47",
      "attribute": "Name"
    }
  ],
  "where": [
    {
      "conjunction": "CO_OPEN"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Length",
        "longArray": {
          "values": [
            1
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Mass",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Time",
        "longArray": {
          "values": [
            -1
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Current",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Temperature",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "MolarAmount",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "LuminousIntensity",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_OR"
    },
    {
      "conjunction": "CO_OPEN"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Length",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Mass",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Time",
        "longArray": {
          "values": [
            1
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Current",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Temperature",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "MolarAmount",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "LuminousIntensity",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_CLOSE"
    },
    {
      "conjunction": "CO_CLOSE"
    }
  ],
  "joins": [
    {
      "aidFrom": "54",
      "aidTo": "47",
      "relation": "PhysDimension"
    }
  ]
}
```

````


#### Search time based Units

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {
        "phys_dimension": {
            "length_exp": 0,
            "mass_exp": 0,
            "time_exp": 1,
            "current_exp": 0,
            "temperature_exp": 0,
            "molar_amount_exp": 0,
            "luminous_intensity_exp": 0
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "47",
        "attribute": "Length",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Mass",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Time",
        "longArray": {
          "values": [
            1
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Current",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "Temperature",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "MolarAmount",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    },
    {
      "conjunction": "CO_AND"
    },
    {
      "condition": {
        "aid": "47",
        "attribute": "LuminousIntensity",
        "longArray": {
          "values": [
            0
          ]
        }
      }
    }
  ],
  "joins": [
    {
      "aidFrom": "54",
      "aidTo": "47",
      "relation": "PhysDimension"
    }
  ]
}
```

````


### Use  $between  operator


#### Get measurements that started in a time interval

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoMeasurement": {
        "measurement_begin": {
            "$between": [
                "2000-04-22T00:00:00.001Z",
                "2024-04-23T00:00:00.002Z"
            ]
        }
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "79",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "79",
        "attribute": "MeasurementBegin",
        "operator": "OP_BETWEEN",
        "stringArray": {
          "values": [
            "20000422000000001",
            "20240423000000002"
          ]
        }
      }
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get measurements that started in a time interval, ODS time

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoMeasurement": {
        "measurement_begin": {
            "$between": [
                "20001223000000",
                "20241224000000"
            ]
        }
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "79",
      "attribute": "*"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "79",
        "attribute": "MeasurementBegin",
        "operator": "OP_BETWEEN",
        "stringArray": {
          "values": [
            "20001223000000",
            "20241224000000"
          ]
        }
      }
    }
  ],
  "rowLimit": "5"
}
```

````


### Use aggregates  $min ,  $max ,  $dcount , and  $distinct


#### Get the distincted count of Unit description

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {},
    "$attributes": {
        "description": {
            "$dcount": 1
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Description",
      "aggregate": "AG_DCOUNT"
    }
  ]
}
```

````


#### Get the distincted values of Unit description

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {},
    "$attributes": {
        "description": {
            "$distinct": 1
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Description",
      "aggregate": "AG_DISTINCT"
    }
  ]
}
```

````


#### Get the min and max of unit factor

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {},
    "$attributes": {
        "factor": {
            "$max": 1,
            "$min": 1
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Factor",
      "aggregate": "AG_MAX"
    },
    {
      "aid": "54",
      "attribute": "Factor",
      "aggregate": "AG_MIN"
    }
  ]
}
```

````


#### Get the min and max of unit factor and offset

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoUnit": {},
    "$attributes": {
        "factor": {
            "$max": 1,
            "$min": 1
        },
        "offset": {
            "$max": 1,
            "$min": 1
        }
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "54",
      "attribute": "Factor",
      "aggregate": "AG_MAX"
    },
    {
      "aid": "54",
      "attribute": "Factor",
      "aggregate": "AG_MIN"
    },
    {
      "aid": "54",
      "attribute": "Offset",
      "aggregate": "AG_MAX"
    },
    {
      "aid": "54",
      "attribute": "Offset",
      "aggregate": "AG_MIN"
    }
  ]
}
```

````


### Inner and outer joins


#### Use inner join

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoMeasurementQuantity": {},
    "$attributes": {
        "name": 1,
        "unit.name": 1,
        "quantity.name": 1
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "80",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "55",
      "attribute": "Name"
    }
  ],
  "joins": [
    {
      "aidFrom": "80",
      "aidTo": "54",
      "relation": "Unit"
    },
    {
      "aidFrom": "80",
      "aidTo": "55",
      "relation": "Quantity"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Use outer join

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoMeasurementQuantity": {},
    "$attributes": {
        "name": 1,
        "unit:OUTER.name": 1,
        "quantity:OUTER.name": 1
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "80",
      "attribute": "Name"
    },
    {
      "aid": "54",
      "attribute": "Name"
    },
    {
      "aid": "55",
      "attribute": "Name"
    }
  ],
  "joins": [
    {
      "aidFrom": "80",
      "aidTo": "54",
      "relation": "Unit",
      "joinType": "JT_OUTER"
    },
    {
      "aidFrom": "80",
      "aidTo": "55",
      "relation": "Quantity",
      "joinType": "JT_OUTER"
    }
  ],
  "rowLimit": "5"
}
```

````


### Use  $groupby


#### Use a  $groupby  on two attributes

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoMeasurement": {},
    "$attributes": {
        "name": 1,
        "description": 1
    },
    "$orderby": {
        "name": 1
    },
    "$groupby": {
        "name": 1,
        "description": 1
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "79",
      "attribute": "Name"
    },
    {
      "aid": "79",
      "attribute": "Description"
    }
  ],
  "orderBy": [
    {
      "aid": "79",
      "attribute": "Name"
    }
  ],
  "groupBy": [
    {
      "aid": "79",
      "attribute": "Name"
    },
    {
      "aid": "79",
      "attribute": "Description"
    }
  ]
}
```

````


## Access OpenMDM content


### Access hierarchy elements


#### Get  AoTest  instances

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoTest": {},
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "48",
      "attribute": "*"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  MeaQuantity  instances

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "MeaResult": {},
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "79",
      "attribute": "*"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  MeaResult  instances

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "MeaResult": {},
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "79",
      "attribute": "*"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  Project  instances

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "Project": {},
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "48",
      "attribute": "*"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  StructureLevel  instances

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "StructureLevel": {},
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "67",
      "attribute": "*"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  Test  instances

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "Test": {},
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "77",
      "attribute": "*"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  TestStep  instances

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "TestStep": {},
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "78",
      "attribute": "*"
    }
  ],
  "rowLimit": "5"
}
```

````


### Browse ODS tree


#### Get  MeaResult  from  TestStep  with  id  equal 4

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "TestStep": 4,
    "$attributes": {
        "children": {
            "name": 1,
            "id": 1
        }
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "79",
      "attribute": "Name"
    },
    {
      "aid": "79",
      "attribute": "Id"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "78",
        "attribute": "Id",
        "longlongArray": {
          "values": [
            "4"
          ]
        }
      }
    }
  ],
  "joins": [
    {
      "aidFrom": "79",
      "aidTo": "78",
      "relation": "TestStep"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  MeaResult  with  test  with  id  equal 4

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "MeaResult": {
        "test": 4
    },
    "$attributes": {
        "name": 1,
        "id": 1
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "79",
      "attribute": "Name"
    },
    {
      "aid": "79",
      "attribute": "Id"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "79",
        "attribute": "TestStep",
        "longlongArray": {
          "values": [
            "4"
          ]
        }
      }
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  StructureLevel  from  Project  with  id  equal 3

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "Project": 3,
    "$attributes": {
        "children": {
            "name": 1,
            "id": 1
        }
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "67",
      "attribute": "Name"
    },
    {
      "aid": "67",
      "attribute": "Id"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "48",
        "attribute": "Id",
        "longlongArray": {
          "values": [
            "3"
          ]
        }
      }
    }
  ],
  "joins": [
    {
      "aidFrom": "67",
      "aidTo": "48",
      "relation": "Project"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  StructureLevel  with  parent_test  with  id  equal 3

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "StructureLevel": {
        "parent_test": 3
    },
    "$attributes": {
        "name": 1,
        "id": 1
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "67",
      "attribute": "Name"
    },
    {
      "aid": "67",
      "attribute": "Id"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "67",
        "attribute": "Project",
        "longlongArray": {
          "values": [
            "3"
          ]
        }
      }
    }
  ],
  "rowLimit": "5"
}
```

````


### Access descriptive meta


#### Get some meta of related  AoUnitUnderTestPart  instances

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "MeaResult": {},
    "$attributes": {
        "id": 1,
        "name": 1,
        "units_under_test": {
            "id": 1,
            "vehicle.model": 1,
            "engine.type": 1,
            "gearbox.transmission": 1
        }
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "79",
      "attribute": "Id"
    },
    {
      "aid": "79",
      "attribute": "Name"
    },
    {
      "aid": "56",
      "attribute": "Id"
    },
    {
      "aid": "95",
      "attribute": "model"
    },
    {
      "aid": "88",
      "attribute": "type"
    },
    {
      "aid": "91",
      "attribute": "transmission"
    }
  ],
  "joins": [
    {
      "aidFrom": "79",
      "aidTo": "56",
      "relation": "UnitUnderTest"
    },
    {
      "aidFrom": "95",
      "aidTo": "56",
      "relation": "UnitUnderTest"
    },
    {
      "aidFrom": "88",
      "aidTo": "56",
      "relation": "UnitUnderTest"
    },
    {
      "aidFrom": "91",
      "aidTo": "56",
      "relation": "UnitUnderTest"
    }
  ],
  "rowLimit": "5"
}
```

````


### Access parameter sets


#### Get name value pairs attached to  MeaResult

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "ResultParameter": {
        "parameter_set.MeaResult.Name": {
            "$like": "APS*"
        }
    },
    "$attributes": {
        "Name": 1,
        "Value": 1,
        "DataType": 1,
        "parameter_set": {
            "name": 1,
            "MeaResult.id": 1
        }
    },
    "$options": {
        "$rowlimit": 20
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "66",
      "attribute": "Name"
    },
    {
      "aid": "66",
      "attribute": "Value"
    },
    {
      "aid": "66",
      "attribute": "DataType"
    },
    {
      "aid": "49",
      "attribute": "Name"
    },
    {
      "aid": "79",
      "attribute": "Id"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "79",
        "attribute": "Name",
        "operator": "OP_LIKE",
        "stringArray": {
          "values": [
            "APS*"
          ]
        }
      }
    }
  ],
  "joins": [
    {
      "aidFrom": "66",
      "aidTo": "49",
      "relation": "ResultParameterSet"
    },
    {
      "aidFrom": "49",
      "aidTo": "79",
      "relation": "MeaResult"
    }
  ],
  "rowLimit": "20"
}
```

````


## Access Bulk


### Read data from Measurement


#### Get  AoMeasurementQuantity  from  AoMeasurement  with  id  equal 153

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoMeasurement": 153,
    "$attributes": {
        "measurement_quantities": {
            "name": 1,
            "id": 1
        }
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "80",
      "attribute": "Name"
    },
    {
      "aid": "80",
      "attribute": "Id"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "79",
        "attribute": "Id",
        "longlongArray": {
          "values": [
            "153"
          ]
        }
      }
    }
  ],
  "joins": [
    {
      "aidFrom": "80",
      "aidTo": "79",
      "relation": "MeaResult"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  AoMeasurementQuantity  with  measurement  with  id  equal 153

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoMeasurementQuantity": {
        "measurement": 153
    },
    "$attributes": {
        "name": 1,
        "id": 1
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "80",
      "attribute": "Name"
    },
    {
      "aid": "80",
      "attribute": "Id"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "80",
        "attribute": "MeaResult",
        "longlongArray": {
          "values": [
            "153"
          ]
        }
      }
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get  AoSubmatrix  with  measurement  with  id  equal 153

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoSubmatrix": {
        "measurement": 153
    },
    "$attributes": {
        "name": 1,
        "id": 1,
        "number_of_rows": 1
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "81",
      "attribute": "Name"
    },
    {
      "aid": "81",
      "attribute": "Id"
    },
    {
      "aid": "81",
      "attribute": "SubMatrixNoRows"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "81",
        "attribute": "MeaResult",
        "longlongArray": {
          "values": [
            "153"
          ]
        }
      }
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get bulk of  AoLocalColumn  with  submatrix.measurement  with  id  equal 153

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoLocalColumn": {
        "submatrix.measurement": 153
    },
    "$attributes": {
        "id": 1,
        "flags": 1,
        "generation_parameters": 1,
        "values": 1
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "82",
      "attribute": "Id"
    },
    {
      "aid": "82",
      "attribute": "Flags"
    },
    {
      "aid": "82",
      "attribute": "GenerationParameters"
    },
    {
      "aid": "82",
      "attribute": "Values"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "81",
        "attribute": "MeaResult",
        "longlongArray": {
          "values": [
            "153"
          ]
        }
      }
    }
  ],
  "joins": [
    {
      "aidFrom": "82",
      "aidTo": "81",
      "relation": "SubMatrix"
    }
  ],
  "rowLimit": "5"
}
```

````


#### Get meta of  AoLocalColumn  with  submatrix.measurement  with  id  equal 153

````{tab-set}

```{tab-item} JAQuel Query

```json
{
    "AoLocalColumn": {
        "submatrix.measurement": 153
    },
    "$attributes": {
        "name": 1,
        "id": 1,
        "sequence_representation": 1,
        "independent": 1,
        "global_flag": 1
    },
    "$options": {
        "$rowlimit": 5
    }
}
```

```{tab-item} ODS SelectStatement

```json
{
  "columns": [
    {
      "aid": "82",
      "attribute": "Name"
    },
    {
      "aid": "82",
      "attribute": "Id"
    },
    {
      "aid": "82",
      "attribute": "SequenceRepresentation"
    },
    {
      "aid": "82",
      "attribute": "IndependentFlag"
    },
    {
      "aid": "82",
      "attribute": "GlobalFlag"
    }
  ],
  "where": [
    {
      "condition": {
        "aid": "81",
        "attribute": "MeaResult",
        "longlongArray": {
          "values": [
            "153"
          ]
        }
      }
    }
  ],
  "joins": [
    {
      "aidFrom": "82",
      "aidTo": "81",
      "relation": "SubMatrix"
    }
  ],
  "rowLimit": "5"
}
```

````
