# JAQueL

**IMPORTANT**: It is always possible to use application or base names for entity, relations, attributes.

The lookup order is:

1. **application name**
2. **base name**<br/>
   If base name is not uniquely assigned in the model just one is picked.
   This is important for writing generic code without having to analyze
   the complete application model. Be aware that application name can
   always be used to access any element or attribute.

*Almost all examples use base names to make they work on any application model.*

## Examples

### Get all AoTest instances

```json
{
    "AoTest": {}
}
```

### Access instances using id only

Get measurement with id 4711

```json
{
    "AoMeasurement": 4711
}
```

```json
{
    "AoMeasurement": "4711"
}
```

```json
{
    "AoMeasurement": {
        "id": 4711
    }
}
```

Using application names.

```json
{
    "MeaResult": {
        "Id": 4711
    }
}
```


### Get children of a given SubTest


```json
{
    "AoMeasurement": {
        "test": 4611
    }
}
```

```json
{
    "AoMeasurement": {
        "test.id": 4611
    }
}
```

TIP: `test.id` is a duplicate because the id is also stored in test column

### Use inverse to do the same job

```json
{
    "AoSubTest": "4611",
    "$attributes": {
        "children.name": 1,
        "children.id": 1
    }
}
```

```json
{
    "AoSubTest": "4611",
    "$attributes": {
        "children": {
            "name": 1,
            "id": 1
        }
    }
}
```

### Search for a AoTestSequence by name and version

```json
{
    "AoTestSequence": {
        "name": "MyTestSequence",
        "version": "V1"
    }
}
```
### Case insensitive match

```json
{
    "AoTest": {
        "name": {
            "$eq": "MyTest",
            "$options": "i"
        }
    }
}
```
### Case insensitive match

```json
{
    "AoTest": {
        "name": {
            "$like": "My*",
            "$options": "i"
        }
    }
}
```

### Resolve asam path

```json
{
    "AoMeasurement": {
        "name": "MyMea",
        "version": "V1",
        "test.name": "MySubTest2",
        "test.version": "V1",
        "test.parent_test.name": "MySubTest1",
        "test.parent_test.version": "V1",
        "test.parent_test.parent_test.name": "MyTest",
        "test.parent_test.parent_test.version": "V1"
    }
}
```

```json
{
    "AoMeasurement": {
        "name": "MyMea",
        "version": "V1",
        "test": {
            "name": "MySubTest2",
            "version": "V1",
            "parent_test": {
                "name": "MySubTest1",
                "version": "V1",
                "parent_test": {
                    "name": "MyTest",
                    "version": "V1"
                }
            }
        }
    }
}
```

### Use $in operator

```json
{
    "AoMeasurement": {
        "id": {
            "$in": [
                4711,
                4712,
                4713
            ]
        }
    }
}
```

### Search for a time span

```json
{
    "AoMeasurement": {
        "measurement_begin": {
            "$gte": "2012-04-23T00:00:00.000Z",
            "$lt": "2012-04-24T00:00:00.000Z"
        }
    }
}
```

### Use between operator

```json
{
    "AoMeasurement": {
        "measurement_begin": {
            "$between": [
                "2012-04-23T00:00:00.000Z",
                "2012-04-24T00:00:00.000Z"
            ]
        }
    }
}
```

### Simple $and example

```json
{
    "AoMeasurement": {
        "$and": [
            {
                "measurement_begin": {
                    "$gte": "2012-04-23T00:00:00.000Z",
                    "$lt": "2012-04-24T00:00:00.000Z"
                }
            },
            {
                "measurement_end": {
                    "$gte": "2012-04-23T00:00:00.000Z",
                    "$lt": "2012-04-24T00:00:00.000Z"
                }
            }
        ]
    }
}
```

### Simple or example

```json
{
    "AoMeasurement": {
        "$or": [
            {
                "measurement_begin": {
                    "$gte": "2012-04-23T00:00:00.000Z",
                    "$lt": "2012-04-24T00:00:00.000Z"
                }
            },
            {
                "measurement_begin": {
                    "$gte": "2012-05-23T00:00:00.000Z",
                    "$lt": "2012-05-24T00:00:00.000Z"
                }
            },
            {
                "measurement_begin": {
                    "$gte": "2012-06-23T00:00:00.000Z",
                    "$lt": "2012-06-24T00:00:00.000Z"
                }
            }
        ]
    }
}
```

### Simple $not example

```json
{
    "AoTestSequence": {
        "$not": {
            "$and": [
                {
                    "name": "MyTestSequence"
                },
                {
                    "version": "V1"
                }
            ]
        }
    }
}
```

### Mixed case sensitive/insensitive

```json
{
    "AoTest": {
        "$and": [
            {
                "name": {
                    "$like": "My*",
                    "$options": "i"
                }
            },
            {
                "name": {
                    "$like": "??Test"
                }
            }
        ]
    }
}
```
### Define unit for attribute to be retrieved

```json
{
    "AoMeasurementQuantity": 4711,
    "$attributes": {
        "name": 1,
        "id": 1,
        "maximum": {
            "$unit": 1234
        }
    }
}
```

### Define unit for attribute value in condition

```json
{
    "AoMeasurementQuantity": {
        "maximum": {
            "$unit": 3,
            "$between": [
                1.2,
                2.3
            ]
        }
    }
}
```

### Access $min and $max from minimum and maximum

```json
{
    "AoMeasurementQuantity": {
        "name": "Revs"
    },
    "$attributes": {
        "minimum": {
            "$min": 1,
            "$max": 1
        },
        "maximum": {
            "$min": 1,
            "$max": 1
        }
    }
}
```

### Do a full query filling some query elements

```json
{
    "AoMeasurement": {
        "$or": [
            {
                "measurement_begin": {
                    "$gte": "2012-04-23T00:00:00.000Z",
                    "$lt": "2012-04-24T00:00:00.000Z"
                }
            },
            {
                "measurement_begin": {
                    "$gte": "2012-05-23T00:00:00.000Z",
                    "$lt": "2012-05-24T00:00:00.000Z"
                }
            },
            {
                "measurement_begin": {
                    "$gte": "2012-06-23T00:00:00.000Z",
                    "$lt": "2012-06-24T00:00:00.000Z"
                }
            }
        ]
    },
    "$options": {
        "$rowlimit": 1000,
        "$rowskip": 500
    },
    "$attributes": {
        "name": 1,
        "id": 1,
        "test": {
            "name": 1,
            "id": 1
        }
    },
    "$orderby": {
        "test.name": 0,
        "name": 1
    },
    "$groupby": {
        "id": 1
    }
}
```

```json
{
    "AoMeasurement": {},
    "$attributes": {
        "name": {
            "$distinct": 1
        }
    }
}
```

### Use outer join to retrieve sparse set unit names

```json
{
    "AoMeasurementQuantity": {
        "measurement": 4712
    },
    "$attributes": {
        "name": 1,
        "id": 1,
        "datatype": 1,
        "unit:OUTER.name": 1
    }
}
```

## Special key values

| top level   | description                                 |
|-------------|---------------------------------------------|
| $attributes | list of attributes to retrieve.             |
| $orderby    | order the results by this (1 ascending, 0 descending). |
| $groupby    | group the results by this.                  |
| $options    | global options.                             |

| conjunctions | description                                |
|--------------|--------------------------------------------|
| $and         | connect array elements with logical AND. Contains Array of expressions. |
| $or          | connect array elements with logical OR. Contains Array of expressions. |
| $not         | invert result of object. Contains single expression. |

| operators    | description                                |
|--------------|--------------------------------------------|
| $eq          | equal |
| $neq         | not equal |
| $lt          | lesser than |
| $gt          | greater than |
| $lte         | lesser than equal |
| $gte         | greater than equal |
| $in          | contained in array |
| $notinset    | not contained in array |
| $like        | equal using wildcards *? |
| $null        | is null value ("$null":1) |
| $notnull     | not is null value ("$notnull":1) |
| $notlike     | not equal using wildcards *? |
| $between     | two values in an array. Equal to a $gte $lt pair |
| $options     | string containing letters: `i` for case insensitive |
| $unit        | define the unit the condition value is given in. If 0 it assumed its the default unit. |

| aggregates   | description                                |
|--------------|--------------------------------------------|
| $none      | no aggregate |
| $count     | return int containing the number of rows |
| $dcount    | return int containing the number of distinct rows |
| $min       | returns minimal value of the attribute |
| $max       | returns maximal value of the attribute |
| $avg       | returns average value of the attribute |
| $stddev    | returns standard derivation value of the attribute |
| $sum       | returns sum of all attribute values |
| $distinct  | distinct attribute values |
| $point     | used for query on bulk data. returning indices of local column values |
| $ia        | Retrieve an instance attribute |
| $unit      | define the unit by its id that should be used for the return values |

| global options | description                                |
|----------------|--------------------------------------------|
| $rowlimit      | maximal number of rows to return |
| $rowskip       | number of rows to be skipped |
| $seqlimit      | maximal number of entries in a single sequence |
| $seqskip       | number of entries to be skipped in a single sequence |

## Remarks

* enum values in conditions can be given as string or number. Be aware that the string is case sensitive.
* longlong values can be given as string or number because not all longlong values can be
  represented as a number in json.
* outer joins are given by adding `:OUTER` to the end of a relation name before the next dot.
