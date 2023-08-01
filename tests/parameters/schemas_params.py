import random

from datetime import datetime, timedelta
from typing import Any, List, Tuple, Type, Union
from zoneinfo import ZoneInfo

from tests.parameters.common import INTEG, _random_positive_int, random_step, random_string

from deker import Scale
from deker.ABC.base_schemas import BaseDimensionSchema
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    TimeDimensionSchema,
    VArraySchema,
)
from deker.types.private.typings import NoneType, Numeric, NumericDtypes


def get_vgrids(dimensions: List[BaseDimensionSchema]):
    def make_variations(vgrid, index) -> List[list]:
        vgrds = []
        d = dimensions[index]
        if len(vgrid) == 1:
            return [[i] for i in range(1, d.size) if d.size % i == 0]

        for j in range(1, dimensions[index].size):
            if d.size % j == 0:
                for item in make_variations(vgrid[1:], index + 1):
                    vgrds.append([j, *item])
        return vgrds

    return make_variations([1] * len(dimensions), 0)


def attributes_schema_params(primary: bool, name: str = "") -> dict:
    """Get params for AttributeSchema.

    :param primary: flag for attribute type: primary or custom
    :param name: attribute name (for TimeDimensionSchema start_value reference)
    """
    if not name:
        name = random_string()
        dtype = random.choice(NumericDtypes)
    else:
        dtype = datetime
    return {"name": name, "dtype": dtype, "primary": primary}


def dimension_schema_random_params() -> dict:
    """Get random params for dimension schema."""
    randint = random.randint(1, INTEG)
    desc = random.choice(("labels", "scale"))
    if desc == "labels":
        labels: set = set()
        while len(labels) < randint:
            labels.add(random_string())
        res = random.choice((list(labels), None))
    else:
        start_value = random.uniform(-180.0, 90.0)
        step = random.random()
        name = random.choice((random_string(), None))
        res = random.choice((Scale(start_value, step, name), None))

    return {"name": random_string(), "size": randint, desc: res}


def time_dimension_schema_datetime_random_params() -> dict:
    """Get random params for time dimension schema."""

    tz = ["UTC", "Europe/Moscow", "Asia/Almaty", "America/Dominica"]
    return {
        "name": random_string(),
        "size": _random_positive_int(),
        "step": timedelta(random_step()),
        "start_value": datetime.now(tz=ZoneInfo(random.choice(tz))),
    }


class SchemaParams:
    @classmethod
    def _get_dims_dtype(cls) -> Tuple[List[DimensionSchema], Type[Numeric]]:
        dims = [
            DimensionSchema(**dimension_schema_random_params())
            for _ in range(_random_positive_int())
        ]
        dtype = random.choice(NumericDtypes)
        return dims, dtype

    @classmethod
    def _get_attrs(cls, primary: bool) -> List[AttributeSchema]:
        attrs = [
            AttributeSchema(**attributes_schema_params(primary))
            for _ in range(random.randint(1, 3))
        ]
        return attrs


class ArraySchemaParamsNoTime(SchemaParams):
    @classmethod
    def OK_params_no_vgrid_no_attrs(cls):
        dimensions, dtype = cls._get_dims_dtype()
        return ArraySchema(dimensions=dimensions, dtype=dtype)

    @classmethod
    def OK_params_no_vgrid_primary_attributes(cls):
        dimensions, dtype = cls._get_dims_dtype()
        attrs = [
            AttributeSchema(**attributes_schema_params(True)) for _ in range(random.randint(1, 3))
        ]
        return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attrs)

    @classmethod
    def OK_params_no_vgrid_custom_attributes(cls):
        dimensions, dtype = cls._get_dims_dtype()
        attrs = [
            AttributeSchema(**attributes_schema_params(False)) for _ in range(random.randint(1, 3))
        ]
        return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attrs)

    @classmethod
    def OK_params_no_vgrid_all_attrs(cls):
        dimensions, dtype = cls._get_dims_dtype()
        attrs = [*cls._get_attrs(True), *cls._get_attrs(False)]
        return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attrs)

    @classmethod
    def OK_params_vgrid_primary_attributes(cls):
        dimensions, dtype = cls._get_dims_dtype()
        attrs = [
            AttributeSchema(dtype=str, name="vid", primary=True),
            AttributeSchema(dtype=str, name="v_position", primary=True),
            *[
                AttributeSchema(**attributes_schema_params(True))
                for _ in range(random.randint(1, 3))
            ],
        ]
        return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attrs)

    @classmethod
    def OK_params_vgrid_all_attrs(cls):
        dimensions, dtype = cls._get_dims_dtype()
        attrs = [
            AttributeSchema(dtype=str, name="vid", primary=True),
            AttributeSchema(dtype=str, name="v_position", primary=True),
            *cls._get_attrs(False),
            *cls._get_attrs(True),
        ]
        return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attrs)

    @classmethod
    def WRONG_params(cls) -> tuple:
        return (
            0,
            1,
            -1,
            "str",
            "",
            " ",
            "       ",
            datetime.now(),
            -4.0,
            4.0,
            complex(-0.0000000000001),
            complex(0.0000000000001),
            [],
            tuple(),
            set(),
            dict(),
            [1],
            (2, 3),
            {2, 3},
            dict(a=2),
        )


class VArraySchemaParams:
    class OK(ArraySchemaParamsNoTime):
        @classmethod
        def no_attrs(cls):
            dimensions, dtype = cls._get_dims_dtype()
            return VArraySchema(dimensions=dimensions, dtype=dtype, vgrid=[1] * len(dimensions))

        @classmethod
        def primary_attributes(cls):
            dimensions, dtype = cls._get_dims_dtype()
            attrs = [
                AttributeSchema(**attributes_schema_params(True))
                for _ in range(random.randint(1, 3))
            ]
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attrs, vgrid=[1] * len(dimensions)
            )

        @classmethod
        def custom_attributes(cls):
            dimensions, dtype = cls._get_dims_dtype()
            attrs = [
                AttributeSchema(**attributes_schema_params(False))
                for _ in range(random.randint(1, 3))
            ]
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attrs, vgrid=[1] * len(dimensions)
            )

        @classmethod
        def no_vgrid_all_attrs(cls):
            dimensions, dtype = cls._get_dims_dtype()
            attrs = [*cls._get_attrs(True), *cls._get_attrs(False)]
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attrs, vgrid=[1] * len(dimensions)
            )

        @classmethod
        def vgrid_primary_attributes(cls):
            dimensions, dtype = cls._get_dims_dtype()
            attrs = [
                AttributeSchema(dtype=str, name="vid", primary=True),
                AttributeSchema(dtype=str, name="v_position", primary=True),
                *[
                    AttributeSchema(**attributes_schema_params(True))
                    for _ in range(random.randint(1, 3))
                ],
            ]
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attrs, vgrid=[1] * len(dimensions)
            )

        @classmethod
        def vgrid_all_attrs(cls):
            dimensions, dtype = cls._get_dims_dtype()
            attrs = [
                AttributeSchema(dtype=str, name="vid", primary=True),
                AttributeSchema(dtype=str, name="v_position", primary=True),
                *cls._get_attrs(False),
                *cls._get_attrs(True),
            ]
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attrs, vgrid=[1] * len(dimensions)
            )


class TimedSchemaParams:
    """Parameters for any schema that contains time dimension"""

    class OK(ArraySchemaParamsNoTime):
        @classmethod
        def _get_dims_dtype(cls) -> tuple:
            dims, dtype = super()._get_dims_dtype()
            time = TimeDimensionSchema(**time_dimension_schema_datetime_random_params())
            dims.insert(random.randint(0, len(dims)), time)  # type: ignore
            return dims, dtype

        @classmethod
        def _insert_time_attribute(
            cls, attrs: List[AttributeSchema], dimensions: List[DimensionSchema]
        ) -> None:
            for d in dimensions:
                if isinstance(d, TimeDimensionSchema):
                    if isinstance(d.start_value, str):
                        primary = random.choice([True, False])
                        time_attr = AttributeSchema(
                            **attributes_schema_params(primary=primary, name=d.name)
                        )
                        attrs.insert(random.randint(0, len(attrs)), time_attr)
                        break

        @classmethod
        def _with_attributes(
            cls, make_primary: bool
        ) -> Tuple[List[DimensionSchema], Type[Numeric], List[AttributeSchema]]:
            dimensions, dtype = cls._get_dims_dtype()
            attrs = cls._get_attrs(make_primary)
            cls._insert_time_attribute(attrs, dimensions)
            return dimensions, dtype, attrs

        @classmethod
        def primary_attributes(
            cls,
        ) -> Tuple[List[DimensionSchema], Type[Numeric], List[AttributeSchema]]:
            return cls._with_attributes(True)

        @classmethod
        def custom_attributes(
            cls,
        ) -> Tuple[List[DimensionSchema], Type[Numeric], List[AttributeSchema]]:
            return cls._with_attributes(False)

        @classmethod
        def all_attrs(cls) -> Tuple[List[DimensionSchema], Type[Numeric], List[AttributeSchema]]:
            dimensions, dtype = cls._get_dims_dtype()
            attrs = [*cls._get_attrs(True), *cls._get_attrs(False)]
            cls._insert_time_attribute(attrs, dimensions)
            return dimensions, dtype, attrs

        @classmethod
        def vgrid_primary_attributes(
            cls,
        ) -> Tuple[List[DimensionSchema], Type[Numeric], List[AttributeSchema]]:
            dimensions, dtype = cls._get_dims_dtype()
            attrs = [
                AttributeSchema(dtype=str, name="vid", primary=True),
                AttributeSchema(dtype=str, name="v_position", primary=True),
                *cls._get_attrs(True),
            ]
            cls._insert_time_attribute(attrs, dimensions)
            return dimensions, dtype, attrs

        @classmethod
        def vgrid_all_attrs(
            cls,
        ) -> Tuple[List[DimensionSchema], Type[Numeric], List[AttributeSchema]]:
            dimensions, dtype = cls._get_dims_dtype()
            attrs = [
                AttributeSchema(dtype=str, name="vid", primary=True),
                AttributeSchema(dtype=str, name="v_position", primary=True),
                *cls._get_attrs(False),
            ]
            cls._insert_time_attribute(attrs, dimensions)
            return dimensions, dtype, attrs


class TimedArraySchemaParams:
    class OK(TimedSchemaParams.OK):
        @classmethod
        def no_attrs(cls) -> ArraySchema:
            dims, dtype = cls._get_dims_dtype()
            return ArraySchema(dimensions=dims, dtype=dtype)

        @classmethod
        def primary_attributes(cls) -> ArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.primary_attributes()
            return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attributes)  # type: ignore[arg-type]

        @classmethod
        def custom_attributes(cls) -> ArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.custom_attributes()
            return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attributes)  # type: ignore[arg-type]

        @classmethod
        def all_attrs(cls) -> ArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.all_attrs()
            return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attributes)  # type: ignore[arg-type]

        @classmethod
        def vgrid_primary_attributes(cls) -> ArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.vgrid_primary_attributes()
            return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attributes)  # type: ignore[arg-type]

        @classmethod
        def vgrid_all_attrs(cls) -> ArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.vgrid_all_attrs()
            return ArraySchema(dimensions=dimensions, dtype=dtype, attributes=attributes)  # type: ignore[arg-type]


class TimedVArraySchemaParams:
    """Schema for time dimension with array."""

    class OK(TimedSchemaParams.OK):
        @classmethod
        def no_attrs(cls, vgrid=None) -> VArraySchema:
            dims, dtype = cls._get_dims_dtype()
            vgrd = vgrid or [1] * len(dims)
            return VArraySchema(dimensions=dims, dtype=dtype, vgrid=vgrd)

        @classmethod
        def primary_attributes(cls, vgrid=None) -> VArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.primary_attributes()
            vgrd = vgrid or [1] * len(dimensions)
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attributes, vgrid=vgrd  # type: ignore[arg-type]
            )

        @classmethod
        def custom_attributes(cls, vgrid=None) -> VArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.custom_attributes()
            vgrd = vgrid or [1] * len(dimensions)
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attributes, vgrid=vgrd  # type: ignore[arg-type]
            )

        @classmethod
        def all_attrs(cls, vgrid=None) -> VArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.all_attrs()
            vgrd = vgrid or [1] * len(dimensions)
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attributes, vgrid=vgrd  # type: ignore[arg-type]
            )

        @classmethod
        def vgrid_primary_attributes(cls, vgrid=None) -> VArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.vgrid_primary_attributes()
            vgrd = vgrid or [1] * len(dimensions)
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attributes, vgrid=vgrd  # type: ignore[arg-type]
            )

        @classmethod
        def vgrid_all_attrs(cls, vgrid=None) -> VArraySchema:
            dimensions, dtype, attributes = TimedSchemaParams.OK.vgrid_all_attrs()
            vgrd = vgrid or [1] * len(dimensions)
            return VArraySchema(
                dimensions=dimensions, dtype=dtype, attributes=attributes, vgrid=vgrd  # type: ignore[arg-type]
            )

        @classmethod
        def __make_schema_with_attributes(
            cls,
            start_value: Union[str, datetime],
            primary: bool = None,
            vgrid=None,
            create_as_list=False,
        ):
            dims, dtype = ArraySchemaParamsNoTime._get_dims_dtype()
            time_dimension = TimeDimensionSchema(
                name="dt", size=3, start_value=start_value, step=timedelta(days=10)
            )
            dims.append(time_dimension)  # type: ignore[arg-type]
            attrs = []
            if primary is not None:
                attrs += cls._get_attrs(primary)
            vgrd = vgrid or [1] * len(dims)
            cls._insert_time_attribute(attrs, dims)
            if create_as_list:
                return [
                    VArraySchema(dtype=dtype, dimensions=dims, attributes=attrs, vgrid=grid)  # type: ignore[arg-type]
                    for grid in get_vgrids(dims)  # type: ignore[arg-type]
                ]
            return VArraySchema(dtype=dtype, dimensions=dims, attributes=attrs, vgrid=vgrd)  # type: ignore[arg-type]

        @classmethod
        def start_value_string_no_extra_attrs(cls, create_as_list: bool = False) -> VArraySchema:
            """Varray schema with start  value as string, and without any extra arguments
            (Only vid, vpos and step label attribute)

            :return:
            """
            return cls.__make_schema_with_attributes(
                start_value="$dt", primary=None, create_as_list=create_as_list  # type: ignore[arg-type]
            )

        @classmethod
        def start_value_string_primary_attrs(cls, create_as_list: bool = False) -> VArraySchema:
            """Varray schema with start  value as string, and without any extra arguments
            (Only vid, vpos and step label attribute)

            :return:
            """
            return cls.__make_schema_with_attributes(
                start_value="$dt", primary=True, create_as_list=create_as_list
            )

        @classmethod
        def start_value_string_custom_attrs(cls, create_as_list: bool = False) -> VArraySchema:
            """Varray schema with start  value as string, and without any extra arguments
            (Only vid, vpos and step label attribute)

            :return:
            """
            return cls.__make_schema_with_attributes(
                start_value="$dt", primary=False, create_as_list=create_as_list
            )

        @classmethod
        def start_value_datetime_no_extra_attrs(cls, create_as_list: bool = False) -> VArraySchema:
            return cls.__make_schema_with_attributes(
                start_value=datetime.now(ZoneInfo("UTC")),
                primary=None,  # type: ignore[arg-type]
                create_as_list=create_as_list,
            )

        @classmethod
        def start_value_datetime_primary_attributes(
            cls, create_as_list: bool = False
        ) -> VArraySchema:
            return cls.__make_schema_with_attributes(
                start_value=datetime.now(ZoneInfo("UTC")),
                primary=True,
                create_as_list=create_as_list,
            )

        @classmethod
        def start_value_datetime_custom_attributes(
            cls, create_as_list: bool = False
        ) -> VArraySchema:
            return cls.__make_schema_with_attributes(
                start_value=datetime.now(ZoneInfo("UTC")),
                primary=False,
                create_as_list=create_as_list,
            )


class TypedSchemaParams:
    """Provides functionality to generate list with different types"""

    @classmethod
    def _generate_types(
        cls, base_dict: dict, key: str, *, exception_types: List[type] = None
    ) -> List[dict]:
        """Generates different types.

        :param base_dict: Dict with fixed values
        :param key: key for different types
        :param exception_types: if we need to skip some type
        """
        result = []
        for item in [
            "",
            1,
            1.2,
            None,
            True,
            object,
            object(),
            [],
            set(),
            dict(),
            tuple(),
            datetime.now(),
            Scale(0.1, 0.2),
        ]:
            if exception_types and type(item) in exception_types:
                continue
            result.append({**base_dict.copy(), key: item})
            if key == "scale":
                a = 1
        return result


class ArraySchemaCreationParams(SchemaParams, TypedSchemaParams):
    """Params for creation an array schema."""

    @classmethod
    def WRONG_params_dataclass_raises(cls) -> List[Any]:
        """Returns wrong params for array_schema."""
        dimensions, dtype = cls._get_dims_dtype()

        return [
            # wrong dtype
            *cls._generate_types(base_dict={"dimensions": dimensions}, key="dtype"),
            # wrong dimensions
            *cls._generate_types(base_dict={"dtype": dtype}, key="dimensions"),
            # wrong attributes
            *cls._generate_types(
                base_dict={"dtype": dtype, "dimensions": dimensions},
                key="attributes",
                exception_types=[tuple, list],
            ),
        ]


class VArraySchemaCreationParams(SchemaParams, TypedSchemaParams):
    """Params for creation a varray schema."""

    @classmethod
    def WRONG_params_dataclass_raises(cls) -> List[Any]:
        """Returns wrong params for array_schema."""
        dimensions, dtype = cls._get_dims_dtype()
        vgrid = (1, 1, 1)

        return [
            # wrong dtype
            *cls._generate_types(base_dict={"dimensions": dimensions, "vgrid": vgrid}, key="dtype"),
            # wrong dimensions
            *cls._generate_types(base_dict={"dtype": dtype, "vgrid": vgrid}, key="dimensions"),
            # wrong attributes
            *cls._generate_types(
                base_dict={"dtype": dtype, "dimensions": dimensions, "vgrid": vgrid},
                key="attributes",
                exception_types=[tuple, list],
            ),
            # wrong vgrid
            *cls._generate_types(
                base_dict={"dtype": dtype, "dimensions": dimensions},
                key="vgrid",
                exception_types=[tuple, list],
            ),
        ]


class DimensionSchemaCreationParams(TypedSchemaParams):
    """Params for creation dimension schema."""

    @classmethod
    def WRONG_params_dataclass_raises(cls) -> List[Any]:
        """Returns wrong params for dimension schema."""

        return [
            # wrong size
            *cls._generate_types(
                base_dict={"name": "name", "labels": [], "scale": None},
                key="size",
                exception_types=[int],
            ),
            # wrong name
            *cls._generate_types(base_dict={"labels": [], "size": 2, "scale": None}, key="name"),
            # wrong labels
            *cls._generate_types(
                base_dict={"name": "name", "size": 2, "scale": None},
                key="labels",
                exception_types=[list, tuple, NoneType],
            ),
            # wrong scale
            *cls._generate_types(
                base_dict={"name": "name", "size": 2, "labels": None},
                key="scale",
                exception_types=[Scale, dict, NoneType],
            ),
        ]


class TimeDimensionSchemaCreationParams(TypedSchemaParams):
    """Params for creation dimension schema."""

    @classmethod
    def WRONG_params_dataclass_raises(cls) -> List[Any]:
        """Returns wrong params for dimension schema.

        start_value: datetime | str
        step_label: str
        step: int | float = 1
        """

        return [
            # wrong size
            *cls._generate_types(
                base_dict={
                    "name": random_string(),
                    "start_value": "$foo",
                    "step": timedelta(random_step()),
                },
                key="size",
                exception_types=[int],
            ),
            # wrong name
            *cls._generate_types(
                base_dict={
                    "size": _random_positive_int(),
                    "start_value": "$foo",
                    "step": timedelta(random_step()),
                },
                key="name",
            ),
            # wrong start_value type
            *cls._generate_types(
                base_dict={
                    "name": random_string(),
                    "size": _random_positive_int(),
                    "step": timedelta(random_step()),
                },
                key="start_value",
                exception_types=[datetime],
            ),
            # wrong start_value string
            {
                "name": random_string(),
                "start_value": random_string(),
                "size": _random_positive_int(),
                "step": timedelta(hours=1),
            },
            # wrong step
            *cls._generate_types(
                base_dict={
                    "name": random_string(),
                    "start_value": "$foo",
                    "size": _random_positive_int(),
                },
                key="step",
                exception_types=[timedelta, dict],
            ),
            # wrong_step_params
            {
                "name": random_string(),
                "start_value": "$foo",
                "size": _random_positive_int(),
                "step": {random_string(): random_string()},
            },
        ]
