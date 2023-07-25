from tests.parameters.common import random_string
from tests.parameters.schemas_params import (
    ArraySchemaParamsNoTime,
    TimedArraySchemaParams,
    TimedVArraySchemaParams,
    VArraySchemaParams,
)


class ClientParams:
    class ArraySchema:
        class OK:
            @classmethod
            def no_vgrid_no_attrs(cls):
                return {
                    "name": random_string(),
                    "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_no_attrs(),
                }

            @classmethod
            def no_vgrid_primary_attributes(cls):
                return {
                    "name": random_string(),
                    "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_primary_attributes(),
                }

            @classmethod
            def no_vgrid_custom_attributes(cls):
                return {
                    "name": random_string(),
                    "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_custom_attributes(),
                }

            @classmethod
            def no_vgrid_all_attrs(cls):
                return {
                    "name": random_string(),
                    "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_all_attrs(),
                }

            @classmethod
            def vgrid_all_attrs(cls):
                return {
                    "name": random_string(),
                    "schema": ArraySchemaParamsNoTime.OK_params_vgrid_all_attrs(),
                }

            @classmethod
            def vgrid_primary_attributes(cls):
                return {
                    "name": random_string(),
                    "schema": ArraySchemaParamsNoTime.OK_params_vgrid_primary_attributes(),
                }

            @classmethod
            def time_params_no_vgrid_no_attrs(cls):
                return {
                    "name": random_string(),
                    "schema": TimedArraySchemaParams.OK.no_attrs(),
                }

            @classmethod
            def time_params_no_vgrid_primary_attributes(cls):
                return {
                    "name": random_string(),
                    "schema": TimedArraySchemaParams.OK.primary_attributes(),
                }

            @classmethod
            def time_params_no_vgrid_custom_attributes(cls):
                return {
                    "name": random_string(),
                    "schema": TimedArraySchemaParams.OK.custom_attributes(),
                }

            @classmethod
            def time_params_no_vgrid_all_attrs(cls):
                return {
                    "name": random_string(),
                    "schema": TimedArraySchemaParams.OK.all_attrs(),
                }

            @classmethod
            def time_params_vgrid_all_attrs(cls):
                return {
                    "name": random_string(),
                    "schema": TimedArraySchemaParams.OK.vgrid_all_attrs(),
                }

            @classmethod
            def time_params_vgrid_primary_attributes(cls):
                return {
                    "name": random_string(),
                    "schema": TimedArraySchemaParams.OK.vgrid_primary_attributes(),
                }

            @classmethod
            def multi_array_schemas(cls):
                array_schemas = [
                    ArraySchemaParamsNoTime.OK_params_no_vgrid_no_attrs(),
                    ArraySchemaParamsNoTime.OK_params_no_vgrid_primary_attributes(),
                    ArraySchemaParamsNoTime.OK_params_no_vgrid_custom_attributes(),
                    ArraySchemaParamsNoTime.OK_params_no_vgrid_all_attrs(),
                    ArraySchemaParamsNoTime.OK_params_vgrid_primary_attributes(),
                    ArraySchemaParamsNoTime.OK_params_vgrid_all_attrs(),
                    TimedArraySchemaParams.OK.no_attrs(),
                    TimedArraySchemaParams.OK.primary_attributes(),
                    TimedArraySchemaParams.OK.custom_attributes(),
                    TimedArraySchemaParams.OK.all_attrs(),
                    TimedArraySchemaParams.OK.vgrid_primary_attributes(),
                    TimedArraySchemaParams.OK.vgrid_all_attrs(),
                ]
                return [{"name": random_string(), "schema": schema} for schema in array_schemas]

    class VArraySchema:
        class OK:
            @classmethod
            def OK_params_with_varray_schema(cls):
                return {
                    "name": random_string(),
                    "schema": TimedVArraySchemaParams.OK.no_attrs(),
                }

            @classmethod
            def OK_params_multi_varray_schemas(cls):
                varray_schema = [
                    TimedVArraySchemaParams.OK.no_attrs(),
                    TimedVArraySchemaParams.OK.primary_attributes(),
                    TimedVArraySchemaParams.OK.custom_attributes(),
                    TimedVArraySchemaParams.OK.all_attrs(),
                    TimedVArraySchemaParams.OK.vgrid_all_attrs(),
                    TimedVArraySchemaParams.OK.vgrid_primary_attributes(),
                    TimedVArraySchemaParams.OK.start_value_datetime_primary_attributes(),
                    TimedVArraySchemaParams.OK.start_value_datetime_no_extra_attrs(),
                    TimedVArraySchemaParams.OK.start_value_datetime_custom_attributes(),
                    TimedVArraySchemaParams.OK.start_value_string_custom_attrs(),
                    TimedVArraySchemaParams.OK.start_value_datetime_primary_attributes(),
                    TimedVArraySchemaParams.OK.start_value_string_no_extra_attrs(),
                    VArraySchemaParams.OK.no_attrs(),
                    VArraySchemaParams.OK.primary_attributes(),
                    VArraySchemaParams.OK.custom_attributes(),
                    VArraySchemaParams.OK.no_vgrid_all_attrs(),
                    VArraySchemaParams.OK.vgrid_all_attrs(),
                    VArraySchemaParams.OK.vgrid_primary_attributes(),
                ]
                return [
                    {
                        "name": random_string(),
                        "schema": schema,
                    }
                    for schema in varray_schema
                ]


class CollectionParams:
    @classmethod
    def OK_params_no_vgrid_no_attrs(cls):
        return {
            "name": random_string(),
            "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_no_attrs(),
        }

    @classmethod
    def OK_params_no_vgrid_primary_attributes(cls):
        return {
            "name": random_string(),
            "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_primary_attributes(),
        }

    @classmethod
    def OK_params_no_vgrid_custom_attributes(cls):
        return {
            "name": random_string(),
            "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_custom_attributes(),
        }

    @classmethod
    def OK_params_no_vgrid_all_attrs(cls):
        return {
            "name": random_string(),
            "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_all_attrs(),
        }

    def OK_params_vgrid_all_attrs(cls):
        return {
            "name": random_string(),
            "schema": ArraySchemaParamsNoTime.OK_params_vgrid_all_attrs(),
        }

    def OK_params_vgrid_primary_attributes(cls):
        return {
            "name": random_string(),
            "schema": ArraySchemaParamsNoTime.OK_params_vgrid_primary_attributes(),
        }

    @classmethod
    def OK_time_params_no_vgrid_no_attrs(cls):
        return {
            "name": random_string(),
            "schema": TimedArraySchemaParams.OK.OK_params_no_vgrid_no_attrs(),
        }

    @classmethod
    def OK_time_params_no_vgrid_primary_attributes(cls):
        return {
            "name": random_string(),
            "schema": TimedArraySchemaParams.OK.OK_params_no_vgrid_primary_attributes(),
        }

    @classmethod
    def OK_time_params_no_vgrid_custom_attributes(cls):
        return {
            "name": random_string(),
            "schema": TimedArraySchemaParams.OK.OK_params_no_vgrid_custom_attributes(),
        }

    @classmethod
    def OK_time_params_no_vgrid_all_attrs(cls):
        return {
            "name": random_string(),
            "schema": TimedArraySchemaParams.OK.OK_params_no_vgrid_all_attrs(),
        }

    def OK_time_params_vgrid_all_attrs(cls):
        return {
            "name": random_string(),
            "schema": TimedArraySchemaParams.OK.OK_params_vgrid_all_attrs(),
        }

    def OK_time_params_vgrid_primary_attributes(cls):
        return {
            "name": random_string(),
            "schema": TimedArraySchemaParams.OK.OK_params_vgrid_primary_attributes(),
        }

    @classmethod
    def OK_params_for_collection_no_varray(cls):
        return {
            "name": random_string(),
            "schema": ArraySchemaParamsNoTime.OK_params_no_vgrid_primary_attributes(),
        }

    @classmethod
    def OK_params_for_collection_varray(cls):
        return {
            "name": random_string(),
            "schema": TimedVArraySchemaParams.OK.no_attrs(),
        }
