import abc
import os
import types

from classes.supabase import supabase_types_to_python_types
from classes.utilities.Utilities import Utilities


class AbstractSupabaseClient:
    """An abstract class for supabase clients."""

    @property
    def supabase_service_client(self):
        raise NotImplementedError("supabase_service_client property must be implemented.")

    @abc.abstractmethod
    def create_supabase_service_client(self):
        raise NotImplementedError("create_supabase_service_client method must be implemented.")

    @property
    @abc.abstractmethod
    def datatypes_file_path_from_root(self):
        """
        The path to the file that contains the data types for the supabase database.
        """
        raise NotImplementedError("type_file_path_from_root property must be implemented.")

    @property
    @abc.abstractmethod
    def linked_table_column_types(self):
        """
        The linked_table_column_types retrieved from the supabase data types file.

        Should always be called LinkedTableColumnTypes. The import statements dictates which database applies.
        """
        raise NotImplementedError("linked_table_column_types property must be implemented.")

    def insert_row(self, table_name: str, row_data: dict):
        """
        Inserts a row into the table. Validates the row data against the table structure.
        """
        self._validate_insertion_row_data(table_name, row_data)
        json_serialized_row_data = Utilities.custom_json_serializer(row_data)
        return self.supabase_service_client.table(table_name).insert(json_serialized_row_data).execute()

    def _validate_insertion_row_data(self, table_name: str, row_data: dict):
        """
        Validates the row data against the table structure.
        """
        try:
            table_data_structure = self.linked_table_column_types.linked_table_dict[table_name]
        except KeyError:
            raise KeyError(f"Table name '{table_name}' not found.")

        self._validate_column_presence_for_insertion(table_data_structure, row_data)
        self._validate_types(table_data_structure, row_data)

    @staticmethod
    def _validate_column_presence_for_insertion(table_data_structure: any, row_data: dict):
        """
        Validates that the row data has all the columns required for insertion.
        """
        # validate all necessary columns are present
        column_names = [column_name for column_name in table_data_structure.__annotations__.keys()]
        optional_columns = [column_name for column_name in column_names if
                            type(table_data_structure.__annotations__[column_name]) == types.UnionType
                            and Utilities.has_type(table_data_structure.__annotations__[column_name], type(None))]

        set_of_mandatory_column_names = set(column_names)
        set_of_optional_columns = set(optional_columns)

        set_of_mandatory_column_names = set_of_mandatory_column_names - set_of_optional_columns

        for column_name in row_data.keys():
            if column_name in set_of_mandatory_column_names:
                set_of_mandatory_column_names.remove(column_name)
                continue
            if column_name in set_of_optional_columns:
                continue
            raise KeyError(f"Column name '{column_name}' not found in table data structure.")

        if len(set_of_mandatory_column_names) > 0:
            raise KeyError(f"Column names {set_of_mandatory_column_names} are mandatory. "
                           f"Missing {row_data.keys()} in row data.")

    @staticmethod
    def _validate_types(table_data_structure: any, row_data: dict):
        """
        Validates that the row data has the correct types.
        """
        for column_name in row_data.keys():
            # Check against union types
            if type(table_data_structure.__annotations__[column_name]) == types.UnionType:
                union_type = table_data_structure.__annotations__[column_name]
                if not Utilities.has_type(union_type, type(row_data[column_name])):
                    raise ValueError(f"Column '{column_name}' is not of type {union_type}.")

            # Check against array types
            elif type(table_data_structure.__annotations__[column_name]) == type(list):
                list_type = table_data_structure.__annotations__[column_name]
                if not isinstance(row_data[column_name], list):
                    raise ValueError(f"Column '{column_name}' is not of type list.")
                for item in row_data[column_name]:
                    if not isinstance(item, list_type.__args__[0]):
                        raise ValueError(f"Column '{column_name}' is not of type {list_type}.")

            # Check against single types
            elif not isinstance(row_data[column_name], table_data_structure.__annotations__[column_name]):
                raise ValueError(
                    f"Column '{column_name}' is not of type {table_data_structure.__annotations__[column_name]}.")

    def get_all_tables_from_supabase(self):
        """
        Calls the rpc method 'get_all_tables'.

        This method must first be implemented in supabase.
        Run this code in the SQL editor to implement in a new database.

        create
        or replace function public.get_all_tables () returns table (table_name text) as $$
        BEGIN
            RETURN QUERY
            SELECT t.table_name::text
            FROM information_schema.tables AS t
            WHERE t.table_schema = 'public';
        END;
        $$ language plpgsql;

        -- Test the function
        select
          *
        from
          public.get_all_tables ();
        """
        return self.supabase_service_client.rpc("get_all_tables").execute().data

    def get_table_data_structure_from_supabase(self, table_name: str):
        """
        Calls the rpc method 'get_table_data_structure'.

        This method must first be implemented in supabase.
        Run this code in the SQL editor to implement in a new database.

        CREATE
        OR REPLACE FUNCTION public.get_table_data_structure (selected_table_name TEXT) RETURNS jsonb AS $$
        DECLARE
            result jsonb := '[]'::jsonb;  -- Initialize an empty JSON array
        BEGIN
            SELECT jsonb_agg(jsonb_build_object(
                'column_name', c.column_name,
                'data_type', c.data_type,
                'is_primary_key', EXISTS (
                    SELECT
                      1
                    FROM
                      information_schema.table_constraints tc
                      JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                    WHERE
                      tc.table_schema = 'public'
                      AND tc.table_name = c.table_name
                      AND kcu.column_name = c.column_name
                      AND tc.constraint_type = 'PRIMARY KEY'
                ),
                'is_nullable', c.is_nullable = 'YES',
                'is_identity', c.is_identity = 'YES',
                'has_default_value', c.column_default IS NOT NULL,
                'array_subtype', CASE
                    WHEN c.data_type = 'ARRAY' THEN
                        c.udt_name -- Extract the subtype
                    ELSE
                        NULL
                END
            ))
            INTO result
            FROM information_schema.columns c
            WHERE c.table_schema = 'public'
            AND c.table_name = selected_table_name;  -- Use the input parameter

            RETURN result;
        END;
        $$ LANGUAGE plpgsql;

        -- Test the function:
        SELECT
          *
        FROM
          public.get_table_data_structure ('backtests');
        """
        return self.supabase_service_client.rpc("get_table_data_structure",
                                                {"selected_table_name": table_name}).execute().data

    def update_table_types(self):
        """
        Calls the rpc method 'update_table_types'.

        This method must first be implemented in supabase.
        """
        tables = self.get_all_tables_from_supabase()
        tables_list = [table["table_name"] for table in tables]
        import_list_type = False
        type_definitions = {}

        # Make an initial class that links the table name to the column type identifiers
        linked_table_column_types = f"class LinkedTableColumnTypes:\n    linked_table_dict = {{\n"

        for table_name in tables_list:
            column_definitions = self.get_table_data_structure_from_supabase(table_name)

            class_name = table_name.replace("_", " ")
            class_name = class_name.title()
            class_name = class_name.replace(" ", "")
            if class_name[-1] == "s":
                class_name = class_name[:-1]
            class_name += "ColumnTypes"

            type_definition = f"class {class_name}:\n"
            for column_definition in column_definitions:

                column_name = column_definition["column_name"]
                type_name = column_definition["data_type"]
                is_primary_key = column_definition["is_primary_key"]
                is_nullable = column_definition["is_nullable"]
                is_identity = column_definition["is_identity"]
                has_default_value = column_definition["has_default_value"]

                try:
                    stringified_python_type = supabase_types_to_python_types[type_name]
                except KeyError:
                    raise KeyError(f"Type {type_name} not found in supabase_types_to_python_types.")

                if type_name != "ARRAY":
                    type_definition += f"    {column_name}: {stringified_python_type}\n"
                else:
                    array_subtype = column_definition["array_subtype"].replace("_", "").replace(" ", "")
                    import_list_type = True
                    stringified_python_type = supabase_types_to_python_types[array_subtype]
                    type_definition += f"    {column_name}: List[{stringified_python_type}]\n"

                # Set the type to None if needed
                if not is_primary_key and (is_nullable or has_default_value or is_identity):
                    type_definition = type_definition[:-1] + f" | None\n"

            type_definitions[table_name] = type_definition

            linked_table_column_types += f'        "{table_name}": {class_name},\n'

        # close linked_table_dict
        linked_table_column_types += "    }\n"

        root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        full_file_path = os.path.join(root_path, self.datatypes_file_path_from_root)

        with open(full_file_path, "w") as f:
            if import_list_type:
                f.write("from typing import List\n\n\n")

            for table_name, type_definition in type_definitions.items():
                f.write(f"{type_definition}\n\n")

            f.write(f"{linked_table_column_types}")
