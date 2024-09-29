import abc
import os


class AbstractSupabaseClient:
    """An abstract class for supabase clients."""

    @property
    def supabase_service_client(self):
        raise NotImplementedError("supabase_service_client property must be implemented.")

    @property
    @abc.abstractmethod
    def datatypes_file_path_from_root(self):
        raise NotImplementedError("type_file_path_from_root property must be implemented.")

    @abc.abstractmethod
    def create_supabase_service_client(self):
        raise NotImplementedError("create_supabase_service_client method must be implemented.")

    def get_all_tables(self):
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

    def get_table_data_structure(self, table_name: str):
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
                'is_nullable', c.is_nullable  -- Include the nullable information
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
          public.get_table_data_structure ('api_keys');
        """
        return self.supabase_service_client.rpc("get_table_data_structure",
                                                {"selected_table_name": table_name}).execute().data

    def update_table_types(self):
        """
        Calls the rpc method 'update_table_types'.

        This method must first be implemented in supabase.
        """
        tables = self.get_all_tables()
        tables_list = [table["table_name"] for table in tables]
        type_definitions = {}

        for table_name in tables_list:
            column_definitions = self.get_table_data_structure(table_name)

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
                is_optional = column_definition["is_nullable"]
                if type_name == "integer":
                    type_definition += f"    {column_name}: int\n"
                elif type_name == "text":
                    type_definition += f"    {column_name}: str\n"
                elif type_name == "timestamp with time zone":
                    type_definition += f"    {column_name}: str\n"
                elif type_name == "numeric":
                    type_definition += f"    {column_name}: float\n"
                elif type_name == "boolean":
                    type_definition += f"    {column_name}: bool\n"
                elif type_name == "jsonb":
                    type_definition += f"    {column_name}: dict\n"
                elif type_name == "bigint":
                    type_definition += f"    {column_name}: int\n"
                elif type_name == "uuid":
                    type_definition += f"    {column_name}: str\n"
                else:
                    raise ValueError(f"Unknown type: {type_name}")

                if is_optional == "YES":
                    type_definition = type_definition[:-1] + f" | None\n"

            type_definitions[table_name] = type_definition

        root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        full_file_path = os.path.join(root_path, self.datatypes_file_path_from_root)

        with open(full_file_path, "w") as f:
            for table_name, type_definition in type_definitions.items():
                f.write(f"{type_definition}\n\n")
