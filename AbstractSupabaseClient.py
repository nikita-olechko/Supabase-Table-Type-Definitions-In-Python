import abc
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from types import GenericAlias, UnionType

import httpcore
import httpx
from dotenv import load_dotenv
from postgrest import APIError, APIResponse
from supabase import create_client, Client, ClientOptions

from AWS.server_utils.GrafanaLokiLogger import GrafanaLokiLogger, LogLevel
from classes.config.Config import Config
from classes.supabase import supabase_types_to_python_types, DEFAULT_ADVISORY_LOCK_TIMEOUT, \
    DEFAULT_POSTGREST_CLIENT_TIMEOUT
from classes.types.AvoError import FailedToAcquireAdvisoryLockError
from classes.utilities.Utilities import Utilities, ConsistentHashOutput


class RequestLimiter:
    """Limits the maximum number of requests that can be made at a time (max outbound streams is 100)."""

    def __init__(self, max_requests=99):
        self._semaphore = threading.Semaphore(max_requests)
        self._lock = threading.Lock()

    def __enter__(self):
        self._semaphore.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.release()


@dataclass(slots=True, frozen=True, unsafe_hash=True)
class LockSessionManager:
    """Internal lock session manager."""
    owner: str
    acquired_at: str
    expires_at: str | None


class AdvisoryLockManager:
    """
    Manages PostgreSQL advisory locks for rows in Supabase.

    Must run

    CREATE TABLE IF NOT EXISTS public.advisory_locks (
        lock_key TEXT PRIMARY KEY,
        owner TEXT NOT NULL,
        acquired_at INT8 NOT NULL,
        expires_at INT8 NOT NULL
    );

    For this to be functional.
    """

    def __init__(self, supabase_client):
        """Initialize with a reference to the Supabase client."""
        self.supabase_client = supabase_client
        self.active_locks: set[str] = set()

    @staticmethod
    def generate_lock_key(table_name: str, row_id_or_keys: int | str | tuple | list | dict) -> str:
        """Generate a consistent string key for a lock."""
        if isinstance(row_id_or_keys, (int, str, float)):
            return f"{table_name}:{row_id_or_keys}"
        elif isinstance(row_id_or_keys, (list, tuple)):
            return f"{table_name}:{'-'.join(str(k) for k in row_id_or_keys)}"
        elif isinstance(row_id_or_keys, dict):
            sorted_items = sorted(row_id_or_keys.items())
            return f"{table_name}:{'-'.join(f'{k}={v}' for k, v in sorted_items)}"
        else:
            raise ValueError(f"Unsupported key type: {type(row_id_or_keys)}")

    @staticmethod
    def get_lock_ids(lock_key: str) -> tuple[int, int]:
        """Convert a lock key string to PostgreSQL advisory lock IDs."""
        # Create a hash from the lock string
        lock_hash = Utilities.consistent_hash(
            lock_key, output_format=ConsistentHashOutput.STR)

        # Split the hash for PostgreSQL's two-parameter advisory lock
        lock_id1 = int(lock_hash[:8], 16) % (2 ** 31 - 1)
        lock_id2 = int(lock_hash[8:16], 16) % (2 ** 31 - 1)

        return lock_id1, lock_id2

    def acquire_lock(self, table_name: str, row_id_or_keys: int | str | tuple | list | dict,
                     lock_acquisition_timeout: int = DEFAULT_ADVISORY_LOCK_TIMEOUT, schema: str = "public",
                     lock_timeout: int = 300) -> bool:
        """
        Acquire an advisory lock on a row.

        Args:
            table_name: The name of the table
            row_id_or_keys: Identifier for the row (id, composite key, etc.)
            lock_acquisition_timeout: Maximum time to wait for the lock
            schema: The database schema containing the table
            lock_timeout: The timeout for the lock in seconds

        Returns:
            bool: True if lock was acquired, False if timeout
        """
        # Include schema in the lock key to prevent cross-schema lock conflicts
        lock_key = f"{schema}:{self.generate_lock_key(table_name, row_id_or_keys)}"

        # Check if we already hold this lock
        if lock_key in self.active_locks:
            return True

        # Try to acquire the lock with timeout
        start_time = time.time()
        while True:
            print(f"Attempting to acquire lock: {lock_key}")
            with AbstractSupabaseClient.request_limiter:
                connection: Client = self.supabase_client.create_supabase_service_client(
                    postgrest_client_timeout=max(lock_acquisition_timeout * 1.2, lock_acquisition_timeout + 10)
                )  # Ensure client does not time out before lock
                current_time: int = Utilities.get_current_unix_timestamp()
                expires_at: int = current_time + lock_timeout
                try:

                    existing_lock: dict = self.supabase_client.fetch_all_rows_with_filter(
                        table_name="advisory_locks",
                        filter_value=lock_key,
                        filter_column="lock_key",
                        single_row=True,
                        connection=connection, try_count=1)

                    if len(existing_lock) == 0:
                        lock_success: bool = self._insert_lock(lock_key, current_time, expires_at)
                        if lock_success:
                            print(f"\nLock acquired: {lock_key}\n")
                            self.active_locks.add(lock_key)
                        return lock_success

                    if existing_lock["expires_at"] < current_time:
                        # Lock has expired, delete and reacquire
                        self._delete_lock(lock_key)
                        lock_success: bool = self._insert_lock(lock_key, current_time, expires_at)
                        if lock_success:
                            print(f"\nLock overridden and acquired: {lock_key}\n")
                            GrafanaLokiLogger.log(f"Lock overridden and acquired: {lock_key}", level=LogLevel.WARNING)
                            self.active_locks.add(lock_key)
                        return lock_success

                except APIError as e:
                    print(f"Failed to acquire lock: {e}")

            # Check if we've exceeded the timeout
            elapsed = time.time() - start_time
            if 0 < lock_acquisition_timeout <= elapsed:
                return False

            # Wait before trying again
            time.sleep(1)

    def release_lock(self, table_name: str, row_id_or_keys: int | str | tuple | list | dict,
                     schema: str = "public") -> bool:
        """
        Release an advisory lock on a row.

        Args:
            table_name: The name of the table
            row_id_or_keys: Identifier for the row (id, composite key, etc.)
            schema: The database schema containing the table

        Returns:
            bool: True if lock was released, False otherwise
        """
        # Include schema in the lock key to match the key used in acquire_lock
        lock_key = f"{schema}:{self.generate_lock_key(table_name, row_id_or_keys)}"

        # Check if we hold this lock
        if lock_key not in self.active_locks:
            return False

        try:
            with AbstractSupabaseClient.request_limiter:
                lock_deletion_success: bool = self._delete_lock(lock_key)
                if lock_deletion_success:
                    print(f"\nLock removed: {lock_key}\n")
                return lock_deletion_success


        except Exception as e:
            print(f"Error releasing lock: {e}")
            return False

    def _insert_lock(self, lock_key: str, current_time: int, expires_at: int) -> bool:
        """Insert a new lock into the advisory_locks table."""
        try:
            with AbstractSupabaseClient.request_limiter:
                connection: Client = self.supabase_client.create_supabase_service_client(
                    postgrest_client_timeout=10  # Use a reasonable timeout for insert
                )
                result: APIResponse = connection.table("advisory_locks").insert(
                    {"lock_key": lock_key,
                     "owner": GrafanaLokiLogger.log_group_labels,
                     "acquired_at": current_time,
                     "expires_at": expires_at}).execute()
            return result is not None
        except Exception as e:
            print(f"Error inserting lock: {e}")
            return False

    def _delete_lock(self, lock_key: str) -> bool:
        """Upsert a lock into the advisory_locks table."""
        try:
            with AbstractSupabaseClient.request_limiter:
                connection: Client = self.supabase_client.create_supabase_service_client(
                    postgrest_client_timeout=10  # Use a reasonable timeout for upsert
                )
                result: APIResponse = connection.table("advisory_locks").delete().eq(
                    "lock_key", lock_key).execute()
            return result is not None and len(result.data) > 0
        except Exception as e:
            print(f"Error deleting lock: {e}")
            return False

    def is_locked(self, table_name: str, row_id_or_keys: int | str | tuple | list | dict,
                  schema: str = "public") -> bool:
        """
        Check if we currently hold a lock on this row.

        Args:
            table_name: The name of the table
            row_id_or_keys: Identifier for the row
            schema: The database schema containing the table

        Returns:
            bool: True if the lock is held, False otherwise
        """
        lock_key = f"{schema}:{self.generate_lock_key(table_name, row_id_or_keys)}"
        return lock_key in self.active_locks

    def get_active_locks(self) -> set[str]:
        """Get a list of all currently held locks."""
        return self.active_locks

    @contextmanager
    def with_lock(self, table_name: str, unique_lock_identifier: int | str | tuple | list | dict,
                  lock_acquisition_timeout: int = DEFAULT_ADVISORY_LOCK_TIMEOUT,
                  lock_timeout: int = 300, schema: str = "public"):
        """
        Context manager for acquiring and releasing row locks.

        Args:
            table_name: The name of the table
            unique_lock_identifier: Identifier for the row
            lock_acquisition_timeout: Maximum time to wait for the lock
            lock_timeout: The timeout for the lock in seconds
            schema: The database schema containing the table

        Usage:
            with lock_manager.with_lock("users", 123, schema="auth"):
                # Perform operations on the locked row
        """
        lock_acquired = False
        try:
            lock_acquired = self.acquire_lock(
                table_name,
                unique_lock_identifier,
                lock_acquisition_timeout,
                schema=schema,
                lock_timeout=lock_timeout
            )
            if not lock_acquired:
                raise FailedToAcquireAdvisoryLockError(
                    f"Could not acquire lock for {schema}.{table_name}:{unique_lock_identifier} within {lock_acquisition_timeout} seconds")
            yield
        finally:
            if lock_acquired:
                self.release_lock(table_name, unique_lock_identifier, schema=schema)


class AbstractSupabaseClient(abc.ABC):
    """An abstract class for supabase clients that enforces the singleton pattern."""
    config = Config()
    request_limiter = RequestLimiter()
    init_lock = threading.Lock()

    if DEFAULT_ADVISORY_LOCK_TIMEOUT > DEFAULT_POSTGREST_CLIENT_TIMEOUT:
        raise ValueError("WITH_ROW_LOCK_TIMEOUT must be less than or equal to POSTGREST_CLIENT_TIMEOUT.")

    def __init__(self):
        """Initialize the Supabase client."""
        self.lock_manager = AdvisoryLockManager(self)

    @property
    def schemas_to_include(self) -> list[str]:
        """The schemas to include when fetching table data. Defaults to ['public']."""
        return ['public']

    @property
    @abc.abstractmethod
    def SUPABASE_URL_ENV_NAME(self) -> str:
        """The environment variable name for the Supabase URL."""
        raise NotImplementedError("SUPABASE_URL_ENV_NAME property must be implemented.")

    @property
    @abc.abstractmethod
    def SUPABASE_KEY_ENV_NAME(self) -> str:
        """The environment variable name for the Supabase key."""
        raise NotImplementedError("SUPABASE_KEY_ENV_NAME property must be implemented.")

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

    @property
    @abc.abstractmethod
    def database_tables(self):
        """
        The tables in the database.
        """
        raise NotImplementedError("database_tables property must be implemented.")

    @property
    @abc.abstractmethod
    def row_retrieval_limit(self):
        """
        The max number of rows that can be retrieved at a time. Can be increased in Supabase, and set in the _config.
        """
        raise NotImplementedError("row_retrieval_limit property must be implemented.")

    @property
    def supabase_service_client(self):
        return self.create_supabase_service_client(schema="public")

    @property
    def supabase_queue_client(self):
        return self.create_supabase_service_client(schema="pgmq_public")

    def create_supabase_service_client(self, schema: str = "public",
                                       postgrest_client_timeout: int = DEFAULT_POSTGREST_CLIENT_TIMEOUT) -> Client:
        """
        Creates a Supabase service client instance

        Args:
            schema: The schema to use for the client (default: "public")
            postgrest_client_timeout: The timeout for the client

        Returns:
            The Supabase client instance
        """
        load_dotenv()
        url: str = os.environ.get(self.SUPABASE_URL_ENV_NAME)
        key: str = os.environ.get(self.SUPABASE_KEY_ENV_NAME)
        if not isinstance(url, str) or not isinstance(key, str):
            raise ValueError(
                f"Supabase URL or key not found in environment variables {self.SUPABASE_URL_ENV_NAME} and {self.SUPABASE_KEY_ENV_NAME}")
        options = ClientOptions(
            schema=schema,
            persist_session=False,
            postgrest_client_timeout=postgrest_client_timeout,
        )
        return create_client(url, key, options=options)

    def insert_row(self, table_name: str, row_data: any) -> dict:
        """
        Inserts a row into the table. Validates the row data against the table structure.

        Allows for the insertion of a dictionary or an object.
        The object would be of the type automatically created for the table.
        """
        if not isinstance(row_data, dict):
            row_data = row_data.to_dict()

        self._validate_insertion_row_data(table_name, row_data)
        json_serialized_row_data = Utilities.custom_json_serializer(row_data)

        # Remove any None or null values in the row data
        json_serialized_row_data = {key: value for key, value in json_serialized_row_data.items() if value is not None}
        try:
            with AbstractSupabaseClient.request_limiter:
                return self.supabase_service_client.table(table_name).insert(json_serialized_row_data).execute().data[0]
        except httpx.HTTPError as e:
            raise Utilities.raise_error_with_formatted_traceback(e)

    def upsert_row(self, table_name: str, row_data: any):
        """
        Upserts a row into the table. Validates the row data against the table structure.

        Allows for the insertion of a dictionary or an object.
        The object would be of the type automatically created for the table.
        """
        if not isinstance(row_data, dict):
            row_data = row_data.__dict__

        self._validate_insertion_row_data(table_name, row_data)
        json_serialized_row_data = Utilities.custom_json_serializer(row_data)
        try:
            with AbstractSupabaseClient.request_limiter:
                return self.supabase_service_client.table(table_name).upsert(json_serialized_row_data).execute()
        except httpx.HTTPError as e:
            raise Utilities.raise_error_with_formatted_traceback(e)

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

    # noinspection PyUnresolvedReferences
    def fetch_all_rows(self, table_name, sort_by_column=None, query_limit=None, try_count: int = 0,
                       allowed_retries: int = 3, connection: Client | None = None) -> list[dict]:
        """
        Fetches all rows from a Supabase table.
        """
        data = []
        has_more = True
        pagination_range = 0

        if connection is None:
            connection = self.supabase_service_client

        print(f"Fetching all rows from {table_name}...")

        try:
            while has_more:
                query = connection.table(table_name).select("*").limit(
                    self.row_retrieval_limit).range(pagination_range,
                                                    pagination_range + self.row_retrieval_limit)
                with AbstractSupabaseClient.request_limiter:
                    response = query.execute()
                fetched_data = response.data

                if fetched_data:
                    data.extend(fetched_data)
                else:
                    has_more = False

                if query_limit and len(data) >= query_limit:
                    break

                pagination_range += self.row_retrieval_limit

            if sort_by_column:
                data = self.sort_columns(data, sort_by_column)

            return data
        except httpx.HTTPError as e:
            if try_count < allowed_retries:
                return self.fetch_all_rows(table_name, sort_by_column, query_limit, try_count + 1, allowed_retries)
            raise Utilities.raise_error_with_formatted_traceback(e)

    def fetch_all_rows_with_filter(self, table_name, filter_column, filter_value, single_row=False,
                                   sort_by_column=None, query_limit=None, try_count: int = 0,
                                   allowed_retries: int = 3, connection: Client | None = None) -> dict | list[dict]:
        """
        Fetches all rows from a Supabase table with a filter for a single column value.
        """
        data = []
        has_more = True
        pagination_range = 0

        if connection is None:
            connection = self.supabase_service_client

        try:
            while has_more:
                query = connection.table(table_name).select("*").eq(filter_column,
                                                                    filter_value).limit(
                    self.row_retrieval_limit).range(pagination_range,
                                                    pagination_range + self.row_retrieval_limit)
                with AbstractSupabaseClient.request_limiter:
                    response = query.execute()
                fetched_data = response.data

                if fetched_data:
                    data.extend(fetched_data)
                else:
                    has_more = False

                if query_limit and len(data) >= query_limit:
                    break

                pagination_range += self.row_retrieval_limit

            if single_row:
                try:
                    return data[0]
                except IndexError:
                    return {}

            if sort_by_column:
                data = self.sort_columns(data, sort_by_column)

            return data
        except httpx.HTTPError as e:
            if try_count < allowed_retries:
                return self.fetch_all_rows_with_filter(table_name, filter_column, filter_value, single_row,
                                                       sort_by_column, query_limit, try_count + 1, allowed_retries)
            raise Utilities.raise_error_with_formatted_traceback(e)
        except httpcore.ConnectError as e:
            if try_count < allowed_retries:
                return self.fetch_all_rows_with_filter(table_name, filter_column, filter_value, single_row,
                                                       sort_by_column, query_limit, try_count + 1, allowed_retries)
            raise Utilities.raise_error_with_formatted_traceback(e)

    def fetch_all_rows_with_filter_by_multiple_values(self, table_name, filter_column, filter_values, single_row=False,
                                                      sort_by_column=None, query_limit=None, try_count: int = 0,
                                                      allowed_retries: int = 3
                                                      ):
        """
        Fetches all rows from a Supabase table with a filter for multiple column values.
        """
        data = []
        has_more = True
        pagination_range = 0

        try:
            while has_more:
                query = self.supabase_service_client.table(table_name).select("*").in_(filter_column,
                                                                                       filter_values).limit(
                    self.row_retrieval_limit).range(pagination_range,
                                                    pagination_range + self.row_retrieval_limit)
                with AbstractSupabaseClient.request_limiter:
                    response = query.execute()
                fetched_data = response.data

                if fetched_data:
                    data.extend(fetched_data)
                else:
                    has_more = False

                if query_limit and len(data) >= query_limit:
                    break

                pagination_range += self.row_retrieval_limit

            if single_row:
                try:
                    return data[0]
                except IndexError:
                    return []

            if sort_by_column:
                data = self.sort_columns(data, sort_by_column)

            return data
        except httpx.HTTPError as e:
            if try_count < allowed_retries:
                return self.fetch_all_rows_with_filter_by_multiple_values(table_name, filter_column, filter_values,
                                                                          single_row, sort_by_column, query_limit,
                                                                          try_count + 1, allowed_retries)
            raise Utilities.raise_error_with_formatted_traceback(e)

    def fetch_all_rows_with_a_value(self, table_name, filter_column, sort_by_column=None,
                                    query_limit=None, try_count: int = 0, allowed_retries: int = 3):
        """
        Fetches all rows from a Supabase table with a filter for a single column value.
        """
        data = []
        has_more = True
        pagination_range = 0

        try:
            while has_more:
                query = self.supabase_service_client.table(table_name).select("*").not_.is_(filter_column,
                                                                                            "null").limit(
                    self.row_retrieval_limit).range(pagination_range,
                                                    pagination_range + self.row_retrieval_limit)
                with AbstractSupabaseClient.request_limiter:
                    response = query.execute()
                fetched_data = response.data

                if fetched_data:
                    data.extend(fetched_data)
                else:
                    has_more = False

                if query_limit and len(data) >= query_limit:
                    data = data[:query_limit]
                    break

                pagination_range += self.row_retrieval_limit

            if sort_by_column:
                data = self.sort_columns(data, sort_by_column)

            return data
        except httpx.HTTPError as e:
            if try_count < allowed_retries:
                return self.fetch_all_rows_with_a_value(table_name, filter_column, sort_by_column, query_limit,
                                                        try_count + 1, allowed_retries)
            raise Utilities.raise_error_with_formatted_traceback(e)

    def execute_query_with_retry(self, table_name: str, filters: list[dict], single_row: bool = False,
                                 max_retries: int = 5) -> list[dict] | dict:
        """
        Executes a query on a table with multiple filters and handles retries on failure.

        Args:
            table_name (str): Name of the table to query
            filters (list[dict]): List of filter dictionaries. Each dict should contain:
                - 'column': Column name to filter on
                - 'operator': Operator to use ('eq', 'gt', 'lt', 'gte', 'lte', 'in_', 'not_', 'is_', etc.)
                - 'value': Value to filter by
            max_retries (int): Maximum number of retry attempts (default: 5)
            single_row (bool): Whether to return a single row (default: False)

        Returns:
            list[dict]: List of matching rows

        Example:
            filters = [
                {'column': 'status_code', 'operator': 'eq', 'value': 200},
                {'column': 'timestamp', 'operator': 'gte', 'value': some_timestamp}
            ]
            results = client.execute_query_with_retry('my_table', filters)
        """
        data = []
        has_more = True
        pagination_range = 0

        for attempt in range(max_retries):
            try:
                while has_more:
                    # Start the query
                    query = self.supabase_service_client.table(table_name).select("*").limit(
                        self.row_retrieval_limit).range(
                        pagination_range, pagination_range + self.row_retrieval_limit)

                    # Apply all filters
                    for filter_dict in filters:
                        column = filter_dict['column']
                        operator = filter_dict['operator']
                        value = filter_dict['value']

                        # Get the operator method
                        if hasattr(query, operator):
                            query = getattr(query, operator)(column, value)
                        else:
                            raise ValueError(f"Invalid operator: {operator}")

                    # Execute the query
                    response = query.execute()
                    fetched_data = response.data

                    if fetched_data:
                        data.extend(fetched_data)
                    else:
                        has_more = False

                    pagination_range += self.row_retrieval_limit

                if single_row:
                    if len(data) > 1:
                        raise IndexError(f"Multiple rows found for query with single row specified: {filters}")
                    if len(data) == 0:
                        return {}
                    return data[0]

                return data

            except httpx.HTTPError as e:
                if attempt < max_retries - 1:
                    sleep_time = 0.2
                    print(
                        f"Query attempt {attempt + 1} failed as a result of httpx.HTTPError. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    print(f"All {max_retries} query attempts failed. Last error: {Utilities.format_error(e)}")
                    raise e

            except Exception as e:
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    sleep_time = 0.2
                    print(f"Data retrieval attempt {attempt + 1} failed. Retrying in {sleep_time} seconds...")
                    print(f"{Utilities.format_error(e)}")
                    time.sleep(sleep_time)
                else:
                    print(f"All retrieval {max_retries} attempts failed. Last error: {Utilities.format_error(e)}")
                    raise e  # Re-raise the last exception if all retries failed

    def read_message_from_queue(self, queue_name: str,
                                item_lock_seconds: int, n: int,
                                try_count: int = 0, allowed_retries: int = 3) -> dict:
        """Reads a message from a queue

        Args:
            queue_name (str): The name of the queue
            item_lock_seconds (int): The number of seconds for which to lock the items read from the queue
            n (int): The number of items to read from the queue
            try_count (int): The number of times the function has been called
            allowed_retries (int): The maximum number of retries allowed
        """
        try:
            with AbstractSupabaseClient.request_limiter:
                response = self.supabase_queue_client.rpc(
                    "read", params={
                        "queue_name": queue_name,
                        "sleep_seconds": item_lock_seconds,
                        "n": n
                    }).execute()
            return response.data
        except httpx.HTTPError as e:
            if try_count < allowed_retries:
                return self.read_message_from_queue(queue_name, try_count + 1, allowed_retries)
            raise Utilities.raise_error_with_formatted_traceback(e)

    def delete_message_from_queue(self, queue_name: str, message_id: int, try_count: int = 0, allowed_retries: int = 3):
        """Deletes a message from a queue"""
        try:
            with AbstractSupabaseClient.request_limiter:
                self.supabase_queue_client.rpc(
                    "delete", {
                        "queue_name": queue_name,
                        "message_id": message_id
                    }).execute()
        except httpx.HTTPError as e:
            if try_count < allowed_retries:
                self.delete_message_from_queue(queue_name, message_id, try_count + 1, allowed_retries)
            raise Utilities.raise_error_with_formatted_traceback(e)

    @staticmethod
    def sort_columns(list_to_sort, column_to_sort_by):
        if column_to_sort_by is None:
            return list_to_sort

        def key_func(whale):
            value = whale.get(column_to_sort_by)
            return value if value is not None else float('-inf')  # Send None values to the end

        return sorted(list_to_sort, key=key_func, reverse=True)

    @staticmethod
    def _validate_column_presence_for_insertion(table_data_structure: any, row_data: dict):
        """
        Validates that the row data has all the columns required for insertion.
        """
        # validate all necessary columns are present
        column_names = [column_name for column_name in table_data_structure.__annotations__.keys()]
        optional_columns = [column_name for column_name in column_names if
                            type(table_data_structure.__annotations__[column_name]) == UnionType
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
            AbstractSupabaseClient._validate_type(correct_type=table_data_structure.__annotations__[column_name],
                                                  data_to_check=row_data[column_name],
                                                  column_name=column_name)

    @staticmethod
    def _validate_type(correct_type: any, data_to_check: any, column_name: str):
        """
        Validates that the row data has the correct types.
        """
        # Check against union types
        if isinstance(correct_type, UnionType):
            for type_ in correct_type.__args__:
                if type(type_) is GenericAlias:  # Type is a list or dict
                    try:
                        AbstractSupabaseClient._validate_type(type_, data_to_check, column_name)
                        return
                    except ValueError:  # Not the correct type
                        continue
                if type_ is type(data_to_check):
                    return
            raise ValueError(f"Column '{column_name}' is not of type {correct_type}.")

        # Check against array types
        elif str(correct_type)[:4] == "list" or str(correct_type)[-6:-2] == "list":

            if not isinstance(data_to_check, list):
                raise ValueError(f"Column '{column_name}' is not of type {correct_type}.")

            if len(data_to_check) == 0:  # Pass by default
                return

            correct_subtype = correct_type.__args__[0]
            first_instance_to_check = data_to_check[0]
            AbstractSupabaseClient._validate_type(correct_subtype, first_instance_to_check, column_name)

        # Check against single types
        elif not isinstance(data_to_check, correct_type):
            raise ValueError(
                f"Column '{column_name}' is not of type {correct_type}.")

    def lock_row(self, table_name: str, row_id_or_keys: int | str | tuple | list | dict,
                 lock_acquisition_timeout: int = 0) -> bool:
        """Delegates to the lock manager to acquire a lock."""
        return self.lock_manager.acquire_lock(table_name, row_id_or_keys, lock_acquisition_timeout)

    def unlock_row(self, table_name: str, row_id_or_keys: int | str | tuple | list | dict) -> bool:
        """Delegates to the lock manager to release a lock."""
        return self.lock_manager.release_lock(table_name, row_id_or_keys)

    @contextmanager
    def with_advisory_lock(self, table_name: str, unique_lock_identifier: int | str | tuple | list | dict,
                           timeout_seconds: int = DEFAULT_ADVISORY_LOCK_TIMEOUT):
        """Delegates to the lock manager's context manager."""
        with self.lock_manager.with_lock(table_name, unique_lock_identifier, timeout_seconds):
            yield

    def is_row_locked(self, table_name: str, row_id_or_keys: int | str | tuple | list | dict) -> bool:
        """Check if a row is currently locked by this client."""
        return self.lock_manager.is_locked(table_name, row_id_or_keys)

    def get_all_tables_from_supabase(self, schema: str = "public"):
        """
        Calls the rpc method 'get_all_tables'.

        This method must first be implemented in supabase.
        Run this code in the SQL editor to implement in a new database.

        CREATE OR REPLACE FUNCTION public.get_all_tables(p_schema TEXT)
        RETURNS TABLE (table_name TEXT) AS $$
        BEGIN
            RETURN QUERY
            SELECT t.table_name::text
            FROM information_schema.tables t
            WHERE
                t.table_schema = p_schema
                AND t.table_type IN ('BASE TABLE', 'TABLE')
            ORDER BY t.table_name;
        END;
        $$ LANGUAGE plpgsql;

        -- Test the function
        select
          *
        from
          public.get_all_tables ();
        """
        return self.supabase_service_client.rpc("get_all_tables", {"p_schema": schema}).execute().data

    def prepare_schema_access(self, schema: str):
        """
        Prepare access to a new schema before querying

        This method must first be implemented in supabase.
        Run this code in the SQL editor to implement in a new database.

        CREATE OR REPLACE FUNCTION public.grant_schema_permissions(p_schema TEXT)
        RETURNS BOOLEAN AS $$
        DECLARE
            v_result BOOLEAN := FALSE;
        BEGIN
            -- Grant usage on schema
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO authenticated, service_role', p_schema);

            -- Grant select on all tables in schema
            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO authenticated, service_role', p_schema);

            -- For future tables, grant select permissions automatically
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO authenticated, service_role', p_schema);

            v_result := TRUE;

            RETURN v_result;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'Error granting permissions on schema %: %', p_schema, SQLERRM;
                RETURN FALSE;
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER;
        """
        response = self.supabase_service_client.rpc(
            "grant_schema_permissions",
            params={"p_schema": schema}
        ).execute()

        # Check if permissions were successfully granted
        return response.data

    def get_table_data_structure_from_supabase(self, table_name: str, schema: str = "public"):
        """
        Calls the rpc method 'get_table_data_structure'.

        This method must first be implemented in supabase.
        Run this code in the SQL editor to implement in a new database.

        CREATE OR REPLACE FUNCTION public.get_table_data_structure(p_schema TEXT, p_table_name TEXT)
        RETURNS jsonb AS $$
        DECLARE
            result jsonb := '[]'::jsonb;  -- Initialize an empty JSON array
        BEGIN
            SELECT jsonb_agg(jsonb_build_object(
                'column_name', c.column_name,
                'data_type', c.data_type,
                'is_primary_key', EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = p_schema
                    AND tc.table_name = c.table_name
                    AND kcu.column_name = c.column_name
                    AND tc.constraint_type = 'PRIMARY KEY'
                ),
                'is_nullable', c.is_nullable = 'YES',
                'is_identity', c.is_identity = 'YES',
                'has_default_value', c.column_default IS NOT NULL,
                'array_subtype', CASE
                    WHEN c.data_type = 'ARRAY' THEN c.udt_name
                    ELSE NULL
                END
            ))
            INTO result
            FROM information_schema.columns c
            WHERE c.table_schema = p_schema
            AND c.table_name = p_table_name;

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
                                                {"p_table_name": table_name, "p_schema": schema}
                                                ).execute().data

    def update_table_types(self):
        """
        Updates the local database types file with the types from the supabase database.
        Uses dataclasses with slots=True for better performance and cleaner code.
        """
        print(f"Updating table types for {self.__class__.__name__}")

        # Add the required imports at the beginning of the file
        imports = (
            "from dataclasses import dataclass, field, asdict\n"
            "from typing import Optional, List, Dict, Any, Union\n\n"
        )

        for schema in self.schemas_to_include:
            if schema != "public":
                self.prepare_schema_access(schema)  # Ensure we have access to the schema
            tables = self.get_all_tables_from_supabase(schema=schema)
            tables_list = [table["table_name"] for table in tables]
            type_definitions = {}

            # Make an initial class that links the table name to the column type identifiers
            linked_table_column_types = f"class LinkedTableColumnTypes:\n    linked_table_dict = {{\n"
            database_tables = f"class DatabaseTables:\n"

            # to_dict function
            to_dict_function = (
                f"def dataclass_to_dict(obj):\n"
                f"    \"\"\"Returns a dictionary representation of the dataclass with None values removed.\"\"\"\n"
                f"    return {{k: v for k, v in asdict(obj).items() if v is not None}}\n\n"
            )

            for table_name in tables_list:
                column_definitions = self.get_table_data_structure_from_supabase(table_name, schema=schema)

                class_name = table_name.replace("_", " ")
                class_name = class_name.title()
                class_name = class_name.replace(" ", "")
                if class_name[-1] == "s":
                    class_name = class_name[:-1]
                class_name += "ColumnTypes"

                # Start with a dataclass but disable the auto-generated __init__
                type_definition = f"@dataclass(slots=True, init=False)\nclass {class_name}:\n"

                # Define fields
                field_definitions = []
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

                    # Handle field type annotation
                    if type_name != "ARRAY":
                        python_type = stringified_python_type
                    else:
                        array_subtype = column_definition["array_subtype"].replace("_", "").replace(" ", "")
                        stringified_python_type = supabase_types_to_python_types[array_subtype]
                        python_type = f"List[{stringified_python_type}]"

                    # Add Optional for nullable fields
                    if not is_primary_key and (is_nullable or has_default_value or is_identity):
                        python_type = f"Optional[{python_type}]"

                    # Add the field with default=None
                    type_definition += f"    {column_name}: {python_type} = None\n"
                    field_definitions.append(column_name)

                # Add custom __init__ method that accepts extra arguments via *args and **kwargs
                type_definition += f"\n    def __init__(self, "

                # Add all the normal parameters with default values
                for field_name in field_definitions:
                    type_definition += f"{field_name}=None, "

                # Add *args and **kwargs to handle any unexpected arguments
                type_definition += "*args, **kwargs):\n"

                # Set all fields from the parameters
                for field_name in field_definitions:
                    type_definition += f"        self.{field_name} = {field_name}\n"

                # Add to_dict method
                type_definition += f"\n    def to_dict(self):\n"
                type_definition += f"        \"\"\"Returns a dictionary representation of the object with None values removed.\"\"\"\n"
                type_definition += f"        return dataclass_to_dict(self)\n"

                type_definitions[table_name] = type_definition

                linked_table_column_types += f'        "{table_name}": {class_name},\n'
                database_tables += f"    {table_name}: str = '{table_name}'\n"

            # close linked_table_dict
            linked_table_column_types += "    }\n"

            # close database_tables
            database_tables += "\n"

            root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            full_file_path = os.path.join(root_path, self.datatypes_file_path_from_root)[:-3] + f"_{schema}.py"

            with open(full_file_path, "w") as f:
                # Write the imports
                f.write(imports)

                # Write the to_dict function
                f.write(to_dict_function)

                # Write all the class definitions
                for table_name, type_definition in type_definitions.items():
                    f.write(f"{type_definition}\n\n")

                # Write the linked_table_column_types and database_tables
                f.write(f"{linked_table_column_types}\n\n")
                f.write(f"{database_tables}")
