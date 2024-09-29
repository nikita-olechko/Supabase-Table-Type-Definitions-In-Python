import os

from dotenv import load_dotenv
from supabase import create_client, Client

from AbstractSupabaseClient import AbstractSupabaseClient


class MySupabaseClient(AbstractSupabaseClient):
    """
    An example implementation for a database.
    """

    def __init__(self):
        self._supabase_service_client = self.create_supabase_service_client()

    @property
    def datatypes_file_path_from_root(self):
        return "types/my_supabase_client.py"

    @property
    def supabase_service_client(self):
        return self._supabase_service_client

    def create_supabase_service_client(self):
        """
        Creates a Supabase service client instance.
        :return: The Supabase client instance
        """
        load_dotenv()
        url: str = os.environ.get("MY_SUPABASE_URL")
        key: str = os.environ.get("MY_SUPABASE_SERVICE_KEY")
        supabase: Client = create_client(url, key)
        return supabase
