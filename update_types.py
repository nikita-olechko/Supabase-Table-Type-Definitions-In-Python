from MySupabaseClient import MySupabaseClient


def main():
    """
    Updates all types across supabase databases.
    """
    my_supabase_client = MySupabaseClient()
    my_supabase_client.update_table_types()


if __name__ == "__main__":
    main()
