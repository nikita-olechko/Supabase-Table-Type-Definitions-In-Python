# Supabase-Table-Type-Definitions-In-Python
Quick implementation of autogenerating type definitions for Supabase tables in Python.

The equivalent command for Node projects is "npx supabase gen types typescript --project-id \"<YOUR_PROJECT_ID>\" > <YOUR_FILE_PATH (ending in .ts)>"

Far from a comprehensive implementation, but this works. PRs and other contributions are welcome.

First, check this issue log to see if Supabase has already implemented their own solution: https://github.com/supabase/postgres-meta/issues/795 

To use:

```pip install supabase-py```

1. Open AbstractSupabaseClient, and create the two SQL functions listed under get_all_tables and get_table_data_structure. To do so, copy the function creation code and run it in the Supabase SQL editor.
2. Implement your Supabase client by adding the MY_SUPABASE_URL and MY_SUPABASE_SERVICE_KEY environment variables (or add your own version of a client manager).
3. Run the script 'python update_types.py'
