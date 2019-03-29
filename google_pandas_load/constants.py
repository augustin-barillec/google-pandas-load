LOCATIONS = ['query', 'bq', 'gs', 'local', 'dataframe']

MIDDLE_LOCATIONS = LOCATIONS[1: -1]

REVERSED_LOCATIONS = list(reversed(LOCATIONS))

ATOMIC_FUNCTION_NAMES = ['query_to_bq', 'bq_to_gs', 'gs_to_local', 'local_to_dataframe',
                         'dataframe_to_local', 'local_to_gs', 'gs_to_bq', 'bq_to_query']
