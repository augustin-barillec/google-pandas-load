LOCATIONS = ['query', 'bq', 'gs', 'local', 'dataframe']
REVERSED_LOCATIONS = list(reversed(LOCATIONS))
SOURCE_LOCATIONS = LOCATIONS
DESTINATION_LOCATIONS = LOCATIONS[1:]
MIDDLE_LOCATIONS = LOCATIONS[1: -1]
DESTINATIONS_TO_ALWAYS_CLEAR = ['gs', 'local']

ATOMIC_FUNCTION_NAMES = [
    'query_to_bq', 'bq_to_gs', 'gs_to_local', 'local_to_dataframe',
    'dataframe_to_local', 'local_to_gs', 'gs_to_bq'
]
BQ_CLIENT_ATOMIC_FUNCTION_NAMES = ['query_to_bq', 'bq_to_gs', 'gs_to_bq']
