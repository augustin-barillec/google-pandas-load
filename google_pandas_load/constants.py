LOCATIONS = ['query', 'dataset', 'bucket', 'local', 'dataframe']
REVERSED_LOCATIONS = list(reversed(LOCATIONS))
SOURCE_LOCATIONS = LOCATIONS
DESTINATION_LOCATIONS = LOCATIONS[1:]
MIDDLE_LOCATIONS = LOCATIONS[1: -1]
DESTINATIONS_TO_ALWAYS_CLEAR = ['bucket', 'local']
ATOMIC_FUNCTION_NAMES = [
    'query_to_dataset', 'dataset_to_bucket', 'bucket_to_local',
    'local_to_dataframe', 'dataframe_to_local', 'local_to_bucket',
    'bucket_to_dataset']
BQ_CLIENT_ATOMIC_FUNCTION_NAMES = [
    'query_to_dataset', 'dataset_to_bucket', 'bucket_to_dataset']
