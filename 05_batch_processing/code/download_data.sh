# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet

set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in $(seq -w 1 12); do
  URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${MONTH}.parquet"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${MONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${MONTH}.parquet"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  mkdir -p ${LOCAL_PREFIX}

  wget $URL -O ${LOCAL_PATH}

  # gzip ${LOCAL_PATH}
done
