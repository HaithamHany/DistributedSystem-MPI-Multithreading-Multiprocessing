import time
import pandas as pd
import multiprocessing as multiprocessing


def GetCancelledFlights(reading_info: list):
    start_time = time.time()
    df = pd.read_csv('../datasets/Combined_Flights_2021.csv', nrows=reading_info[0], skiprows=range(1, reading_info[1]))
    df2 = df[['Airline', 'Cancelled', 'FlightDate']]
    cancelledData = df2.loc[df['Cancelled'] == True]

    # Getting most cancelled Airline in September
    start_date = '2021-09-01'
    end_date = '2021-09-30'

    mask = (cancelledData['FlightDate'] > start_date) & (cancelledData['FlightDate'] <= end_date)
    fd = cancelledData.loc[mask]
    fd.value_counts()

    if fd.empty:
        print('Empty chunk...')
        end_time = time.time()
        print("Chunk time taken  : " + str(end_time - start_time))
        return fd

    print('processing one chunk...')
    count = fd.groupby(['Airline']).count()

    # get chunk processed and sorted
    processed_chunk = count.sort_values(ascending=False, by=['Cancelled'])
    # remove flight date coloumn
    processed_chunk.pop("FlightDate")
    # Removing header
    processed_chunk.columns = range(processed_chunk.shape[1])

    end_time = time.time()
    print("Chunk time taken  : " + str(end_time - start_time))
    return processed_chunk


def GetTotalResults(results):
    processed_chunks = []
    for res in results:
        if res.empty:
            continue
        processed_chunks.append(res)

    merged_chunks = pd.concat(processed_chunks);

    # get name and value
    name = merged_chunks.iloc[:1]

    return name


def distribute_rows(n_rows: int, n_processes):
    reading_info = []
    skip_rows = 0
    reading_info.append([n_rows - skip_rows, skip_rows])
    skip_rows = n_rows

    for _ in range(1, n_processes - 1):
        reading_info.append([n_rows, skip_rows])
        skip_rows = skip_rows + n_rows

    reading_info.append([None, skip_rows])
    return reading_info


def main():
    multiprocessing_pool_size = 4
    mp_pool = multiprocessing.Pool(multiprocessing_pool_size)

    rows = GetNumberOfRows()
    chunk_size = int(rows / multiprocessing_pool_size);
    print('using multiprocessing')

    start_time = time.time()

    result = mp_pool.map(GetCancelledFlights, distribute_rows(n_rows=chunk_size, n_processes=multiprocessing_pool_size))
    print(f'{GetTotalResults(result)}')
    mp_pool.close()
    mp_pool.join()
    end_time = time.time()
    print("Total time of handling  : " + str(end_time - start_time))


def GetNumberOfRows():
    return int(sum(1 for line in open('../datasets/Combined_Flights_2021.csv')))


if __name__ == "__main__":
   main()
