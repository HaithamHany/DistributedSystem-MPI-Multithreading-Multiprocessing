import time
import pandas as pd
from multiprocessing.pool import ThreadPool


def GetMissingDates(reading_info: list):
    print(f'started processing chunk. ')
    start_time = time.time()
    df = pd.read_csv('../datasets/Combined_Flights_2021.csv', nrows=reading_info[0], skiprows=range(1, reading_info[1]))
    # Getting missing dates
    missing_dates = df[df.isna().any(axis=1)]

    if missing_dates.empty:
        print("Empty Chunk")
        return

    # Getting missing dates by flight date
    df_new = missing_dates.groupby(df['FlightDate'], as_index=False)

    # crunch them all and get their size
    combined = df_new.size()
    # Sort them by size
    combined.sort_values(by='size', ascending=False)
    end_time = time.time()
    print("Chunk time taken  : " + str(end_time - start_time))
    return combined


def GetTotalResults(results):
    processed_chunks = []
    for res in results:
        if res.empty:
            continue
        processed_chunks.append(res)

    merged_chunks = pd.concat(processed_chunks)
    merged_chunks.groupby(merged_chunks['FlightDate'], as_index=False).size()
    return merged_chunks.sort_values(by='size', ascending=False)


def main():
    thread_pool_size = 10
    thread_pool = ThreadPool(thread_pool_size)

    rows = GetNumberOfRows()
    # Split chunk evenly.
    chunk_size = int(rows / thread_pool_size);
    print('started multithreading....')
    start_time = time.time()

    results = thread_pool.map(GetMissingDates, distribute_rows(n_rows=chunk_size, n_processes=thread_pool_size))

    result = GetTotalResults(results)
    print( result)

    thread_pool.close()
    thread_pool.join()
    end_time = time.time()
    print("Total time of handling  : " + str(end_time - start_time))


def distribute_rows(n_rows: int, n_processes):
    start_time = time.time()
    reading_info = []
    skip_rows = 0
    reading_info.append([n_rows - skip_rows, skip_rows])
    skip_rows = n_rows

    for _ in range(1, n_processes - 1):
        reading_info.append([n_rows, skip_rows])
        skip_rows = skip_rows + n_rows

    reading_info.append([None, skip_rows])
    end_time = time.time()
    return reading_info


def GetNumberOfRows():
    return int(sum(1 for line in open('../datasets/Combined_Flights_2021.csv')))


if __name__ == "__main__":
    main()
