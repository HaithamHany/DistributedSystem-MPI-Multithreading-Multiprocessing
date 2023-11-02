import time
import pandas as pd
import multiprocessing as multiprocessing


def GetDivertedFlights(reading_info: list):
    start_time = time.time()
    df = pd.read_csv('../datasets/Combined_Flights_2021.csv', nrows=reading_info[0], skiprows=range(1, reading_info[1]))
    df = df[['Airline', 'Diverted', 'FlightDate']]

    start_date = '2021-11-20'
    end_date = '2021-11-30'
    # Select DataFrame rows between two dates
    mask = (df['FlightDate'] > start_date) & (df['FlightDate'] <= end_date)
    df2 = df.loc[mask]

    res = df2.loc[df['Diverted'] == True]
    diverted_flights = res.value_counts().sum()
    print(f'diverted flights {diverted_flights}')
    end_time = time.time()
    print(f'process time  : {str(end_time - start_time)}')
    return diverted_flights


def GetTotalResults(results):
    total = 0
    for res in results:
        total += res;
    return total


def main():
    multiprocessing_pool_size = 4
    mp_pool = multiprocessing.Pool(multiprocessing_pool_size)

    rows = GetNumberOfRows()
    # Split chunk evenly by the pool size
    chunk_size = int(rows / multiprocessing_pool_size);

    print('using multiprocessing')

    start_time = time.time()
    result = mp_pool.map(GetDivertedFlights, distribute_rows(n_rows=chunk_size, n_processes=multiprocessing_pool_size))
    print(f'TOTAL DIVERTED FLIGHTS Between 20th and 30th NOV = {GetTotalResults(result)}')
    mp_pool.close()
    mp_pool.join()
    end_time = time.time()
    print("Total time of handling  : " + str(end_time - start_time))


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


def GetNumberOfRows():
    return int(sum(1 for line in open('../datasets/Combined_Flights_2021.csv')))


if __name__ == "__main__":
   main()
