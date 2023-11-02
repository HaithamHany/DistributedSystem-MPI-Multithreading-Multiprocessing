import time
import pandas as pd
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


def GetDivertedFlights(reading_info: list):
    start_time = time.time()
    df = pd.read_csv('../datasets/Combined_Flights_2021.csv', nrows=reading_info[0], skiprows=range(1, reading_info[1]))
    df = df[['Airline', 'Diverted', 'FlightDate']]

    if df.empty:
        print('Empty chunk...')
        end_time = time.time()
        print("Chunk time taken  : " + str(end_time - start_time))
        return df

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


def main():
    slave_workers = size - 1
    rows = GetNumberOfRows()
    chunk_size = int(rows / slave_workers)
    print('using MPI....')

    start_time = time.time()
    chunk_distribution = distribute_rows(n_rows=chunk_size, n_processes=slave_workers)
    distributeTasks(chunk_distribution, size)

    results = GetResults()

    print(f'TOTAL DIVERTED FLIGHTS Between 20th and 30th NOV = {GetTotalResults(results)}')
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


def GetTotalResults(results):
    total = 0
    for res in results:
        total += res;
    return total


def distributeTasks(distribution, size):
    for worker in range(1, size):
        chunk = worker - 1
        comm.send(distribution[chunk], dest=worker)


def GetResults():
    results = []
    for worker in (range(1, size)):  # receive
        result = comm.recv(source=worker)
        results.append(result)
        print(f'received from Worker slave {worker}')
    return results


if __name__ == "__main__":

    if rank == 0:
        main()
    elif rank > 0:
        chunk = comm.recv()
        print(f'Worker {rank} is assigned chunk info {chunk}')
        result = GetDivertedFlights(chunk)
        print(f'Worker slave {rank} is done. Sending back to master')
        comm.send(result, dest=0)
