import datetime
import math
import time
import pandas as pd
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


# Getting average flight time from Nashville to Chicago
def GetAverageFlightTime(reading_info: list):

    start_time = time.time()
    df = pd.read_csv('../datasets/Combined_Flights_2021.csv', nrows=reading_info[0], skiprows=range(1, reading_info[1]))
    # getting flights between Nashvill and Chicago
    mask = (df['Origin'] == 'BNA') & (df['Dest'] == 'ORD')
    df2 = df.loc[mask]

    # getting the flights wheel off and wheel on column
    flights_time = df2[['WheelsOff', 'WheelsOn']]

    # applying custom function to get difference for each row
    diff = flights_time.apply(lambda x: GetTimeTakenInAir(x.WheelsOff, x.WheelsOn), axis=1)

    # getting the average
    average = diff.mean()

    print(f'chunk average {average}')
    end_time = time.time()
    print(f'process time  : {str(end_time - start_time)}')
    return average


# custom function that gets time in air by calculating difference between wheel on and wheel off
# and returns time in hours
def GetTimeTakenInAir(val, val2):
    if math.isnan(val) or math.isnan(val2):
        return
    wheelsOff = int(val)
    wheelsOn = int(val2)

    start = datetime.datetime.strptime(str(wheelsOff), "%H%M")  # convert string to time
    end = datetime.datetime.strptime(str(wheelsOn), "%H%M")
    diff = end - start

    return diff / pd.Timedelta('1 hour')


def GetTotalResults(results):
    length = len(results)
    total = 0
    for res in results:
        total += res;

    return total/length;


def main():
    slave_workers = size - 1
    rows = GetNumberOfRows()
    chunk_size = int(rows / slave_workers)
    print('using MPI....')

    start_time = time.time()
    chunk_distribution = distribute_rows(n_rows=chunk_size, n_processes=slave_workers)
    distributeTasks(chunk_distribution, size)
    results = GetResults()

    print(f'Average flight between Nashville and chicago = {GetTotalResults(results)}')
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
        result = GetAverageFlightTime(chunk)
        print(f'Worker slave {rank} is done. Sending back to master')
        comm.send(result, dest=0)
