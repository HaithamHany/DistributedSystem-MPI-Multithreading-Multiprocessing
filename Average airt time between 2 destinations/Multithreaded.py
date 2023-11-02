import datetime
import math
import time
import pandas as pd
from multiprocessing.pool import ThreadPool


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
    thread_pool_size = 10
    thread_pool = ThreadPool(thread_pool_size)

    rows = GetNumberOfRows()
    chunk_size = int(rows / thread_pool_size);
    print('started multithreading....')
    start_time = time.time()
    # Split chunk evenly by the pool size
    result = thread_pool.map(GetAverageFlightTime, distribute_rows(n_rows=chunk_size, n_processes=thread_pool_size))
    print(f'Average flight between Nashville and chicago = {GetTotalResults(result)}')

    thread_pool.close()
    thread_pool.join()
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
