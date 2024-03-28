import polars as pl
import uuid
import random
from time import perf_counter


def run(dict_size, ops_count):
    data = {str(uuid.uuid4()): random.randrange(100000) for _ in range(0, dict_size)}

    time_start = perf_counter()
    df = pl.DataFrame({'keys': data.keys(), 'values': data.values()}).lazy()
    for _ in range(0, ops_count):
        x = df.with_columns((pl.col('values') + 4))
        x = df.join(x, on='keys').with_columns((pl.col('values') + pl.col('values_right'))).drop("values_right")
        x = df.with_columns((pl.col('values') - 4))
        x = df.join(x, on='keys').with_columns((pl.col('values') - pl.col('values_right'))).drop("values_right")
        x = df.with_columns((pl.col('values') * 4))
        x.collect()
    time_duration = perf_counter() - time_start

    print(f"Polars {ops_count} X 5 Element-wise ops on collection of {dict_size}:")
    print(f"  {time_duration:.3f} seconds")


if __name__ == "__main__":
    run(10, 100000)
    run(10, 1000000)
    run(1000, 10000)
    run(1000, 100000)
