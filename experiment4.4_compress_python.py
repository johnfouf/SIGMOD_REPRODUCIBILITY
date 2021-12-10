from pyimplementation.fastparquet import write
import pandas as pd
import timeit
print('Compressing using parquet techniques at: ',timeit.timeit('''
df = pd.read_csv("/home/openaire/arc/arcade/src/citationspart.0.csv", usecols = ['c1'])
write("citations.parq", df, row_group_offsets=65535)
''', globals=globals(), number=1))
print('Compressing using adaptive dictionaries at: ',timeit.timeit('''
df1 = pd.read_csv("/home/openaire/arc/arcade/src/citationspart.0.csv", usecols = ['c1'])
write("citations.diffparq", df1, row_group_offsets=65535, format = "ADAPTIVE")
''', globals=globals(), number=1))

