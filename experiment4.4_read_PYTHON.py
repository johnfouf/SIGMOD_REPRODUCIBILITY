from pyimplementation.fastparquet import ParquetFile
pf = ParquetFile('citations.parq')

import timeit
print('Filter parquet file, time:',timeit.timeit('''
df3 = pf.to_pandas(['c1'], filters=[('c1', '=', '10.1002/9783527644506.refs')])
newdf = df3[(df3.c1 == "10.1002/9783527644506.refs")]
print('rows found: ', len(newdf.index))
''', globals=globals(), number=1))


pf = ParquetFile('citations.diffparq')
print('Filter adaptive file, time:',timeit.timeit('''
df3 = pf.to_pandas(['c1'], filters=[('c1', '=', '10.1002/9783527644506.refs')])
newdf = df3[(df3.c1 == "10.1002/9783527644506.refs")]
print('rows found: ', len(newdf.index))
''', globals=globals(), number=1))
