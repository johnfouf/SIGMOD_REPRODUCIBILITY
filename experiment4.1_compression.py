from pyimplementation.pyarcade import write
import pyimplementation.global_encoding as global_encoding
import pandas as pd

df = pd.read_csv("/home/openaire/phd/datasets/zipf/affszipf13random.csv") # edit with csv input file path

import timeit
print('Adaptive random zipf 1.3, time:', timeit.timeit('write("zipf13rand.diff", df, row_group_offsets=65535, format="ADAPTIVE")', globals=globals(), number=1) )
print('Local random zipf 1.3, time:',timeit.timeit('write("zipf13rand.localdiff", df, row_group_offsets=65535, format="LOCAL")', globals=globals(), number=1))
print('Global random zipf 1.3, time:',timeit.timeit('global_encoding.write_global(df,"zipf13rand.unorderedglob",0,65535)', globals=globals(), number=1))
print('Indirect random zipf 1.3, time:',timeit.timeit('global_encoding.write_indirect(df,"zipf13rand.unindirect",0,65535)', globals=globals(), number=1))

df.sort_values(by=['c'],inplace=True)
df = df.reset_index(drop=True)
print('Adaptive sorted zipf 1.3, time:',timeit.timeit('write("zipf13sorted.diff", df, row_group_offsets=65535, format="ADAPTIVE")', globals=globals(), number=1))
print('Local sorted zipf 1.3, time:',timeit.timeit('write("zipf13sorted.localdiff", df, row_group_offsets=65535, format="LOCAL")', globals=globals(), number=1))
print('Global sorted zipf 1.3, time:',timeit.timeit('global_encoding.write_global(df,"zipf13sorted.unorderedglob",0,65535)', globals=globals(), number=1))
print('Indirect sorted zipf 1.3, time:',timeit.timeit('global_encoding.write_indirect(df,"zipf13sorted.unindirect",0,65535)', globals=globals(), number=1))

df = pd.read_csv("/home/openaire/phd/datasets/zipf/affszipf07random.csv") # edit with csv input file path
print('Adaptive random zipf 0.7, time:',timeit.timeit('write("zipf07rand.diff", df, row_group_offsets=65535, format="ADAPTIVE")', globals=globals(), number=1))
print('Local random zipf 0.7, time:',timeit.timeit('write("zipf07rand.localdiff", df, row_group_offsets=65535, format="LOCAL")', globals=globals(), number=1))
print('Global random zipf 0.7, time:',timeit.timeit('global_encoding.write_global(df,"zipf07rand.unorderedglob",0,65535)', globals=globals(), number=1))
print('Indirect random zipf 0.7, time:',timeit.timeit('global_encoding.write_indirect(df,"zipf07rand.unindirect",0,65535)', globals=globals(), number=1))

df.sort_values(by=['c'],inplace=True)
df = df.reset_index(drop=True)
print('Adaptive sorted zipf 0.7, time:',timeit.timeit('write("zipf07sorted.diff", df, row_group_offsets=65535, format="ADAPTIVE")', globals=globals(), number=1))
print('Local sorted zipf 0.7, time:',timeit.timeit('write("zipf07sorted.localdiff", df, row_group_offsets=65535, format="LOCAL")', globals=globals(), number=1))
print('Global sorted zipf 0.7, time:',timeit.timeit('global_encoding.write_global(df,"zipf07sorted.unorderedglob",0,65535)', globals=globals(), number=1))
print('Indirect sorted zipf 0.7, time:',timeit.timeit('global_encoding.write_indirect(df,"zipf07sorted.unindirect",0,65535)', globals=globals(), number=1))



df = pd.read_csv("/home/openaire/phd/datasets/zipf/affszipf0random.csv") # edit with csv input file path
print('Adaptive random zipf 0, time:',timeit.timeit('write("zipf0rand.diff", df, row_group_offsets=65535, format="ADAPTIVE")', globals=globals(), number=1))
print('Local random zipf 0, time:',timeit.timeit('write("zipf0rand.localdiff", df, row_group_offsets=65535, format="LOCAL")', globals=globals(), number=1))
print('Global random zipf 0, time:',timeit.timeit('global_encoding.write_global(df,"zipf0rand.unorderedglob",0,65535)', globals=globals(), number=1))
print('Indirect random zipf 0, time:',timeit.timeit('global_encoding.write_indirect(df,"zipf0rand.unindirect",0,65535)', globals=globals(), number=1))

df.sort_values(by=['c'],inplace=True)
df = df.reset_index(drop=True)
print('Adaptive sorted zipf 0, time:',timeit.timeit('write("zipf0sorted.diff", df, row_group_offsets=65535, format="ADAPTIVE")', globals=globals(), number=1))
print('Local sorted zipf 0, time:',timeit.timeit('write("zipf0sorted.localdiff", df, row_group_offsets=65535, format="LOCAL")', globals=globals(), number=1))
print('Global sorted zipf 0, time:',timeit.timeit('global_encoding.write_global(df,"zipf0sorted.unorderedglob",0,65535)', globals=globals(), number=1))
print('Indirect sorted zipf 0, time:',timeit.timeit('global_encoding.write_indirect(df,"zipf0sorted.unindirect",0,65535)', globals=globals(), number=1))



