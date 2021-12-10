from pyimplementation.pyarcade import write
import pyimplementation.global_encoding as global_encoding
import pandas as pd


import pandas as pd
df = pd.read_csv("reprod_datasets/edgar/ips.csv") # edit with csv input file path
write("ips.diff", df, row_group_offsets=65535, format="ADAPTIVE") 
write("ips.localdiff", df, row_group_offsets=65535, format="LOCAL")
global_encoding.write_global(df,"ips.unorderedglob",0,65535)




