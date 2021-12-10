echo 3 | tee /proc/sys/vm/drop_caches
time ./read citations.diff 0 "10.1002/9783527644506.refs" 1 1
echo 3 | tee /proc/sys/vm/drop_caches
time ./read citations.lcdiff 0 "10.1002/9783527644506.refs" 1 1
