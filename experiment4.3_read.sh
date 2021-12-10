echo 3 | tee /proc/sys/vm/drop_caches
echo "value 98.6.98.djf differential dictionaries zonemaps enabled"
time ./read ips.diff 0 "98.6.98.djf" 1 1
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 98.6.98.djf differential dictionaries zonemaps disabled"
time ./read ips.diff 0 "98.6.98.djf" 0 0
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 98.6.98.djf global dictionaries zonemaps enabled"
time ./read ips.unorderedglob 0 "98.6.98.djf" 1 1
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 98.6.98.djf global dictionaries zonemaps disabled"
time ./read ips.unorderedglob 0 "98.6.98.djf" 0 0
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 98.6.98.djf local dictionaries zonemaps enabled"
time ./read ips.localdiff 0 "98.6.98.djf" 1 1
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 98.6.98.djf local dictionaries zonemaps disabled"
time ./read ips.localdiff 0 "98.6.98.djf" 0 0
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 101.81.231.jde differential dictionaries zonemaps enabled"
time ./read ips.diff 0 "101.81.231.jde" 1 1
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 101.81.231.jde differential dictionaries zonemaps disabled"
time ./read ips.diff 0 "101.81.231.jde" 0 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "value 101.81.231.jde global dictionaries zonemaps enabled"
time ./read ips.unorderedglob 0 "101.81.231.jde" 1 1
echo 3 | tee /proc/sys/vm/drop_caches
echo "value 101.81.231.jde global dictionaries zonemaps disabled"
time ./read ips.unorderedglob 0 "101.81.231.jde" 0 0
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 101.81.231.jde local dictionaries zonemaps enabled"
time ./read ips.localdiff 0 "101.81.231.jde" 1 1
echo 3 |  tee /proc/sys/vm/drop_caches
echo "value 101.81.231.jde local dictionaries zonemaps disabled"
time ./read ips.localdiff 0 "101.81.231.jde" 0 0
