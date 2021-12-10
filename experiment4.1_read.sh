echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 1.3 random filter execution: "
time ./read zipf13rand.diff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 1.3 random filter execution: "
time ./read zipf13rand.localdiff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 1.3 random filter execution: "
time ./read zipf13rand.unorderedglob 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 1.3 random filter execution: "
time ./read zipf13rand.unindirect 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0.7 random filter execution: "
time ./read zipf07rand.diff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0.7 random filter execution: "
time ./read zipf07rand.localdiff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0.7 random filter execution: "
time ./read zipf07rand.unorderedglob 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0.7 random filter execution: "
time ./read zipf07rand.unindirect 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0 random filter execution: "
time ./read zipf0rand.diff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0 random filter execution: "
time ./read zipf0rand.localdiff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0 random filter execution: "
time ./read zipf0rand.unorderedglob 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0 random filter execution: "
time ./read zipf0rand.unindirect 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 1.3 sorted filter execution: "
time ./read zipf13sorted.diff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 1.3 sorted filter execution: "
time ./read zipf13sorted.localdiff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 1.3 sorted filter execution: "
time ./read zipf13sorted.unorderedglob 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 1.3 sorted filter execution: "
time ./read zipf13sorted.unindirect 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0.7 sorted filter execution: "
time ./read zipf07sorted.diff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0.7 sorted filter execution: "
time ./read zipf07sorted.localdiff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0.7 sorted filter execution: "
time ./read zipf07sorted.unorderedglob 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0.7 sorted filter execution: "
time ./read zipf07sorted.unindirect 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0 sorted filter execution: "
time ./read zipf0sorted.diff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0 sorted filter execution: "
time ./read zipf0sorted.localdiff 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0 sorted filter execution: "
time ./read zipf0sorted.unorderedglob 0 "kuban state agrarian university" 1 0
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0 sorted filter execution: "
time ./read zipf0sorted.unindirect 0 "kuban state agrarian university" 1 0


echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 1.3 random full scan execution: "
time ./read zipf13rand.diff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 1.3 random full scan execution: "
time ./read zipf13rand.localdiff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 1.3 random full scan execution: "
time ./read zipf13rand.unorderedglob 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 1.3 random full scan execution: "
time ./read zipf13rand.unindirect 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0.7 random full scan execution: "
time ./read zipf07rand.diff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0.7 random full scan execution: "
time ./read zipf07rand.localdiff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0.7 random full scan execution: "
time ./read zipf07rand.unorderedglob 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0.7 random full scan execution: "
time ./read zipf07rand.unindirect 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0 random full scan execution: "
time ./read zipf0rand.diff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0 random full scan execution: "
time ./read zipf0rand.localdiff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0 random full scan execution: "
time ./read zipf0rand.unorderedglob 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0 random full scan execution: "
time ./read zipf0rand.unindirect 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 1.3 sorted full scan execution: "
time ./read zipf13sorted.diff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 1.3 sorted full scan execution: "
time ./read zipf13sorted.localdiff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 1.3 sorted full scan execution: "
time ./read zipf13sorted.unorderedglob 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 1.3 sorted full scan execution: "
time ./read zipf13sorted.unindirect 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0.7 sorted full scan execution: "
time ./read zipf07sorted.diff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0.7 sorted full scan execution: "
time ./read zipf07sorted.localdiff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0.7 sorted full scan execution: "
time ./read zipf07sorted.unorderedglob 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0.7 sorted full scan execution: "
time ./read zipf07sorted.unindirect 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0 sorted full scan execution: "
time ./read zipf0sorted.diff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0 sorted full scan execution: "
time ./read zipf0sorted.localdiff 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0 sorted full scan execution: "
time ./read zipf0sorted.unorderedglob 0 
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0 sorted full scan execution: "
time ./read zipf0sorted.unindirect 0 


echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 1.3 random - random access execution: "
time ./read zipf13rand.diff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 1.3 random - random access execution: "
time ./read zipf13rand.localdiff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 1.3 random - random access execution: "
time ./read zipf13rand.unorderedglob 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 1.3 random - random access execution: "
time ./read zipf13rand.unindirect 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0.7 random - random access execution: "
time ./read zipf07rand.diff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0.7 random - random access execution: "
time ./read zipf07rand.localdiff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0.7 random - random access execution: "
time ./read zipf07rand.unorderedglob 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0.7 random - random access execution: "
time ./read zipf07rand.unindirect 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0 random - random access execution: "
time ./read zipf0rand.diff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0 random - random access execution: "
time ./read zipf0rand.localdiff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0 random - random access execution: "
time ./read zipf0rand.unorderedglob 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0 random - random access execution: "
time ./read zipf0rand.unindirect 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 1.3 sorted - random access execution: "
time ./read zipf13sorted.diff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 1.3 sorted - random access execution: "
time ./read zipf13sorted.localdiff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 1.3 sorted - random access execution: "
time ./read zipf13sorted.unorderedglob 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 1.3 sorted - random access execution: "
time ./read zipf13sorted.unindirect 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0.7 sorted - random access execution: "
time ./read zipf07sorted.diff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0.7 sorted - random access execution: "
time ./read zipf07sorted.localdiff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0.7 sorted - random access execution: "
time ./read zipf07sorted.unorderedglob 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0.7 sorted - random access execution: "
time ./read zipf07sorted.unindirect 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "ADAPTIVE zipf 0 sorted - random access execution: "
time ./read zipf0sorted.diff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "LOCAL zipf 0 sorted - random access execution: "
time ./read zipf0sorted.localdiff 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "GLOBAL zipf 0 sorted - random access execution: "
time ./read zipf0sorted.unorderedglob 0 60000000
echo 3 | tee /proc/sys/vm/drop_caches
echo "INDIRECT zipf 0 sorted - random access execution: "
time ./read zipf0sorted.unindirect 0 60000000
