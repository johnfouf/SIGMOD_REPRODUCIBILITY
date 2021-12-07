CPPFLAGS = -O3  -std=c++20   -fcoroutines -lz -lsnappy -freorder-blocks-algorithm=simple -fPIC


%.o: %.cpp
	$(CXX) -c -o $@ $< $(CPPFLAGS) 

libarcade.a: reader.o cache.o process.o writer.o
	ar rcs libarcade.a reader.o cache.o process.o writer.o

depend:
	git clone https://github.com/google/snappy.git
	cd snappy; git submodule update --init; mkdir build; cd build && cmake ../ && make
	
runner:  ./libarcade.a runner.cpp
	$(CXX)   -O3 -std=c++20 -fcoroutines -freorder-blocks-algorithm=simple -o runner runner.cpp -L. -larcade -lsnappy -lz 

clean:  reader.o cache.o process.o writer.o libarcade.a
	rm -f *.o
