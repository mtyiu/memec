all:
	javac -cp . edu/cuhk/cse/memec/*.java

run:
	java -cp . edu.cuhk.cse.memec.Main 255 4096 127.0.0.1 10091 50000 10 100000 false

ncs:
	java -cp . edu.cuhk.cse.memec.Main 255 4096 192.168.0.19 9112 100000 1000 500000 false

hpc:
	java -cp . edu.cuhk.cse.memec.Main 255 4096 137.189.88.46 9112 100000 10 500000 false

clean:
	rm -f edu/cuhk/cse/memec/*.class
