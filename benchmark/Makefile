

default: plio

plio: \
	src/edu/cuhk/cse/plio/Protocol.class \
	src/edu/cuhk/cse/plio/PLIO.class \
	src/edu/cuhk/cse/plio/Main.class \
	build/plio.jar

%.class: %.java
	javac -cp src $<

build/plio.jar: \
	src/edu/cuhk/cse/plio/Protocol.class \
	src/edu/cuhk/cse/plio/Protocol$$Header.class \
	src/edu/cuhk/cse/plio/Protocol$$KeyHeader.class \
	src/edu/cuhk/cse/plio/Protocol$$KeyValueHeader.class \
	src/edu/cuhk/cse/plio/Protocol$$KeyValueUpdateHeader.class \
	src/edu/cuhk/cse/plio/PLIO.class
	cd src && jar cf ../build/plio.jar \
		edu/cuhk/cse/plio/Protocol*.class \
		edu/cuhk/cse/plio/PLIO.class

ycsb: PLIOClient

%: src/com/yahoo/src/db/%.java
	javac -cp build/plio.jar:~/Development/src/ycsb-0.3.0/lib/* $<

run:
	java -cp ycsb edu.cuhk.cse.plio.Main 255 4096 127.0.0.1 10091 console

clean:
	rm -f src/edu/cuhk/cse/plio/*.class build/plio.jar
