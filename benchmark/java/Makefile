default: plio

plio: \
	edu/cuhk/cse/plio/Protocol.class \
	edu/cuhk/cse/plio/PLIO.class \
	edu/cuhk/cse/plio/Main.class

%.class: %.java
	javac -cp . $<

run:
	java -cp . edu.cuhk.cse.plio.Main 255 4096 127.0.0.1 10091 console

clean:
	rm -f edu/cuhk/cse/plio/*.class
