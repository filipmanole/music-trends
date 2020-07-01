SRC = src/*.java src/util/*.java
OUT = out

SCALA_LIBRARY=$(SPARK_HOME)/jars/scala-library-2.11.12.jar
SPARK_CORE=$(SPARK_HOME)/jars/spark-core_2.11-2.4.4.jar
HADOOP_COMMON=$(SPARK_HOME)/jars/hadoop-common-2.7.3.jar
ASCII_RENDER=/home/spuser/DCC-spark/plot-jar/ascii-render-2.1.3.jar
ASCII_RENDER_API=/home/spuser/DCC-spark/plot-jar/ascii-render-api-2.1.3.jar
SRC_FILES=~/DCC-spark/src

DRIVER_MEMORY=2g
CLASS_PATH=$(SCALA_LIBRARY):$(SPARK_CORE):$(HADOOP_COMMON):$(SRC_FILES):$(ASCII_RENDER):$(ASCII_RENDER_API)
EXTERNAL_JARS=$(ASCII_RENDER),$(ASCII_RENDER_API)
MASTER_URL=spark://ip-172-31-22-14.us-east-2.compute.internal:7077
WORD_COUNT=JavaWordCount

build: $(SRC)
	mkdir -p $(OUT)
	javac -Xdiags:verbose -classpath $(CLASS_PATH) -d $(OUT) $(SRC)
	jar -cvf app.jar -C $(OUT) .

word-count:
	spark-submit --driver-memory=$(DRIVER_MEMORY) --class $(WORD_COUNT) --master $(MASTER_URL) app.jar $(FILE)

run:
	spark-submit --driver-memory=$(DRIVER_MEMORY) --class $(TASK) --master $(MASTER_URL) --jars $(EXTERNAL_JARS) app.jar $(FILE)

clean:
	rm -fr $(OUT) *.jar
