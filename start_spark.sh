rm -r /opt/hadoop/home/hadoop2/data/output/*
rm -r /opt/hadoop/home/hadoop2/data/output_kml/*
#cd CS226_code
mvn clean
mvn assembly:assembly
spark-submit --class CS226 --master local[8] --executor-memory 100G target/test-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/hadoop/home/hadoop2/data/roads_sample 2
python KML.py
