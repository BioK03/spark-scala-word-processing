/* Word Count */

%spark
val textFile = sc.textFile("hdfs_path_to_poeme.txt")
val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
counts.saveAsTextFile("ouputfile")

/* Word Count Line */

%spark
val textFile = sc.textFile("hdfs_path_to_poeme.txt")

val reg = "\\s+".r

val counts = textFile.flatMap(line => line.split("\n"))
                .map(line => (reg.findAllIn(line).length+1, line))
                .max()

val rdd = sqlContext.sparkContext.parallelize(Seq(counts))

rdd.saveAsTextFile("outputfile")

/* Word Anagrams */
%spark 
val textFile = sc.textFile("hdfs_path_to_common_words_subset.txt")

val counts = textFile.flatMap(line => line.split("\n"))
            .map(word => (
                word.toLowerCase().toCharArray().sortWith(_ < _).mkString,
                word.toLowerCase()))
            .reduceByKey(_ +"|"+ _)

counts.saveAsTextFile("outputfile")