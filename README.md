<img src="http://spark.apache.org/images/spark-logo-trademark.png" alt="Spark Logo" height="200"/>

# spark-scala-word-processing

This repo is an example of Spark Word processing with Scala.

## Input files

The poeme.txt is a 2978 line-long file separated into sections. It represents a foreign poem translated into French.

The common_words_subset.txt are the most common French words (1,102 words).

## Code and Results

### Word count

This goal was to count all words from a file :

```scala
%spark
val textFile = sc.textFile("hdfs_path_to_poeme.txt")
val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
counts.saveAsTextFile("ouputfile")
```

Here are some lines of the ouput (`Pair <String, Integer>`)  : 

```
(revêtus,1)
(francs,1)
(souvent,8)
(épais,5)
(derniers,3)
(voile,6)
(dois-je,1)
(collines;,1)
(Remplit,1)
(l'aigle,1)
(d'ailes,1)
(Verse,1)
(Verdissent,1)
(frappait,1)
(Viennent,2)
(saisie,1)
(guider,2)
(tristesse!,1)
(demeure;,1)
(Dans,6)
(distrait,1)
(gentille:,1)
```

### Word Count Line

This code will print the line with the most words :

```scala
%spark
val textFile = sc.textFile("hdfs_path_to_poeme.txt")

val reg = "\\s+".r

val counts = textFile.flatMap(line => line.split("\n"))
                .map(line => (reg.findAllIn(line).length+1, line))
                .max()

val rdd = sqlContext.sparkContext.parallelize(Seq(counts))

rdd.saveAsTextFile("outputfile")
```

Here is the result : 

```
12,--«Je sais fort peu de chose et fais mieux de me taire)
```
The line with the most words is 12 word-long.

### Word Anagrams

This code will process every word to list its anagrams inside the file.

To do this, letters from words are ordered alphabetically and put in as the key.

After, words with the same key are concatenated.

```scala
%spark 
val textFile = sc.textFile("hdfs_path_to_common_words_subset.txt")

val counts = textFile.flatMap(line => line.split("\n"))
            .map(word => (
                word.toLowerCase().toCharArray().sortWith(_ < _).mkString,
                word.toLowerCase()))
            .reduceByKey(_ +"|"+ _)

counts.saveAsTextFile("outputfile")
```

Results are styled as `Pair<Text, Text>` with the second is "word1|word2|word3" :

```
(eeiqssuuv,visqueuse)
(aeeimnstt,estaminet|tantiemes)
(acceeirst,circaetes)
(aaeeillst,allaitees)
(deegirruv,degivreur)
(egiilnrss,rieslings)
(eeimrs,misere|remise|rimees)
(adenort,tornade|erodant)
(einrtux,nitreux)
(eeegir,egerie|erigee)
(aabeeilln,alienable)
(ceeegnors,congreees)
(adeeloprs,leopardes)
(aeglnos,losange|solange)
(aiiopptt,pipotait)
(ademoorst,moderatos)
( aabcilooppss,pablo picasso|pascal obispo)
```


