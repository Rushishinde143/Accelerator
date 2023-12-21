from pyspark import SparkContext

sc = SparkContext("local", "WordCount")

text_file = sc.textFile("path_to_text_file.txt")

words = text_file.flatMap(lambda line: line.split(" "))

word_counts = words.map(lambda word: (word, 1))

word_counts = word_counts.reduceByKey(lambda a, b: a + b)

for word, count in word_counts.collect():
    print(f"{word}: {count}")

sc.stop()
