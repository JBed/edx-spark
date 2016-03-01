
wordsList = ['cat', 'elephant', 'rat', 'rat', 'cat']
wordsRDD = sc.parallelize(wordsList, 4)
print type(wordsRDD)


def makePlural(word):
    """Adds an 's' to `word`.

    Note:
        This is a simple function that only adds an 's'.  No attempt is made to follow proper
        pluralization rules.

    Args:
        word (str): A string.

    Returns:
        str: A string with 's' added to it.
    """
    return word + 's'

print makePlural('cat')



def makePlural(word):
    return word + 's'

print makePlural('cat')


from test_helper import Test
Test.assertEquals(makePlural('rat'), 'rats', 'incorrect result: makePlural does not add an s')



pluralRDD = wordsRDD.map(makePlural)
print pluralRDD.collect()


Test.assertEquals(pluralRDD.collect(), ['cats', 'elephants', 'rats', 'rats', 'cats'], 'incorrect values for pluralRDD')


pluralLambdaRDD = wordsRDD.map(lambda x: x + 's')
print pluralLambdaRDD.collect()


Test.assertEquals(pluralLambdaRDD.collect(), ['cats', 'elephants', 'rats', 'rats', 'cats'], 'incorrect values for pluralLambdaRDD (1d)')



pluralLengths = (pluralRDD
                 .map(len)
                 .collect())

print pluralLengths


Test.assertEquals(pluralLengths, [4, 9, 4, 4, 4],
                  'incorrect values for pluralLengths')



wordPairs = wordsRDD.map(lambda x: (x,1))
print wordPairs.collect()



Test.assertEquals(wordPairs.collect(),
                  [('cat', 1), ('elephant', 1), ('rat', 1), ('rat', 1), ('cat', 1)],
                  'incorrect value for wordPairs')


wordsGrouped = wordPairs.groupByKey()
for key, value in wordsGrouped.collect():
    print '{0}: {1}'.format(key, list(value))



Test.assertEquals(sorted(wordsGrouped.mapValues(lambda x: list(x)).collect()),
                  [('cat', [1, 1]), ('elephant', [1]), ('rat', [1, 1])],
                  'incorrect value for wordsGrouped')



wordCountsGrouped = wordsGrouped.map(lambda (k, v): (k, sum(v)))
print wordCountsGrouped.collect()


Test.assertEquals(sorted(wordCountsGrouped.collect()),
                  [('cat', 2), ('elephant', 1), ('rat', 2)],
                  'incorrect value for wordCountsGrouped')



wordCounts = wordPairs.reduceByKey(lambda x, y: x+ y)
print wordCounts.collect()



Test.assertEquals(sorted(wordCounts.collect()), [('cat', 2), ('elephant', 1), ('rat', 2)],
                  'incorrect value for wordCounts')



wordCountsCollected = (wordsRDD
                       .map(lambda x: (x,1))
                       .reduceByKey(lambda x, y: x+ y)
                       .collect())
print wordCountsCollected
print wordsRDD



Test.assertEquals(sorted(wordCountsCollected), [('cat', 2), ('elephant', 1), ('rat', 2)],
                  'incorrect value for wordCountsCollected')


uniqueWords =  wordsRDD.distinct().count()
print uniqueWords


# In[36]:

# TEST Unique words (3a)
Test.assertEquals(uniqueWords, 3, 'incorrect count of uniqueWords')


# #### ** (3b) Mean using `reduce` **
# #### Find the mean number of words per unique word in `wordCounts`.
# #### Use a `reduce()` action to sum the counts in `wordCounts` and then divide by the number of unique words.  First `map()` the pair RDD `wordCounts`, which consists of (key, value) pairs, to an RDD of values.

# In[57]:

# TODO: Replace <FILL IN> with appropriate code
from operator import add
totalCount = (wordCounts
            .map(lambda (key, val): val)
             .reduce(add))
average = totalCount / float(uniqueWords)
print totalCount
#print round(average, 2)


# In[58]:

# TEST Mean using reduce (3b)
Test.assertEquals(round(average, 2), 1.67, 'incorrect value of average')


# ### ** Part 4: Apply word count to a file **

# #### In this section we will finish developing our word count application.  We'll have to build the `wordCount` function, deal with real world problems like capitalization and punctuation, load in our data source, and compute the word count on the new data.

# #### ** (4a) `wordCount` function **
# #### First, define a function for word counting.  You should reuse the techniques that have been covered in earlier parts of this lab.  This function should take in an RDD that is a list of words like `wordsRDD` and return a pair RDD that has all of the words and their associated counts.

# In[66]:

# TODO: Replace <FILL IN> with appropriate code
def wordCount(wordListRDD):
    """Creates a pair RDD with word counts from an RDD of words.

    Args:
        wordListRDD (RDD of str): An RDD consisting of words.

    Returns:
        RDD of (str, int): An RDD consisting of (word, count) tuples.
    """
    return (wordListRDD.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y))
    
print wordCount(wordsRDD).collect()


# In[67]:

# TEST wordCount function (4a)
Test.assertEquals(sorted(wordCount(wordsRDD).collect()),
                  [('cat', 2), ('elephant', 1), ('rat', 2)],
                  'incorrect definition for wordCount function')




import re
def removePunctuation(text):
    """Removes punctuation, changes to lowercase, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated. (e.g. it's becomes its)

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
    """
    return re.sub(r'([^A-Za-z0-9\s+])', '', text.strip().lower())

print removePunctuation("The Elephant's 4 cats. ")


Test.assertEquals(removePunctuation(" The Elephant's 4 cats. "),
                  'the elephants 4 cats',
                  'incorrect definition for removePunctuation function')



import os.path
baseDir = os.path.join('data')
inputPath = os.path.join('cs100', 'lab1', 'shakespeare.txt')
fileName = os.path.join(baseDir, inputPath)

shakespeareRDD = (sc
                  .textFile(fileName, 8)
                  .map(removePunctuation))

print '\n'.join(shakespeareRDD
                .zipWithIndex()
                .map(lambda (l, num): '{0}: {1}'.format(num, l))
                .take(15))



shakespeareWordsRDD = shakespeareRDD.flatMap(lambda x: x.split(' '))
shakespeareWordCount = shakespeareWordsRDD.count()
print shakespeareWordsRDD.top(5)
print shakespeareWordCount



Test.assertEquals(shakespeareWordCount, 928908, 'incorrect value for shakespeareWordCount')
Test.assertEquals(shakespeareWordsRDD.top(5),
                  [u'zwaggerd', u'zounds', u'zounds', u'zounds', u'zounds'],
                  'incorrect value for shakespeareWordsRDD')



shakeWordsRDD = shakespeareWordsRDD.filter(lambda x: x != '')
shakeWordCount = shakeWordsRDD.count()
print shakeWordCount



Test.assertEquals(shakeWordCount, 882996, 'incorrect value for shakeWordCount')



top15WordsAndCounts = wordCount(shakeWordsRDD).takeOrdered(15, lambda (k,v): -v)
print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top15WordsAndCounts))



Test.assertEquals(top15WordsAndCounts,
                  [(u'the', 27361), (u'and', 26028), (u'i', 20681), (u'to', 19150), (u'of', 17463),
                   (u'a', 14593), (u'you', 13615), (u'my', 12481), (u'in', 10956), (u'that', 10890),
                   (u'is', 9134), (u'not', 8497), (u'with', 7771), (u'me', 7769), (u'it', 7678)],
                  'incorrect value for top15WordsAndCounts')


