import sys
import os
from test_helper import Test

baseDir = os.path.join('data')
inputPath = os.path.join('cs100', 'lab4', 'small')
ratingsFilename = os.path.join(baseDir, inputPath, 'ratings.dat.gz')
moviesFilename = os.path.join(baseDir, inputPath, 'movies.dat')

numPartitions = 2
rawRatings = sc.textFile(ratingsFilename).repartition(numPartitions)
rawMovies = sc.textFile(moviesFilename)

def get_ratings_tuple(entry):
    """ Parse a line in the ratings dataset
    Args:
        entry (str): a line in the ratings dataset in the form of UserID::MovieID::Rating::Timestamp
    Returns:
        tuple: (UserID, MovieID, Rating)
    """
    items = entry.split('::')
    return int(items[0]), int(items[1]), float(items[2])


def get_movie_tuple(entry):
    """ Parse a line in the movies dataset
    Args:
        entry (str): a line in the movies dataset in the form of MovieID::Title::Genres
    Returns:
        tuple: (MovieID, Title)
    """
    items = entry.split('::')
    return int(items[0]), items[1]


ratingsRDD = rawRatings.map(get_ratings_tuple).cache()
moviesRDD = rawMovies.map(get_movie_tuple).cache()

ratingsCount = ratingsRDD.count()
moviesCount = moviesRDD.count()

print 'There are {ratings} ratings and {movies} movies in the datasets'.format(ratings=ratingsCount, movies=moviesCount)
print 'Ratings: {}'.format(ratingsRDD.take(3))
print 'Movies: {}'.format(moviesRDD.take(3))

assert ratingsCount == 487650
assert moviesCount == 3883
assert moviesRDD.filter(lambda (id, title): title == 'Toy Story (1995)').count() == 1
assert (ratingsRDD.takeOrdered(1, key=lambda (user, movie, rating): movie) == [(1, 1, 5.0)])


tmp1 = [(1, u'alpha'), (2, u'alpha'), (2, u'beta'), (3, u'alpha'), (1, u'epsilon'), (1, u'delta')]
tmp2 = [(1, u'delta'), (2, u'alpha'), (2, u'beta'), (3, u'alpha'), (1, u'epsilon'), (1, u'alpha')]

oneRDD = sc.parallelize(tmp1)
twoRDD = sc.parallelize(tmp2)
oneSorted = oneRDD.sortByKey(True).collect()
twoSorted = twoRDD.sortByKey(True).collect()
print oneSorted
print twoSorted
assert set(oneSorted) == set(twoSorted)
assert twoSorted[0][0] < twoSorted.pop()[0]]
assert oneSorted[0:2] != twoSorted[0:2]


def sortFunction(tuple):
    """ Construct the sort string (does not perform actual sorting)
    Args:
        tuple: (rating, MovieName)
    Returns:
        sortString: the value to sort with, 'rating MovieName'
    """
    key = unicode('{}'.format(tuple[0]))
    value = tuple[1]
    return (key + ' ' + value)


print oneRDD.sortBy(sortFunction, True).collect()
print twoRDD.sortBy(sortFunction, True).collect()


oneSorted1 = oneRDD.takeOrdered(oneRDD.count(),key=sortFunction)
twoSorted1 = twoRDD.takeOrdered(twoRDD.count(),key=sortFunction)

assert oneSorted1 == twoSorted1

def getCountsAndAverages(IDandRatingsTuple):
    """ Calculate average rating
    Args:
        IDandRatingsTuple: a single tuple of (MovieID, (Rating1, Rating2, Rating3, ...))
    Returns:
        tuple: a tuple of (MovieID, (number of ratings, averageRating))
    """
    tup = len(IDandRatingsTuple[1])
    return (IDandRatingsTuple[0], (tup,sum(IDandRatingsTuple[1])/float((tup))))



Test.assertEquals(getCountsAndAverages((1, (1, 2, 3, 4))), (1, (4, 2.5)),
                            'incorrect getCountsAndAverages() with integer list')
Test.assertEquals(getCountsAndAverages((100, (10.0, 20.0, 30.0))), (100, (3, 20.0)),
                            'incorrect getCountsAndAverages() with float list')
Test.assertEquals(getCountsAndAverages((110, xrange(20))), (110, (20, 9.5)),
                            'incorrect getCountsAndAverages() with xrange')



def sortFunction(tuple):
    """ Construct the sort string (does not perform actual sorting)
    Args:
        tuple: (rating, MovieName)
    Returns:
        sortString: the value to sort with, 'rating MovieName'
    """
    key = unicode('{}'.format(tuple[0]))
    value = tuple[1]
    return (key + ' ' + value)


movieIDsWithRatingsRDD = (ratingsRDD.map(lambda x: (x[1],x[2]))).groupByKey()
print 'movieIDsWithRatingsRDD: {}\n'.format(movieIDsWithRatingsRDD.take(3))

movieIDsWithAvgRatingsRDD = movieIDsWithRatingsRDD.map(getCountsAndAverages)
print 'movieIDsWithAvgRatingsRDD: {}\n'.format(movieIDsWithAvgRatingsRDD.take(3))

movieNameWithAvgRatingsRDD = (moviesRDD.join(movieIDsWithAvgRatingsRDD)).map(lambda x: (x[1][1][1],x[1][0],x[1][1][0])).sortBy(lambda x: x[1])
print 'movieNameWithAvgRatingsRDD: {}\n'.format(movieNameWithAvgRatingsRDD.take(3))


print movieNameWithAvgRatingsRDD.takeOrdered(3)
Test.assertEquals(movieIDsWithRatingsRDD.count(), 3615,
                'incorrect movieIDsWithRatingsRDD.count() (expected 3615)')
movieIDsWithRatingsTakeOrdered = movieIDsWithRatingsRDD.takeOrdered(3)
Test.assertTrue(movieIDsWithRatingsTakeOrdered[0][0] == 1 and
                len(list(movieIDsWithRatingsTakeOrdered[0][1])) == 993,
                'incorrect count of ratings for movieIDsWithRatingsTakeOrdered[0] (expected 993)')
Test.assertTrue(movieIDsWithRatingsTakeOrdered[1][0] == 2 and
                len(list(movieIDsWithRatingsTakeOrdered[1][1])) == 332,
                'incorrect count of ratings for movieIDsWithRatingsTakeOrdered[1] (expected 332)')
Test.assertTrue(movieIDsWithRatingsTakeOrdered[2][0] == 3 and
                len(list(movieIDsWithRatingsTakeOrdered[2][1])) == 299,
                'incorrect count of ratings for movieIDsWithRatingsTakeOrdered[2] (expected 299)')

Test.assertEquals(movieIDsWithAvgRatingsRDD.count(), 3615,
                'incorrect movieIDsWithAvgRatingsRDD.count() (expected 3615)')
Test.assertEquals(movieIDsWithAvgRatingsRDD.takeOrdered(3),
                [(1, (993, 4.145015105740181)), (2, (332, 3.174698795180723)),
                 (3, (299, 3.0468227424749164))],
                'incorrect movieIDsWithAvgRatingsRDD.takeOrdered(3)')

Test.assertEquals(movieNameWithAvgRatingsRDD.count(), 3615,
                'incorrect movieNameWithAvgRatingsRDD.count() (expected 3615)')
Test.assertEquals(movieNameWithAvgRatingsRDD.takeOrdered(3),
                [(1.0, u'Autopsy (Macchie Solari) (1975)', 1), (1.0, u'Better Living (1998)', 1),
                 (1.0, u'Big Squeeze, The (1996)', 3)],
                 'incorrect movieNameWithAvgRatingsRDD.takeOrdered(3)')


movieLimitedAndSortedByRatingRDD = (movieNameWithAvgRatingsRDD
                                    .filter(lambda x: x[2] > 500)
                                    .sortBy(sortFunction, False))


print 'Movies with highest ratings: {}'.format(movieLimitedAndSortedByRatingRDD.take(20))

trainingRDD, validationRDD, testRDD = ratingsRDD.randomSplit([6, 2, 2], seed=0L)

print 'Training: {train}, validation: {validation}, test: {test}\n'.format(train=trainingRDD.count(),
                                                    validation=validationRDD.count(),
                                                    test=testRDD.count())


print trainingRDD.take(3)
print validationRDD.take(3)
print testRDD.take(3)

assert trainingRDD.count() == 292716
assert validationRDD.count() == 96902
assert testRDD.count() == 98032

assert trainingRDD.filter(lambda t: t == (1, 914, 3.0)).count() == 1
assert trainingRDD.filter(lambda t: t == (1, 2355, 5.0)).count() == 1
assert trainingRDD.filter(lambda t: t == (1, 595, 5.0)).count() == 1

assert validationRDD.filter(lambda t: t == (1, 1287, 5.0)).count() == 1
assert validationRDD.filter(lambda t: t == (1, 594, 4.0)).count() == 1
assert validationRDD.filter(lambda t: t == (1, 1270, 5.0)).count() == 1

assert testRDD.filter(lambda t: t == (1, 1193, 5.0)).count() == 1
assert testRDD.filter(lambda t: t == (1, 2398, 4.0)).count() == 1
assert testRDD.filter(lambda t: t == (1, 1035, 5.0)).count() == 1


import math
from operator import add
def computeError(predictedRDD, actualRDD):
    """ Compute the root mean squared error between predicted and actual
    Args:
        predictedRDD: predicted ratings for each movie and each user where each entry is in the form
                      (UserID, MovieID, Rating)
        actualRDD: actual ratings where each entry is in the form (UserID, MovieID, Rating)
    Returns:
        RSME (float): computed RSME value
    """
    # Transform predictedRDD into the tuples of the form ((UserID, MovieID), Rating)
    predictedReformattedRDD = predictedRDD.map(lambda x: ((x[0],x[1]),x[2]))

    # Transform actualRDD into the tuples of the form ((UserID, MovieID), Rating)
    actualReformattedRDD = actualRDD.map(lambda x: ((x[0],x[1]),x[2]))

    # Compute the squared error for each matching entry (i.e., the same (User ID, Movie ID) in each
    # RDD) in the reformatted RDDs using RDD transformtions - do not use collect()
    squaredErrorsRDD = predictedReformattedRDD.join(actualReformattedRDD).map( lambda x: (x[0],pow(float(x[1][0])-x[1][1],2)))
    
    # Compute the total squared error - do not use collect()
    totalError = squaredErrorsRDD.map(lambda x: x[1]).reduce(add)

    # Count the number of entries for which you computed the total squared error
    numRatings = squaredErrorsRDD.count()
   
    # Using the total squared error and the number of entries, compute the RSME
    return float(math.sqrt(totalError/float(numRatings)))



testPredicted = sc.parallelize([
    (1, 1, 5),
    (1, 2, 3),
    (1, 3, 4),
    (2, 1, 3),
    (2, 2, 2),
    (2, 3, 4)])
testActual = sc.parallelize([
     (1, 2, 3),
     (1, 3, 5),
     (2, 1, 5),
     (2, 2, 1)])
testPredicted2 = sc.parallelize([
     (2, 2, 5),
     (1, 2, 5)])

testError = computeError(testPredicted, testActual)

print 'Error for test dataset (should be 1.22474487139): {}'.format(testError)

testError2 = computeError(testPredicted2, testActual)
print 'Error for test dataset2 (should be 3.16227766017): {}'.format(testError2)

testError3 = computeError(testActual, testActual)
print 'Error for testActual dataset (should be 0.0): {}'.format(testError3)


Test.assertTrue(abs(testError - 1.22474487139) < 0.00000001,
                'incorrect testError (expected 1.22474487139)')
Test.assertTrue(abs(testError2 - 3.16227766017) < 0.00000001,
                'incorrect testError2 result (expected 3.16227766017)')
Test.assertTrue(abs(testError3 - 0.0) < 0.00000001,
                'incorrect testActual result (expected 0.0)')


from pyspark.mllib.recommendation import ALS
validationForPredictRDD = validationRDD.map(lambda x: (x[0], x[1]))

seed = 5L
iterations = 5
regularizationParameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02

minError = float('inf')
bestRank = -1
bestIteration = -1
for rank in ranks:
    model = ALS.train(trainingRDD, rank, seed=seed, iterations=iterations,
                      lambda_=regularizationParameter)
    predictedRatingsRDD = model.predictAll(validationForPredictRDD)
    error = computeError(predictedRatingsRDD, validationRDD)
    errors[err] = error
    err += 1
    print 'For rank {rank} the RMSE is {error}'.format(rank=rank, error=error)
    if error < minError:
        minError = error
        bestRank = rank


print 'The best model was trained with rank {}'.format(bestRank)

Test.assertEquals(trainingRDD.getNumPartitions(), 2,
                  'incorrect number of partitions for trainingRDD (expected 2)')
Test.assertEquals(validationForPredictRDD.count(), 96902,
                  'incorrect size for validationForPredictRDD (expected 96902)')
Test.assertEquals(validationForPredictRDD.filter(lambda t: t == (1, 1907)).count(), 1,
                  'incorrect content for validationForPredictRDD')

Test.assertTrue(abs(errors[0] - 0.883710109497) < tolerance, 'incorrect errors[0]')
Test.assertTrue(abs(errors[1] - 0.878486305621) < tolerance, 'incorrect errors[1]')
Test.assertTrue(abs(errors[2] - 0.876832795659) < tolerance, 'incorrect errors[2]')


myModel = ALS.train(trainingRDD, bestRank, seed=seed, iterations=iterations,
                    lambda_=regularizationParameter)
testForPredictingRDD = testRDD.map(lambda x: (x[0], x[1]))
predictedTestRDD = myModel.predictAll(testForPredictingRDD)

testRMSE = computeError(testRDD, predictedTestRDD)

print 'The model had a RMSE on the test set of {}'.format(testRMSE)


Test.assertTrue(abs(testRMSE - 0.87809838344) < tolerance, 'incorrect testRMSE')

from operator import add
trainingAvgRating = trainingRDD.map(lambda x: x[2]).reduce(lambda a, b: a + b) / trainingRDD.count()
print 'The average rating for movies in the training set is {}'.format(trainingAvgRating)

testForAvgRDD = testRDD.map(lambda x: (x[0], x[1], trainingAvgRating))
testAvgRMSE = computeError(testRDD, testForAvgRDD)
print 'The RMSE on the average set is {}'.format(testAvgRMSE)


Test.assertTrue(abs(trainingAvgRating - 3.57409571052) < 0.000001,
                'incorrect trainingAvgRating (expected 3.57409571052)')
Test.assertTrue(abs(testAvgRMSE - 1.12036693569) < 0.000001,
                'incorrect testAvgRMSE (expected 1.12036693569)')


print 'Most rated movies:'
print '(average rating, movie name, number of reviews)'
for ratingsTuple in movieLimitedAndSortedByRatingRDD.take(50):
    print ratingsTuple


myUserID = 0
myRatedMovies = [
     (myUserID, 993, 5),
     (myUserID, 789, 5),
     (myUserID, 744, 4),
     (myUserID, 633, 3),
     (myUserID, 1447, 5),
     (myUserID, 1195, 5),
     (myUserID, 551, 4),
     (myUserID, 811, 5),
     (myUserID, 1088, 5),
     (myUserID, 941, 4)
    ]

myRatingsRDD = sc.parallelize(myRatedMovies)
print 'My movie ratings: {}'.format(myRatingsRDD.take(10))


trainingWithMyRatingsRDD = trainingRDD.union(myRatingsRDD)

print ('The training dataset now has {} more entries than the original training dataset'.format(trainingWithMyRatingsRDD.count() - trainingRDD.count()))

assert (trainingWithMyRatingsRDD.count() - trainingRDD.count()) == myRatingsRDD.count()

myRatingsModel = ALS.train(trainingWithMyRatingsRDD, bestRank, seed=seed, iterations=iterations,lambda_=regularizationParameter)

predictedTestMyRatingsRDD = myRatingsModel.predictAll(testForPredictingRDD)
testRMSEMyRatings = computeError(testRDD,predictedTestMyRatingsRDD)
print 'The model had a RMSE on the test set of {}'.format(testRMSEMyRatings)

myUnratedMoviesRDD = (moviesRDD.filter(lambda x: x[0] not in myRatedMovies).map(lambda x: (0, x[0])))
predictedRatingsRDD = myRatingsModel.predictAll(myUnratedMoviesRDD)


movieCountsRDD = movieIDsWithAvgRatingsRDD.map(lambda x: (x[0], x[1][0]))

# Transform predictedRatingsRDD into an RDD with entries that are pairs of the form (Movie ID, Predicted Rating)
predictedRDD = predictedRatingsRDD.map(lambda x: (x[1], x[2]))

# Use RDD transformations with predictedRDD and movieCountsRDD to yield an RDD with tuples of the form (Movie ID, (Predicted Rating, number of ratings))
predictedWithCountsRDD  = (predictedRDD.join(movieCountsRDD))

# Use RDD transformations with PredictedWithCountsRDD and moviesRDD to yield an RDD with tuples of the form (Predicted Rating, Movie Name, number of ratings), for movies with more than 75 ratings
ratingsWithNamesRDD = (predictedWithCountsRDD.filter(lambda x: x[1][1] > 75).join(moviesRDD).map(lambda x: (x[1][0][0], x[1][1])))

predictedHighestRatedMovies = ratingsWithNamesRDD.takeOrdered(20, key=lambda x: -x[0])

print ('My highest rated movies as predicted (for movies with more than 75 reviews):\n{}'.format('\n'.join(map(str, predictedHighestRatedMovies))))


