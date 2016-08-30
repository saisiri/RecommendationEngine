package recommendation.engine

import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel, ALS}
import org.apache.spark.rdd.RDD

class Als {

  val userMusicHistory = new LoadData().loadUserRecentTracks()

  val grouped = userMusicHistory.groupBy(userTrack => userTrack.userId + ":" + userTrack.artistId.getOrElse(0) + ":" + userTrack.trackId.getOrElse(0))

  val rating = userMusicHistory.map(recentTrack => {new Rating(recentTrack.userId.toInt, recentTrack.trackId.getOrElse("0").toInt, )})

  // split data into train (60%), validation (20%), and test (20%) based on the
  val Array(trainingSet, validationSet, testSet) = userMusicHistory.randomSplit(Array(0.6, 0.2, 0.2))

  val numValidation = validationSet.count()

  // train models and evaluate them on the validation set
  val ranks = List(8, 12)
  val lambdas = List(0.1, 10.0)
  val numIters = List(10, 20)
  var bestModel: Option[MatrixFactorizationModel] = None
  var bestValidationRmse = Double.MaxValue
  var bestRank = 0
  var bestLambda = -1.0
  var bestNumIter = -1
  for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
    val model = ALS.train(trainingSet, rank, numIter, lambda)
    val validationRmse = computeRmse(model, validationSet, numValidation)
    println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
      + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
    if (validationRmse < bestValidationRmse) {
      bestModel = Some(model)
      bestValidationRmse = validationRmse
      bestRank = rank
      bestLambda = lambda
      bestNumIter = numIter
    }
  }

  // evaluate the best model on the test set

  val testRmse = computeRmse(bestModel.get, testSet, testSet.count())

  println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
    + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")


  // create a naive baseline and compare it with the best model

  val meanRating = trainingSet.union(validationSet).map(_.rating).mean
  val baselineRmse =
    math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
  val improvement = (baselineRmse - testRmse) / baselineRmse * 100
  println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

  ALSmodel = bestModel

//ALSmodel = Option(MatrixFactorizationModel.load(sc, "musichomedirectory/model/ALS"))


  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[UserTrack], n: Long): Double = {
    val predictions: RDD[UserTrack] = model.predict(data.map(x => (x.userId, x.trackId)))
    val predictionsAndRatings = predictions.map(x => ((x.userId, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }
}
