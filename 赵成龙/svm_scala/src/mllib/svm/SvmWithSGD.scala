package mllib.svm

import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics

/**
 * @author lenovo
 */
class SvmWithSGD{
  
}
object SvmWithSGD{
  def main(args: Array[String]): Unit = {
       // Load and parse the data file
      val sparkConf = new SparkConf().setAppName("SvmWithSGD").setMaster("local[4]")
      val sc = new SparkContext(sparkConf);
      // Load training data in LIBSVM format.
      val data = MLUtils.loadLibSVMFile(sc, "result.txt")
      
      // Split data into training (60%) and test (40%).
      val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
      val training = splits(0).cache()
      val test = splits(1)
      
      // Run training algorithm to build the model
      val numIterations = 100
      val stepSize =1.0
      val regParam =0.01
      val miniBatchFraction =1.0
      
      val model = SVMWithSGD.train(training, numIterations)
      //4 对测试样本进行测试
    
      val prediction = model.predict(test.map(_.features))
      var new_weight = model.weights;
    
      val predictionAndLabel = prediction.zip(test.map(_.label)) 
    
      //5 计算测试误差
    
      val metrics = new MulticlassMetrics(predictionAndLabel)
    
      val precision = metrics.precision
    
      println("Precision = " + precision)
      //-------------------------------------
      //val model = SVMWithSGD.train(training, numIterations)
   
      //for test
      var iter=1;
      while(iter<10){
        //------------------------------------
      val model = SVMWithSGD.train(training, numIterations,stepSize,regParam,miniBatchFraction,new_weight)
      //4 对测试样本进行测试
    
      val prediction = model.predict(test.map(_.features))
      new_weight = model.weights;
    
      val predictionAndLabel = prediction.zip(test.map(_.label)) 
    
      //5 计算测试误差
    
      val metrics = new MulticlassMetrics(predictionAndLabel)
    
      val precision = metrics.precision
    
      println("iter ="+iter+" Precision = " + precision)
      //-------------------------------------
      
      /*// Clear the default threshold.
      model.clearThreshold()
      
      // Compute raw scores on the test set.
      val scoreAndLabels = test.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }
      
      // Get evaluation metrics.
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      val auROC = metrics.areaUnderROC()
      
      println("Area under ROC = " + auROC)*/
      iter +=1
      }
      
      
    }
}