package pp
import java.io.{File, FileInputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

object streamnow {

  def main(args: Array[String]) {


    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("file")
      .getOrCreate()

    val props: Properties = new Properties()

    props.load(new FileInputStream("C:\\spark-3.0.0-bin-hadoop2.7\\spark\\streamnow\\src\\main\\resources\\application.properties"))


    val inputpath = props.getProperty("devinputbasedir")
    val outputpath = props.getProperty("devoutputbasedir")

    //function to get list of file names
    def getListOfFiles(dir: String): List[String] = {
      val file = new File(dir)
      file.listFiles.filter(_.isFile)
        .map(_.getName).toList
    }

    //reading the file names and writing them into partitioned folders in parquet format
    def readandwrite(i:String): Unit =
    {
      val p = i.substring(0, i.lastIndexOf('_'))
      val secondlast = p.lastIndexOf('_')
      val datefolder = p.substring(secondlast + 1)
      val tablename = p.substring(0, secondlast)

      val finaloutputpath = outputpath + tablename + "\\datefolder=" +
        datefolder + "\\"
      // println(i)
      val df = spark.read.csv(inputpath + i)
      df.write.parquet(finaloutputpath)

    }

    val reg = """[a-z]+[A-Z]+_\d{8}_\d{14}.csv""".r

    val files = getListOfFiles(inputpath)


    for (i <- files) {
      val matches = reg.findAllIn(i)
      val pattern = matches.mkString("")
      //println(pattern)
      if (pattern.equals(i)) {

        readandwrite(i)
      }

      else {
        println(i)

        /*val source = new File("C:\\Users\\Palash\\Desktop\\input\\uiactivityout\\"+i).toPath
        val destination = new File("C:\\Users\\Palash\\Desktop\\destination2\\exception\\"+i).toPath
        Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING)*/

       /* val file_target = new Path("C:\\\\Users\\\\Palash\\\\Desktop\\\\destination2\\\\exception")
        fs.mkdirs(file_target)
        fs.rename("C:\\Users\\Palash\\Desktop\\input\\uiactivityout\\"+i, file_target)*/

      }



      }


    }
}
