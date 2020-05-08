package tools

import com.alibaba.fastjson.JSONObject
import dao.{HDFSInfo, Image}
import scalaj.http.{Http, HttpOptions}
import spray.json._

object Init {

    def fetchImage(id: Int, base: String): Image = {
//      var id
      var imageSourceJSON = new JSONObject()
      imageSourceJSON.put("id",id);

      // query image information
      val response = Http(base + "/image/getimage")
        .postForm
        .param("id",id.toString)
        //      .postData(imageSourceJSON.toString())
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000)).asString
      var image = new Image();
      var imageInfo = response.body.parseJson
      var HDFSID = imageInfo.asJsObject.getFields("data")(0).asJsObject.getFields("hdfsid")(0)
      var bands = imageInfo.asJsObject.getFields("data")(0).asJsObject.getFields("bands")(0)
      var rows = imageInfo.asJsObject.getFields("data")(0).asJsObject.getFields("rows")(0)
      var samples = imageInfo.asJsObject.getFields("data")(0).asJsObject.getFields("samples")(0)
      var datatype = imageInfo.asJsObject.getFields("data")(0).asJsObject.getFields("datatype")(0)
      var interleave = imageInfo.asJsObject.getFields("data")(0).asJsObject.getFields("interleave")(0)
      image.setId(id)
      image.setDatatype((datatype.toString().toByte));
      image.setRows(rows.toString().toShort)
      image.setSamples(samples.toString().toShort)
      image.setBands((bands.toString().toShort))
      image.setInterleave(interleave.toString().replaceAll("\"",""))
      image.setHdfsid(HDFSID.toString().toInt)

//      var image = new Image()
      return image;
    }

    def fetchHDFSInfo(id:Int, base: String): HDFSInfo = {
      // query image's hdfs information
      var hdfsSourceJSON = new JSONObject();
      hdfsSourceJSON.put("id",id)
      val responseHDFS = Http(base + "/hdfs/get")
        .postForm
        .param("id", id.toString())
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(10000)).asString
      var hdfsInfo = responseHDFS.body.parseJson
      var hdfsurl = hdfsInfo.asJsObject.getFields("data")(0).asJsObject.getFields("url")(0)
      var blocksize = hdfsInfo.asJsObject.getFields("data")(0).asJsObject.getFields("blocksize")(0)
      var blocknumber = hdfsInfo.asJsObject.getFields("data")(0).asJsObject.getFields("blocknumber")(0)

      var info = new HDFSInfo()
      info.setBlocknumber((blocknumber.toString().toInt))
      info.setBlocksize((blocksize.toString().toLong))
      info.setUrl(hdfsurl.toString())
      return  info
    }


}
