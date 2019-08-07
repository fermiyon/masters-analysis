//  Created by Selman Karaosmanoglu on 11.01.2019.
//  Karaosmanoglu is a Master's Student at the Informatics Institute of Afyon Kocatepe University
//  Copyright © 2019 Selman Karaosmanoglu. All rights reserved.

//  Test System:
//    macOS Mojave(10.14.2) running on MacBook Pro(Early 2015)
//    java version "1.8.0_181"
//    Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
//    Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
//    Scala Version 2.12.7
//    SBT version 1.0




import de.sciss.sheet._
import better.files._
import java.io.{File => JFile}

import ExcelUtils._
import NielsenModel.Reklam
import Model._
import Controller._
import FileUtils._
import Mutables._
import Implicits._

object Main2 extends App {
  val path = "Veri_linkli.xlsx"
  val rootPath = "resources"
  val workbook = load(path)
  val sheet = getFirstSheet(workbook)
  val rows = getRows(sheet)
  val sortedRows = sortRows(rows)
  val infoRow = sortedRows.head
  Mutables.infoRow = Some(infoRow)
  val sortedRowsTail = sortedRows.tail
  val ads = rowsToReklam(sortedRowsTail)
  val adsAndRows = (ads zip sortedRowsTail).toMap
  Mutables.adsAndRowsMap = Some(adsAndRows)

  

  val adsGroupByMedia = ads.groupBy(_.medya)
  val tv8Ads = adsGroupByMedia("TV8")
  val disneyAds = adsGroupByMedia("DISNEY CHANNEL")

  val adsDistinctByVersion = distinctBy(ads)(_.versiyon)
  val tv8AdsDistinctByVersion = distinctBy(tv8Ads)(_.versiyon)
  val disneyAdsDistinctByVersion = distinctBy(disneyAds)(_.versiyon)

  val adsDistinctByUrun = distinctBy(ads)(_.urunHizmet)
  val tv8AdsDistinctByUrun = distinctBy(tv8Ads)(_.urunHizmet)
  val disneyAdsDistinctByUrun = distinctBy(disneyAds)(_.urunHizmet)

  

  val adsGrouped = groupedReklams(adsDistinctByUrun).addInfo.reenumerate
  val tv8Grouped = groupedReklams(tv8AdsDistinctByUrun).addInfo.reenumerate
  val disneyGrouped = groupedReklams(disneyAdsDistinctByUrun).addInfo.reenumerate

  val adsWithInfoReenumerated = ads.toRow.addInfo.reenumerate
  val tv8AdsWithInfoReenumerated = tv8Ads.toRow.addInfo.reenumerate
  val disneyAdsWithInfoReenumerated = disneyAds.toRow.addInfo.reenumerate


  val adsDistinctByVersionWithInfoReenumerated = adsDistinctByVersion.toRow.addInfo.reenumerate
  val tv8AdsDistinctByVersionWithInfoReenumerated = tv8AdsDistinctByVersion.toRow.addInfo.reenumerate
  val disneyAdsDistinctByVersionWithInfoReenumerated = disneyAdsDistinctByVersion.toRow.addInfo.reenumerate

  val adsDistinctByUrunWithInfoReenumerated = adsDistinctByUrun.toRow.addInfo.reenumerate
  val tv8AdsDistinctByUrunWithInfoReenumerated = tv8AdsDistinctByUrun.toRow.addInfo.reenumerate
  val disneyAdsDistinctByUrunWithInfoReenumerated = disneyAdsDistinctByUrun.toRow.addInfo.reenumerate
  
  
  val intersectionAdsDistinctByVersion = tv8AdsDistinctByVersion.filter(x=> disneyAdsDistinctByVersion.exists(_.versiyon == x.versiyon))
  val intersectionAdsDistinctByVersionRowed = intersectionAdsDistinctByVersion.toRow.addInfo.reenumerate
  
  val intersectionAdsDistinctByUrun = tv8AdsDistinctByUrun.filter(x=> disneyAdsDistinctByUrun.exists(_.urunHizmet == x.urunHizmet))
  val intersectionAdsDistinctByUrunRowed = intersectionAdsDistinctByUrun.toRow.addInfo.reenumerate

  /* 
   *Exceptions
  */

  val deadlinkedAds = Nielsen.VideoIdsDeadLink.flatMap{k => ads.find{x=>x.id == k }}
  val kamuSpotAds = adsDistinctByUrun.filter{x => x.spotTipiD == "ZORUNLU KAMU SPOTU" || x.spotTipiD == "SOSYAL REKLAM"  }
  val exceptions  = distinctBy(deadlinkedAds ++ kamuSpotAds)(_.urunHizmet) //Total : 23

  def subtractExceptions(list:List[NielsenModel.Reklam]) = {
    list.filterNot{x=> exceptions.exists{k=>k.id == x.id}}
  }

  val adsDistinctByUrunWithExceptions = subtractExceptions(adsDistinctByUrun)
  val tv8AdsDistinctByUrunWithExceptions = subtractExceptions(tv8AdsDistinctByUrun)
  val disneyAdsDistinctByUrunWithExceptions = subtractExceptions(disneyAdsDistinctByUrun)

  val adsDistinctByUrunWithExceptionsRowed = adsDistinctByUrunWithExceptions.toRow.addInfo.reenumerate
  val tv8AdsDistinctByUrunWithExceptionsRowed = tv8AdsDistinctByUrunWithExceptions.toRow.addInfo.reenumerate
  val disneyAdsDistinctByUrunWithExceptionsRowed = disneyAdsDistinctByUrunWithExceptions.toRow.addInfo.reenumerate



  save(adsWithInfoReenumerated)(s"$rootPath/ads.xls")
  save(tv8AdsWithInfoReenumerated)(s"$rootPath/tv8.xls")
  save(disneyAdsWithInfoReenumerated)(s"$rootPath/disney.xls")

  save(adsDistinctByVersionWithInfoReenumerated)(s"$rootPath/adsDistinctByVersion.xls")
  save(tv8AdsDistinctByVersionWithInfoReenumerated)(s"$rootPath/tv8DistinctByVersion.xls")
  save(disneyAdsDistinctByVersionWithInfoReenumerated)(s"$rootPath/disneyDistinctByVersion.xls")

  save(adsDistinctByUrunWithInfoReenumerated)(s"$rootPath/adsDistinctByUrun.xls")
  save(tv8AdsDistinctByUrunWithInfoReenumerated)(s"$rootPath/tv8DistinctByUrun.xls")
  save(disneyAdsDistinctByUrunWithInfoReenumerated)(s"$rootPath/disneyDistinctByUrun.xls")

  save(adsGrouped)(s"$rootPath/adsDistinctByUrunGrouped.xls")
  save(tv8Grouped)(s"$rootPath/tv8DistinctByUrunGrouped.xls")
  save(disneyGrouped)(s"$rootPath/disneyDistinctByUrunGrouped.xls")

  save(getAnaSektorInfoRows(adsDistinctByUrun))(s"$rootPath/adsAnaSektors.xls")
  save(getAnaSektorInfoRows(tv8AdsDistinctByUrun))(s"$rootPath/tv8AnaSektors.xls")
  save(getAnaSektorInfoRows(disneyAdsDistinctByUrun))(s"$rootPath/disneyAnaSektors.xls")

  save(intersectionAdsDistinctByUrunRowed)(s"$rootPath/intersectionAdsDistinctByUrun.xls")
  save(intersectionAdsDistinctByVersionRowed)(s"$rootPath/intersectionAdsDistinctByVersion.xls")

  //Exceptions
  save(adsDistinctByUrunWithExceptionsRowed)(s"$rootPath/exceptions/adsDistinctByUrunWithExceptions.xls")
  save(tv8AdsDistinctByUrunWithExceptionsRowed)(s"$rootPath/exceptions/tv8DistinctByUrunWithExceptions.xls")
  save(disneyAdsDistinctByUrunWithExceptionsRowed)(s"$rootPath/exceptions/disneyDistinctByUrunWithExceptions.xls")

  save(getAnaSektorInfoRows(adsDistinctByUrunWithExceptions))(s"$rootPath/exceptions/adsAnaSektorsWithExceptions.xls")
  save(getAnaSektorInfoRows(tv8AdsDistinctByUrunWithExceptions))(s"$rootPath/exceptions/tv8AnaSektorsWithExceptions.xls")
  save(getAnaSektorInfoRows(disneyAdsDistinctByUrunWithExceptions))(s"$rootPath/exceptions/disneyAnaSektorsWithExceptions.xls")
}

object ExcelUtils {
  def load(path: String): Workbook = {
    Workbook.fromFile(path)
  }

  def getFirstSheet(workbook: Workbook) = {
    workbook.sheets.toList(0)
  }

  def getRows(sheet: Sheet) = {
    sheet.rows.toList
  }

  def sortRows(rows: List[Row]) = {
    rows.sortBy(_.index)
  }

  def sortCells(cells: List[Cell]) = {
    cells.sortBy(_.index)
  }

  def reenumerateRows(rows: List[Row]) = {
    val length = rows.length
    val list = (0 to length - 1).map {
      k => {
        val row = rows(k)
        val cells = row.cells
        val newRow = Row(k)(cells)
        newRow
      }
    }
    list.toList
  }

  def save(rows: List[Row])(path: String): Unit = {
    val sheet = Sheet("sheet")(rows.toSet)
    val workbook = Workbook(List[Sheet](sheet).toSet)

    //Check file path if exists
    workbook.saveToFile(path)
  }

  def createRow(str: String, index: Int = 0) = {
    val cell = StringCell(0, str)
    Row(index)(Set[Cell](cell))
  }

  def createRow2(str: (String, String), index: Int = 0) = {
    val cell1 = StringCell(0, str._1)
    val cell2 = StringCell(1, str._2)
    Row(index)(Set[Cell](cell1, cell2))
  }
}

object Controller {
  //Mutable Version
  def toRow(ads: List[Reklam]) = {
    ads.map {
      adsAndRowsMap.get(_)
    }
  }

  //Immutable Version
  def toRow(ads: List[Reklam], reklamToRow: Map[Reklam, Row]) = {
    ads.map {
      reklamToRow(_)
    }
  }

  def addInfoRow(infoRow: Row, list: List[Row]) = List[Row](infoRow) ++ list


  //Timothy Klim - distinctBy Method
  //https://stackoverflow.com/questions/3912753/scala-remove-duplicates-in-list-of-objects
  def distinctBy[L, E](list: List[L])(f: L => E): List[L] =
    list.foldLeft((Vector.empty[L], Set.empty[E])) {
      case ((acc, set), item) =>
        val key = f(item)
        if (set.contains(key)) (acc, set)
        else (acc :+ item, set + key)
    }._1.toList

  def updateLinks(list: List[Reklam]) = {
    list.map(updateLink(_))
  }

  def updateLink(reklam: Reklam, path: String = "resources/reklamlar/") = {
    val newLink = reklam.link.split("\"").toList.updated(1, path).mkString("\"")
    val newReklam = reklam.copy(link = newLink)
    newReklam
  }

  def getAnaSektors(list: List[Reklam]): List[String] = {
    list.map {
      _.anaSektor
    }.distinct.sorted
  }

  def getMedias(list: List[Reklam]): List[String] = {
    list.map {
      _.medya
    }.distinct.sorted
  }


  def groupedReklams(list: List[Reklam]) = {
    val group = list.groupBy(_.anaSektor)
    val anaSektors = getAnaSektors(list)
    val rows = anaSektors.map {
      k => {
        val sublist = group(k)
        val sublistAsRow = toRow(sublist)
        val sektorRow = ExcelUtils.createRow(k)
        val subgroup = List[Row](sektorRow) ++ sublistAsRow
        subgroup
      }
    }.flatten

    val k = rows
    k
  }


  //Usage copyAds(adsDistinctByUrun)

  def copyAds(liste: List[Reklam]): Unit = {
    val adFileNames = liste.map { k => s"${k.id.toInt}.mpg" }
    val FolderOfAdsFiles = list("resources" / "reklamlar")
    val filteredFiles = FolderOfAdsFiles.filter { k => adFileNames.contains(k.name) }
    val to = "resources" / "subreklamlar"
    val notExist = adFileNames diff filteredFiles.map{_.name}
    filteredFiles.map {
      file => copy(file)(to)
    }

  }

  def addInfoAndReenumerate(list: List[Reklam], infoRow: Row, adsAndRows: Map[Reklam,Row]) = {
    val rows = toRow(list, adsAndRows)
    val newRows = infoRow +: rows
    reenumerateNielsenRows(newRows)
  }

  def reenumerateNielsenRows(rows: List[Row]) = {
    val rowList = reenumerateRows(rows)
    val newRows = rowList.map{updateFormulaCell(_)}
    newRows
  }

  def updateFormulaCell(row:Row): Row = {
    val index = row.index
    val cells = row.cells.toList.sortBy{_.index}
    val hasFormulaCell = cells.filter{_.isInstanceOf[FormulaCell]}.nonEmpty
    if(hasFormulaCell) {
      val formulaCell = cells.filter{_.isInstanceOf[FormulaCell]}(0).asInstanceOf[FormulaCell]
      val data = formulaCell.data
      val updatedData = data.split(",").toList.updated(1,s"N${index + 1}").mkString(",")
      val updatedFormulaCell = FormulaCell(formulaCell.index, updatedData)
      val updatedCells = cells.updated(cells.indexOf(formulaCell),updatedFormulaCell).sortBy{_.index}
      Row(index)(updatedCells.toSet)
    }
    else {
      row
    }

  }

  def getAnaSektorInfoRows(list:List[Reklam]) = {
    getAnaSektors(list).map{k => createRow2((k,list.filter{_.anaSektor == k}.length.toString))}.reenumerate
  }


}

object Mutables {
  var adsAndRowsMap : Option[Map[Reklam, Row]] = None
  var infoRow : Option[Row] = None
}

object FileUtils {
  // Usage list("resources" / "reklamlar" )
  def list(path: File) = {
    path.list.toList
  }

  def copy(file: File)(to: File) = {
    to.createDirectoryIfNotExists()
    //Fast
    if (!isFileNameExist(file.name, to)) {
      file.copyToDirectory(to)
    }
  }

  //Slow
  def isExist(file: File, in: File) = {
    list(in).exists(f => f.isSameContentAs(file))
  }

  //Fast
  def isFileNameExist(fileName: String, in: File) = {
    in.list.toList.map{_.name}.filter(_ == fileName).length != 0
  }
}

object Model {
  def rowsToReklam(rows: List[Row]) = {
    rows.map {
      k => {
        val cells = sortCells(k.cells.toList)
        val tarih = cells(0).asInstanceOf[NumericCell].data
        val anaSektor = cells(1).asInstanceOf[StringCell].data
        val reklaminFirmasi = cells(2).asInstanceOf[StringCell].data
        val urunHizmet = cells(3).asInstanceOf[StringCell].data
        val medya = cells(4).asInstanceOf[StringCell].data
        val versiyon = cells(5).asInstanceOf[StringCell].data
        val spotTipiD = cells(6).asInstanceOf[StringCell].data
        val baslangic = cells(7).asInstanceOf[StringCell].data
        val bitis = cells(8).asInstanceOf[StringCell].data
        val program = cells(9).asInstanceOf[StringCell].data
        val pgOzel = cells(10).asInstanceOf[StringCell].data
        val ptAdet = cells(11).asInstanceOf[NumericCell].data
        val ptSure = cells(12).asInstanceOf[NumericCell].data
        val id = cells(13).asInstanceOf[NumericCell].data
        val link = cells(14).asInstanceOf[FormulaCell].data
        val reklam = Reklam(tarih,
          anaSektor,
          reklaminFirmasi,
          urunHizmet,
          medya,
          versiyon,
          spotTipiD,
          baslangic,
          bitis,
          program,
          pgOzel,
          ptAdet,
          ptSure,
          id,
          link)
        reklam
      }
    }
  }

  abstract class Ads {
    val ads:List[Reklam]
    val name:String
  }

  case class ChannelAdsSubset(val ads:List[Reklam], val name:String , val channelName:String) extends Ads

  case class AdsSubset(val ads:List[Reklam], val name:String) extends Ads

}


object NielsenModel {

  case class Reklam(tarih: Double,
                    anaSektor: String,
                    reklaminFirmasi: String,
                    urunHizmet: String,
                    medya: String,
                    versiyon: String,
                    spotTipiD: String,
                    baslangic: String,
                    bitis: String,
                    program: String,
                    pgOzel: String,
                    ptAdet: Double,
                    ptSure: Double,
                    id: Double,
                    link: String)

}

object Nielsen {
  val VideoIdsDeadLink = List(428778, 428647, 428651, 401779, 428671, 428684, 428662, 428688, 416356, 201341)
}

object Implicits {
  implicit class AdsSubsetImprovements(val s: AdsSubset) {
    def requestedSubsets:List[AdsSubset] = {
      val distinctByVersion = distinctBy(s.ads)(_.versiyon)
      val distinctByUrun = distinctBy(s.ads)(_.urunHizmet)
      //val grouped = groupedReklams(distinctByUrun)
      List[AdsSubset](
        AdsSubset(distinctByVersion,s"${s.name}DistinctByVersion"),
        AdsSubset(distinctByUrun,s"${s.name}DistinctByUrun")
      )
    }

    def toRow = Controller.toRow(s.ads)
  }


  implicit class RowListImprovements(val s: List[Row]) {
    def addInfo = Mutables.infoRow.get +: s
    def reenumerate = Controller.reenumerateNielsenRows(s)
  }

  implicit class NielsenModelListImprovements(val s: List[Reklam]) {
    def toRow = Controller.toRow(s)
  }

}