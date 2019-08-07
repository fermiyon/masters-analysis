//  Created by Selman Karaosmanoglu.
//  Karaosmanoglu is a Master's Student at the Informatics Institute of Afyon Kocatepe University
//  Copyright © 2019 Selman Karaosmanoglu. All rights reserved.

//  Test System:
//    macOS Mojave(10.14.2) running on MacBook Pro(Early 2015)
//    java version "1.8.0_181"
//    Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
//    Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
//    Scala Version 2.12.7
//    SBT version 1.0

package selmank


import de.sciss.sheet._
import ExcelUtils._
import NielsenModel.Reklam
import Model._
import Controller._
import FileUtils._
import better.files._
import java.io.{File => JFile}

object Main {
  val path = "Veri_linkli.xlsx"
  val rootPath = "resources"
  val workbook = load(path)
  val sheet = getFirstSheet(workbook)
  val rows = getRows(sheet)
  val sortedRows = sortRows(rows)
  val infoRow = sortedRows.head
  val sortedRowsTail = sortedRows.tail
  val ads = rowsToReklam(sortedRowsTail)
  val adsAndRows = (ads zip sortedRowsTail).toMap

  val reklamsGrouped = ads.groupBy(_.medya)
  val tv8Ads = reklamsGrouped("TV8")
  val disneyAds = reklamsGrouped("DISNEY CHANNEL")

  val adsDistinctByVersion = distinctBy(ads)(_.versiyon)
  val tv8AdsDistinctByVersion = distinctBy(tv8Ads)(_.versiyon)
  val disneyAdsDistinctByVersion = distinctBy(disneyAds)(_.versiyon)

  val adsDistinctByUrun = distinctBy(ads)(_.urunHizmet)
  val tv8AdsDistinctByUrun = distinctBy(tv8Ads)(_.urunHizmet)
  val disneyAdsDistinctByUrun = distinctBy(disneyAds)(_.urunHizmet)


  val adsGrouped = groupedReklams(adsDistinctByUrun, adsAndRows, infoRow)
  val tv8Grouped = groupedReklams(tv8AdsDistinctByUrun, adsAndRows, infoRow)
  val disneyGrouped = groupedReklams(disneyAdsDistinctByUrun, adsAndRows, infoRow)

  val adsWithInfoReenumerated = addInfoAndReenumerate(ads, infoRow, adsAndRows)
  val tv8AdsWithInfoReenumerated = addInfoAndReenumerate(tv8Ads, infoRow, adsAndRows)
  val disneyAdsWithInfoReenumerated = addInfoAndReenumerate(disneyAds, infoRow, adsAndRows)

  val adsDistinctByVersionWithInfoReenumerated = addInfoAndReenumerate(adsDistinctByVersion, infoRow, adsAndRows)
  val tv8AdsDistinctByVersionWithInfoReenumerated = addInfoAndReenumerate(tv8AdsDistinctByVersion, infoRow, adsAndRows)
  val disneyAdsDistinctByVersionWithInfoReenumerated = addInfoAndReenumerate(disneyAdsDistinctByVersion, infoRow, adsAndRows)

  val adsDistinctByUrunWithInfoReenumerated = addInfoAndReenumerate(adsDistinctByUrun, infoRow, adsAndRows)
  val tv8AdsDistinctByUrunWithInfoReenumerated = addInfoAndReenumerate(tv8AdsDistinctByUrun, infoRow, adsAndRows)
  val disneyAdsDistinctByUrunWithInfoReenumerated = addInfoAndReenumerate(disneyAdsDistinctByUrun, infoRow, adsAndRows)


  save(adsWithInfoReenumerated)(s"$rootPath/ads.xls")
  save(tv8AdsWithInfoReenumerated)(s"$rootPath/tv8.xls")
  save(disneyAdsWithInfoReenumerated)(s"$rootPath/disney.xls")

  save(adsDistinctByVersionWithInfoReenumerated)(s"$rootPath/adsDistinctByVersion.xls")
  save(tv8AdsDistinctByVersionWithInfoReenumerated)(s"$rootPath/tv8DistinctByVersion.xls")
  save(disneyAdsDistinctByVersionWithInfoReenumerated)(s"$rootPath/disneyDistinctByVersion.xls")


  save(adsGrouped)(s"$rootPath/adsDistinctByUrunGrouped.xls")
  save(tv8Grouped)(s"$rootPath/tv8DistinctByUrunGrouped.xls")
  save(disneyGrouped)(s"$rootPath/disneyDistinctByUrunGrouped.xls")

  //copyAds(adsDistinctByUrun)

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
}

object Controller {
  def toRow(ads: List[Reklam], reklamToRow: Map[Reklam, Row]) = {
    ads.map {
      reklamToRow(_)
    }
  }

  def addInfoRow(infoRow: Row, list: List[Row]) = List[Row](infoRow) ++ list


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


  def groupedReklams(list: List[Reklam], adsAndRows: Map[Reklam, Row], infoRow: Row) = {
    val group = list.groupBy(_.anaSektor)
    val anaSektors = getAnaSektors(list)
    val rows = anaSektors.map {
      k => {
        val sublist = group(k)
        val sublistAsRow = toRow(sublist, adsAndRows)
        val sektorRow = ExcelUtils.createRow(k)
        val subgroup = List[Row](sektorRow) ++ sublistAsRow
        subgroup
      }
    }.flatten

    val k = reenumerateNielsenRows(List[Row](infoRow) ++ rows)
    k
  }

  def copyAds(liste: List[Reklam]): Unit = {
    val adFileNames = liste.map { k => s"${k.id.toInt}.mpg" }
    val adFiles = list("resources" / "reklamlar")
    val filtered = adFiles.filter { k => adFileNames.contains(k.name) }
    filtered.map {
      file => copy(file)("resources" / "subreklamlar")
    }
    //
  }

  def addInfoAndReenumerate(list: List[Reklam], infoRow: Row, adsAndRows: Map[Reklam, Row]) = {
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

}

object FileUtils {
  // Usage list("resources" / "reklamlar" )
  def list(path: File) = {
    path.list.toList
  }

  def copy(file: File)(to: File) = {
    to.createDirectoryIfNotExists()
    if (!isExist(file, to)) {
      file.copyToDirectory(to)
    }
  }

  def isExist(file: File, in: File) = {
    list(in).exists(_ == file)
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