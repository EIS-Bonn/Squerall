package org.squerall.model

import scala.collection.mutable.ListBuffer

class DataQueryFrame {

    private var _selects : ListBuffer[(String, String, String)] = ListBuffer()
    private var _filters : ListBuffer[String] = ListBuffer()
    private var _joins : ListBuffer[(String, String, String, String)] = ListBuffer()
    private var _project : (Seq[String], Boolean) = (null,false)
    private var _orderBy : (String, Int) = ("",0)
    private var _groupBy : ListBuffer[String] = ListBuffer()
    private var _aggregate : List[(String, String)] = List()
    private var _limit : Int = 0
    private var _transform : Map[String,Array[String]] = Map()

    def addSelect(cols_table: (String, String, String)): Unit = {
        _selects += cols_table
    }

    def addFilter(condition: String) : Unit = {
        _filters += condition
    }

    def addJoin(join: (String, String, String, String)): Unit = {
        _joins += join
    }

    def addProject(p: (Seq[String], Boolean)) : Unit = {
        _project = p
    }

    def addOrderBy(ob: (String, Int)) : Unit = {
        _orderBy = ob
    }

    def addGroupBy(cols: ListBuffer[String]) : Unit = {
        _groupBy = cols
    }

    def addAggregate(agg: List[(String, String)]) : Unit = {
        _aggregate = agg
    }

    def addLimit(limitValue: Int) : Unit = {
        _limit = limitValue
    }

    // Add one transfomration
    def addTransform(col: String, transformations: Array[String]) : Unit = {
        _transform += (col -> transformations)
    }

    // Append new to old transformations
    def addTransformations(transformations: Map[String, Array[String]]) : Unit = {
        for (t <- transformations) {
            val col = t._1
            val trans = t._2
            if (_transform.contains(col)) {
                val ondTrans = _transform(col)
                trans :+ ondTrans
            }
            _transform += (col -> trans) // overwrite the old with the full new
        }
    }


    /******* GETTERS *******/
    def getSelects : ListBuffer[(String, String, String)] = _selects

    def getFilters : ListBuffer[String] = _filters

    def getJoins : ListBuffer[(String, String, String, String)] = _joins

    def getProject : (Seq[String], Boolean) = _project

    def getOrderBy : (String, Int) = _orderBy

    def getGroupBy : ListBuffer[String] = _groupBy

    def getAggregate : List[(String, String)] = _aggregate

    def getLimit : Int = _limit

    def getTransform : Map[String, Array[String]] = _transform

}