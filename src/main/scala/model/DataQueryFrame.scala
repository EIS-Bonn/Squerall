package model

import scala.collection.mutable.ListBuffer

class DataQueryFrame {

    private var _selects : ListBuffer[(String, String)] = ListBuffer()
    private var _filters : ListBuffer[String] = ListBuffer()
    private var _joins : ListBuffer[(String, String, String, String)] = ListBuffer()
    private var _project : (Seq[String], Boolean) = (null,false)
    private var _orderBy : (String, Int) = ("",0)
    private var _groupBy : ListBuffer[String] = ListBuffer()
    private var _aggregate : List[(String, String)] = List()
    private var _limit : Int = 0

    def addSelect(cols_table: (String, String)): ListBuffer[(String, String)] = {
        _selects += cols_table
    }

    def addFilter(condition: String): ListBuffer[String] = {
        _filters += condition
    }

    def addJoin(join: (String, String, String, String)): ListBuffer[(String, String, String, String)] = {
        _joins += join
    }

    def addProject(p: (Seq[String], Boolean)): Unit = {
        _project = p
    }

    def addOrderBy(ob: (String, Int)): Unit = {
        _orderBy = ob
    }

    def addGroupBy(cols: ListBuffer[String]): Unit = {
        _groupBy = cols
    }

    def addAggregate(agg: List[(String, String)]): Unit = {
        _aggregate = agg
    }

    def addLimit(limitValue: Int): Unit = {
        _limit = limitValue
    }
}