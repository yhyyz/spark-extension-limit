package com.aws.analytics.extension

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate,GlobalLimit,LocalLimit , LogicalPlan, Project}
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.rules.Rule

// 1. 定义配置参数
object ForceLimitConf {
  val FORCE_LIMIT_ROWS = SQLConf.buildConf("spark.sql.force.limit.rows")
    .doc("Maximum number of rows to return when no LIMIT clause is specified")
    .intConf
    .createWithDefault(1000)
}

// 2. 实现 Rule
class ForceLimitRule(conf: SQLConf) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    // 跳过含有Limit的查询 (需要同时检查 GlobalLimit 和 LocalLimit)
    if (plan.collect {
      case _: GlobalLimit => true
      case _: LocalLimit => true
    }.nonEmpty) {
      return plan
    }

    // 跳过非查询操作（如INSERT、CREATE等）
    val isQueryOperation = plan.collect {
      case _: Project => true
      case _: Aggregate => true
    }.nonEmpty

    if (!isQueryOperation) {
      return plan
    }

    // 获取配置的limit值
    val maxRows = conf.getConf(ForceLimitConf.FORCE_LIMIT_ROWS)
    if (maxRows == -1){
      plan
    }else{
      GlobalLimit(Literal(maxRows), plan)
    }
  }
}

// 3. 实现 Extensions
class ForceLimitExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { session =>
      new ForceLimitRule(session.sessionState.conf)
    }
  }
}