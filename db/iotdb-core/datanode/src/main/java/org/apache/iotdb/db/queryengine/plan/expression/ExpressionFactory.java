/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.expression;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.TimeDuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class ExpressionFactory {

  private ExpressionFactory() {
    // forbidden construction
  }

  public static PartialPath path(String pathStr) throws IllegalPathException {
    return new PartialPath(pathStr);
  }

  public static TimeSeriesOperand timeSeries(String pathStr) throws IllegalPathException {
    PartialPath path = new PartialPath(pathStr);
    return new TimeSeriesOperand(path);
  }

  public static TimeSeriesOperand timeSeries(PartialPath path) {
    return new TimeSeriesOperand(path);
  }

  public static ConstantOperand constant(TSDataType dataType, String valueString) {
    return new ConstantOperand(dataType, valueString);
  }

  public static ConstantOperand intValue(String valueString) {
    return new ConstantOperand(TSDataType.INT32, valueString);
  }

  public static ConstantOperand longValue(long value) {
    return new ConstantOperand(TSDataType.INT64, String.valueOf(value));
  }

  public static TimestampOperand time() {
    return new TimestampOperand();
  }

  public static FunctionExpression function(
      String functionName,
      LinkedHashMap<String, String> functionAttributes,
      Expression... inputExpression) {
    return new FunctionExpression(functionName, functionAttributes, Arrays.asList(inputExpression));
  }

  public static FunctionExpression function(String functionName, Expression... inputExpression) {
    return new FunctionExpression(
        functionName, new LinkedHashMap<>(), Arrays.asList(inputExpression));
  }

  public static FunctionExpression count(Expression inputExpression) {
    return new FunctionExpression(
        TAggregationType.COUNT.toString(),
        new LinkedHashMap<>(),
        Collections.singletonList(inputExpression));
  }

  public static FunctionExpression countTime(Expression inputExpression) {
    return new FunctionExpression(
        TAggregationType.COUNT_TIME.toString(),
        new LinkedHashMap<>(),
        Collections.singletonList(inputExpression));
  }

  public static FunctionExpression sum(Expression inputExpression) {
    return new FunctionExpression(
        TAggregationType.SUM.toString(),
        new LinkedHashMap<>(),
        Collections.singletonList(inputExpression));
  }

  public static LogicAndExpression and(Expression leftExpression, Expression rightExpression) {
    return new LogicAndExpression(leftExpression, rightExpression);
  }

  public static LogicOrExpression or(Expression leftExpression, Expression rightExpression) {
    return new LogicOrExpression(leftExpression, rightExpression);
  }

  public static AdditionExpression add(Expression leftExpression, Expression rightExpression) {
    return new AdditionExpression(leftExpression, rightExpression);
  }

  public static GreaterThanExpression gt(Expression leftExpression, Expression rightExpression) {
    return new GreaterThanExpression(leftExpression, rightExpression);
  }

  public static GreaterEqualExpression gte(Expression leftExpression, Expression rightExpression) {
    return new GreaterEqualExpression(leftExpression, rightExpression);
  }

  public static LessThanExpression lt(Expression leftExpression, Expression rightExpression) {
    return new LessThanExpression(leftExpression, rightExpression);
  }

  public static LessEqualExpression lte(Expression leftExpression, Expression rightExpression) {
    return new LessEqualExpression(leftExpression, rightExpression);
  }

  public static EqualToExpression eq(Expression leftExpression, Expression rightExpression) {
    return new EqualToExpression(leftExpression, rightExpression);
  }

  public static NonEqualExpression neq(Expression leftExpression, Expression rightExpression) {
    return new NonEqualExpression(leftExpression, rightExpression);
  }

  public static WhenThenExpression whenThen(Expression whenExpression, Expression thenExpression) {
    return new WhenThenExpression(whenExpression, thenExpression);
  }

  public static LogicNotExpression not(Expression expression) {
    return new LogicNotExpression(expression);
  }

  public static InExpression in(Expression expression, LinkedHashSet<String> values) {
    return new InExpression(expression, false, values);
  }

  public static InExpression notIn(Expression expression, LinkedHashSet<String> values) {
    return new InExpression(expression, true, values);
  }

  public static BetweenExpression between(
      Expression firstExpression, Expression secondExpression, Expression thirdExpression) {
    return new BetweenExpression(firstExpression, secondExpression, thirdExpression, false);
  }

  public static BetweenExpression notBetween(
      Expression firstExpression, Expression secondExpression, Expression thirdExpression) {
    return new BetweenExpression(firstExpression, secondExpression, thirdExpression, true);
  }

  public static GroupByTimeExpression groupByTime(GroupByTimeParameter parameter) {
    long startTime =
        parameter.isLeftCRightO() ? parameter.getStartTime() : parameter.getStartTime() + 1;
    long endTime = parameter.isLeftCRightO() ? parameter.getEndTime() : parameter.getEndTime() + 1;
    return new GroupByTimeExpression(
        startTime, endTime, parameter.getInterval(), parameter.getSlidingStep());
  }

  public static GroupByTimeExpression groupByTime(
      long startTime, long endTime, long interval, long slidingStep) {
    return new GroupByTimeExpression(
        startTime, endTime, new TimeDuration(0, interval), new TimeDuration(0, slidingStep));
  }
}
