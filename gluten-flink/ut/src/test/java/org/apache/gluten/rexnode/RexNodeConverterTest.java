/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.rexnode;

import org.apache.gluten.rexnode.functions.RexCallConverterFactory;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RexNodeConverterTest {

  private static FlinkTypeFactory typeFactory;
  private static RexBuilder rexBuilder;

  @BeforeAll
  public static void setUp() {
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);
  }

  @Test
  public void testVarcharLiteralExtractsRawValue() {
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral("apple", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isEqualTo("apple");
  }

  @Test
  public void testCharLiteralExtractsRawValue() {
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral("banana", typeFactory.createSqlType(SqlTypeName.CHAR), false);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isEqualTo("banana");
  }

  @Test
  public void testSha2FamilyCallsMapToVeloxNames() {
    RexInputRef stringFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexInputRef intFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
    RexConversionContext context =
        new RexConversionContext(java.util.Arrays.asList("ignored", "s", "n"));

    RexCall sha2 =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.SHA2, stringFieldRef, intFieldRef);
    CallTypedExpr sha2Expr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(sha2, context).toTypedExpr(sha2, context);
    assertThat(sha2Expr.getFunctionName()).isEqualTo("sha2");
    assertThat(sha2Expr.getReturnType()).isInstanceOf(VarCharType.class);
    assertThat(sha2Expr.getInputs()).hasSize(2);

    SqlFunction[] fixedLengthOps = {
      FlinkSqlOperatorTable.SHA224,
      FlinkSqlOperatorTable.SHA256,
      FlinkSqlOperatorTable.SHA384,
      FlinkSqlOperatorTable.SHA512
    };
    String[] veloxNames = {"sha224", "sha256", "sha384", "sha512"};
    for (int i = 0; i < fixedLengthOps.length; i++) {
      RexCall call = (RexCall) rexBuilder.makeCall(fixedLengthOps[i], stringFieldRef);
      CallTypedExpr expr =
          (CallTypedExpr)
              RexCallConverterFactory.getConverter(call, context).toTypedExpr(call, context);
      assertThat(expr.getFunctionName()).isEqualTo(veloxNames[i]);
      assertThat(expr.getReturnType()).isInstanceOf(VarCharType.class);
      assertThat(expr.getInputs()).hasSize(1);
    }
  }

  @Test
  public void testSha2LiteralZeroLengthPassesThrough() {
    // The hash-length 0 rejection is pinned at velox execution time, matching
    // Flink 1.19.2's runtime rejection; the planner must not special-case 0.
    // A folded literal 0 therefore converts like any other length and reaches
    // velox intact instead of being rejected at conversion time.
    RexInputRef stringFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexLiteral zeroLength =
        (RexLiteral)
            rexBuilder.makeLiteral(0, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexConversionContext context =
        new RexConversionContext(java.util.Arrays.asList("ignored", "s", "n"));

    RexCall sha2 =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.SHA2, stringFieldRef, zeroLength);
    CallTypedExpr expr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(sha2, context).toTypedExpr(sha2, context);
    assertThat(expr.getFunctionName()).isEqualTo("sha2");
    assertThat(expr.getInputs()).hasSize(2);
    assertThat(expr.getInputs().get(1)).isInstanceOf(ConstantTypedExpr.class);
    ConstantTypedExpr length = (ConstantTypedExpr) expr.getInputs().get(1);
    assertThat(length.getReturnType()).isInstanceOf(IntegerType.class);
    assertThat(((IntegerValue) length.getValue()).getValue()).isEqualTo(0);
  }

  @Test
  public void testNullLiteralConvertsToNullCarrier() {
    RexLiteral intervalLiteral =
        (RexLiteral)
            rexBuilder.makeNullLiteral(
                typeFactory.createSqlIntervalType(
                    new SqlIntervalQualifier(
                        TimeUnit.SECOND,
                        RelDataType.PRECISION_NOT_SPECIFIED,
                        TimeUnit.SECOND,
                        RelDataType.PRECISION_NOT_SPECIFIED,
                        SqlParserPos.ZERO)));
    assertThat(((VarCharValue) RexNodeConverter.toVariant(intervalLiteral)).getValue()).isNull();
    SqlTypeName[] names = {
      SqlTypeName.VARCHAR,
      SqlTypeName.INTEGER,
      SqlTypeName.BIGINT,
      SqlTypeName.DOUBLE,
      SqlTypeName.TIMESTAMP,
      SqlTypeName.DATE,
      SqlTypeName.SYMBOL,
    };
    for (SqlTypeName name : names) {
      RexLiteral literal = (RexLiteral) rexBuilder.makeNullLiteral(typeFactory.createSqlType(name));
      Variant variant = RexNodeConverter.toVariant(literal);
      assertThat(variant).isInstanceOf(VarCharValue.class);
      assertThat(((VarCharValue) variant).getValue()).isNull();
    }
  }

  @Test
  public void testDecimalNullLiteralConvertsToNullCarrier() {
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2));
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isNull();
  }
}
