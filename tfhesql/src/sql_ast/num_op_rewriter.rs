use super::{
    data_ident::DataIdent,
    data_value::DataValue,
    helpers::{
        make_a_and_b, make_a_gt_b, make_a_gteq_b, make_a_lt_b, make_a_lteq_b, make_a_or_b, make_binary_op, make_minus_a, not_binary_op, reflexive_binary_op
    },
};
use crate::{
    error::FheSqlError,
    sql_ast::{
        helpers::{make_a_cmp_to_num, SqlExprIdentifier, SqlExprUnaryOp, SqlExprValue},
        SqlExprDataType,
    },
};
use arrow_schema::Schema;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};

struct NumBinaryOpRewriter<'a> {
    schema_ref: &'a Schema,
}

impl<'a> NumBinaryOpRewriter<'a> {
    fn new(schema_ref: &'a Schema) -> Self {
        NumBinaryOpRewriter { schema_ref }
    }
    fn recursive_rewrite(
        &self,
        left: &mut Box<Expr>,
        op: &mut BinaryOperator,
        right: &mut Box<Expr>,
        negated: bool,
    ) -> Result<(), FheSqlError> {
        let num_rw = NumRewriter::new();
        num_rw.rewrite(left)?;
        num_rw.rewrite(right)?;

        // Rewrite (-)X > (-)Y if needed
        self.rewrite_sign_op_sign(left, op, right)?;

        // Type may have changed!
        let left_type = left.data_type(self.schema_ref)?;
        let right_type = right.data_type(self.schema_ref)?;

        // Special case when both operands are Ascii
        // (Ascii > Ascii) is only negated if needed
        if left_type.is_ascii() && right_type.is_ascii() {
            // Not recursive
            return self.rewrite_ascii_op_ascii(left, op, right, negated);
        }

        // (Num|Ascii > Num|Ascii) is only negated if needed
        if left_type.is_integer_or_ascii() && right_type.is_integer_or_ascii() {
            // Not recursive
            return self.rewrite_num_op_num(left, op, right, negated);
        }

        // (Bool > Bool) is converted to AND+OR operators
        if left_type.is_bool() && right_type.is_bool() {
            // Recursive
            return self.recursive_rewrite_bool_op_bool(left, op, right, negated);
        }

        // (Num|Ascii > Bool) is converted to >+AND+OR operators
        if left_type.is_integer_or_ascii() {
            assert!(right_type.is_bool());
            // Recursive
            self.recursive_rewrite_num_op_bool(left, op, right, negated)
        } else if right_type.is_integer_or_ascii() {
            assert!(left_type.is_bool());

            *op = reflexive_binary_op(op);
            // Recursive
            self.recursive_rewrite_num_op_bool(right, op, left, negated)?;
            // put it back
            *op = reflexive_binary_op(op);
            Ok(())
        } else {
            panic!("Called NumOpRewriter::rewrite() with wrong arguments")
        }
    }

    /// -X > 123 := X < -123
    /// -X > -Y  := X < Y
    fn rewrite_sign_op_sign(
        &self,
        left: &mut Box<Expr>,
        op: &mut BinaryOperator,
        right: &mut Box<Expr>,
    ) -> Result<(), FheSqlError> {
        let left_has_minus = left.is_minus();
        let right_has_minus = right.is_minus();

        if left_has_minus {
            // -left
            let left_operand = left.try_get_minus_operand().unwrap();
            if right_has_minus {
                // -left > -right := left < right
                let right_operand = right.try_get_minus_operand().unwrap();
                *left = Box::new(left_operand.clone());
                *right = Box::new(right_operand.clone());
                *op = reflexive_binary_op(op);
            } else {
                // if right.is_value_expr() {
                //     // -left > 123 := left < -123
                //     let mut minus_right = make_minus_a(right.clone());
                //     let nrw = NumRewriter::new();
                //     nrw.rewrite(&mut minus_right)?;
                //     *left = left_operand.clone();
                //     *op = reflexive_binary_op(op);
                //     *right = minus_right;
                // } else {
                // Swap
                let mut minus_right = make_minus_a(right.clone());
                let nrw = NumRewriter::new();
                nrw.rewrite(&mut minus_right)?;
                *left = Box::new(left_operand.clone());
                *op = reflexive_binary_op(op);
                *right = minus_right;
            }
        } else if right_has_minus && left.is_value_expr() {
            let right_operand = right.try_get_minus_operand().unwrap();
            // 123 > -right := right > -123
            let mut minus_left = make_minus_a(left.clone());
            let nrw = NumRewriter::new();
            nrw.rewrite(&mut minus_left)?;
            *left = Box::new(right_operand.clone());
            // *op = op
            *right = minus_left;
        }

        Ok(())
    }

    /// Ascii(X) > Ascii(Y)
    fn rewrite_ascii_op_ascii(
        &self,
        _ascii_left: &mut Box<Expr>,
        op: &mut BinaryOperator,
        _ascii_right: &mut Box<Expr>,
        negated: bool,
    ) -> Result<(), FheSqlError> {
        if negated {
            *op = not_binary_op(op);
        }
        Ok(())
    }

    fn assert_ident_or_value(the_expr: &Expr) {
        match the_expr {
            Expr::Identifier(_) => (),
            Expr::Value(_) => (),
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Minus => {
                    if !matches!(expr.as_ref(), Expr::Identifier(_)) {
                        panic!(
                            "Expr is not an Identifier, a Value or Minus Identifier: {}",
                            the_expr
                        )
                    }
                }
                _ => {
                    panic!(
                        "Expr is not an Identifier, a Value or Minus Identifier: {}",
                        the_expr
                    )
                }
            },
            _ => panic!(
                "Expr is not an Identifier, a Value or Minus Identifier: {}",
                the_expr
            ),
        }
    }

    /// Num(X) > Num(Y)
    fn rewrite_num_op_num(
        &self,
        num_left: &mut Box<Expr>,
        op: &mut BinaryOperator,
        num_right: &mut Box<Expr>,
        negated: bool,
    ) -> Result<(), FheSqlError> {
        // left must be : value | ident | -ident
        Self::assert_ident_or_value(num_left);
        // right must be : value | ident | -ident
        Self::assert_ident_or_value(num_right);

        if negated {
            *op = not_binary_op(&*op);
        }
        Ok(())
    }

    /// Bool(X) > Bool(Y)
    fn recursive_rewrite_bool_op_bool(
        &self,
        bool_left: &mut Box<Expr>,
        op: &mut BinaryOperator,
        bool_right: &mut Box<Expr>,
        negated: bool,
    ) -> Result<(), FheSqlError> {
        if negated {
            *op = not_binary_op(op);
        }

        // Do not rewrite 'bool_column_1 > bool_column_2'
        if bool_left.is_identifier_expr() && bool_right.is_identifier_expr() {
            return Ok(());
        }

        match op {
            BinaryOperator::Gt => {
                // toBool(X) AND !toBool(Y)
                recursive_to_bool(bool_left, self.schema_ref, false)?;
                recursive_to_bool(bool_right, self.schema_ref, true)?;
                *op = BinaryOperator::And;
                Ok(())
            }
            BinaryOperator::Lt => {
                recursive_to_bool(bool_left, self.schema_ref, true)?;
                recursive_to_bool(bool_right, self.schema_ref, false)?;
                // !toBool(X) AND toBool(Y)
                *op = BinaryOperator::And;
                Ok(())
            }
            BinaryOperator::GtEq => {
                // toBool(X) OR !toBool(Y)
                recursive_to_bool(bool_left, self.schema_ref, false)?;
                recursive_to_bool(bool_right, self.schema_ref, true)?;
                *op = BinaryOperator::Or;
                Ok(())
            }
            BinaryOperator::LtEq => {
                // !toBool(X) OR toBool(Y)
                recursive_to_bool(bool_left, self.schema_ref, true)?;
                recursive_to_bool(bool_right, self.schema_ref, false)?;
                *op = BinaryOperator::Or;
                Ok(())
            }
            BinaryOperator::Eq => {
                // toBool(X) AND toBool(Y) OR !toBool(X) AND !toBool(Y)
                let mut bool_x = bool_left.clone();
                let mut bool_y = bool_right.clone();
                let mut not_bool_x = bool_left.clone();
                let mut not_bool_y = bool_right.clone();
                recursive_to_bool(&mut bool_x, self.schema_ref, false)?;
                recursive_to_bool(&mut bool_y, self.schema_ref, false)?;
                recursive_to_bool(&mut not_bool_x, self.schema_ref, true)?;
                recursive_to_bool(&mut not_bool_y, self.schema_ref, true)?;

                *bool_left = make_a_and_b(bool_x, bool_y);
                *op = BinaryOperator::Or;
                *bool_right = make_a_and_b(not_bool_x, not_bool_y);

                Ok(())
            }
            BinaryOperator::NotEq => {
                // toBool(X) OR toBool(Y) AND !toBool(X) OR !toBool(Y)
                let mut bool_x = bool_left.clone();
                let mut bool_y = bool_right.clone();
                let mut not_bool_x = bool_left.clone();
                let mut not_bool_y = bool_right.clone();
                recursive_to_bool(&mut bool_x, self.schema_ref, false)?;
                recursive_to_bool(&mut bool_y, self.schema_ref, false)?;
                recursive_to_bool(&mut not_bool_x, self.schema_ref, true)?;
                recursive_to_bool(&mut not_bool_y, self.schema_ref, true)?;

                *bool_left = make_a_or_b(bool_x, bool_y);
                *op = BinaryOperator::And;
                *bool_right = make_a_or_b(not_bool_x, not_bool_y);

                Ok(())
            }
            _ => Err(FheSqlError::unsupported_binary_op(op)),
        }
    }

    /// Num(X) > Bool(Y)
    fn recursive_rewrite_num_op_bool(
        &self,
        num_left: &mut Box<Expr>,
        op: &mut BinaryOperator,
        bool_right: &mut Box<Expr>,
        negated: bool,
    ) -> Result<(), FheSqlError> {
        if negated {
            *op = not_binary_op(op);
        }

        // Do not rewrite 'num_column_1 > bool_column_2'
        if num_left.is_identifier_expr() && bool_right.is_identifier_expr() {
            return Ok(());
        }

        let num_x = num_left.clone();
        let mut not_bool_y = bool_right.clone();
        let mut bool_y = bool_right.clone();
        recursive_to_bool(&mut bool_y, self.schema_ref, false)?;
        recursive_to_bool(&mut not_bool_y, self.schema_ref, true)?;

        // toNum(X) >= toBool(Y) := toNum(X) >= 1 || (toNum(X) >= 0 AND !toBool(Y))
        // toNum(X) >  toBool(Y) := toNum(X) >  1 || (toNum(X) >  0 AND !toBool(Y))
        // toNum(X) <= toBool(Y) := toNum(X) <= 0 || (toNum(X) <= 1 AND  toBool(Y))
        // toNum(X) <  toBool(Y) := toNum(X) <  0 || (toNum(X) <  1 AND  toBool(Y))
        // toNum(X) == toBool(Y) := toNum(X) == 0 AND !toBool(Y) || toNum(X) == 1 AND toBool(Y)
        // toNum(X) != toBool(Y) := toNum(X) == 0 AND toBool(Y)  || toNum(X) == 1 AND !toBool(Y)

        match op {
            // (toNum(X) > 1) || (toNum(X) = 1 AND !toBool(Y))
            BinaryOperator::Gt => {
                // toNum(X) > 1
                *num_left = make_a_cmp_to_num(num_x.clone(), BinaryOperator::Gt, 1);
                // OR
                *op = BinaryOperator::Or;
                // toNum(X) > 0 AND !toBool(Y)
                *bool_right =
                    make_a_and_b(make_a_cmp_to_num(num_x, BinaryOperator::Eq, 1), not_bool_y);
                Ok(())
            }
            // (toNum(X) >= 1) || (toNum(X) = 0 AND !toBool(Y))
            BinaryOperator::GtEq => {
                // toNum(X) > 1
                *num_left = make_a_cmp_to_num(num_x.clone(), BinaryOperator::GtEq, 1);
                // OR
                *op = BinaryOperator::Or;
                // toNum(X) > 0 AND !toBool(Y)
                *bool_right =
                    make_a_and_b(make_a_cmp_to_num(num_x, BinaryOperator::Eq, 0), not_bool_y);
                Ok(())
            }
            // (toNum(X) < 0) || (toNum(X) = 0 AND toBool(Y))
            BinaryOperator::Lt => {
                // toNum(X) < 0
                *num_left = make_a_cmp_to_num(num_x.clone(), BinaryOperator::Lt, 0);
                // OR
                *op = BinaryOperator::Or;
                // toNum(X) = 0 AND toBool(Y)
                *bool_right = make_a_and_b(make_a_cmp_to_num(num_x, BinaryOperator::Eq, 0), bool_y);
                Ok(())
            }
            // (toNum(X) <= 0) || (toNum(X) = 1 AND toBool(Y))
            BinaryOperator::LtEq => {
                // toNum(X) <= 0
                *num_left = make_a_cmp_to_num(num_x.clone(), BinaryOperator::LtEq, 0);
                // OR
                *op = BinaryOperator::Or;
                // toNum(X) = 1 AND toBool(Y)
                *bool_right = make_a_and_b(make_a_cmp_to_num(num_x, BinaryOperator::Eq, 1), bool_y);
                Ok(())
            }
            // toNum(X) == 0 AND !toBool(Y) || toNum(X) == 1 AND toBool(Y)
            BinaryOperator::Eq => {
                // toNum(X) == 0 AND !toBool(Y)
                *num_left = make_a_and_b(
                    make_a_cmp_to_num(num_x.clone(), BinaryOperator::Eq, 0),
                    not_bool_y,
                );
                // OR
                *op = BinaryOperator::Or;
                // toNum(X) == 1 AND toBool(Y)
                *bool_right = make_a_and_b(make_a_cmp_to_num(num_x, BinaryOperator::Eq, 1), bool_y);
                Ok(())
            }
            // toNum(X) == 0 AND toBool(Y) || toNum(X) == 1 AND !toBool(Y)
            BinaryOperator::NotEq => {
                // toNum(X) == 0 AND !toBool(Y)
                *num_left = make_a_and_b(
                    make_a_cmp_to_num(num_x.clone(), BinaryOperator::Eq, 0),
                    not_bool_y,
                );
                // OR
                *op = BinaryOperator::Or;
                // toNum(X) == 1 AND toBool(Y)
                *bool_right = make_a_and_b(make_a_cmp_to_num(num_x, BinaryOperator::Eq, 1), bool_y);
                Ok(())
            }
            _ => Err(FheSqlError::unsupported_binary_op(op)),
        }
    }
}

pub fn rewrite_where_expr_in_place(
    where_expr: &mut Box<Expr>,
    schema: &Schema,
) -> Result<(), FheSqlError> {
    recursive_to_bool(where_expr, schema, false)
}

fn or_2_by_2(list: &[Expr], negated: bool) -> Vec<Expr> {
    assert!(list.len() > 1);
    let n = list.len();
    let mut i = 0;
    let mut v = Vec::<Expr>::new();

    let logic_op = if negated {
        BinaryOperator::And
    } else {
        BinaryOperator::Or
    };

    loop {
        if i >= n {
            break;
        }
        if i == n - 1 {
            v.push(list[i].clone());
            break;
        } else {
            v.push(Expr::BinaryOp {
                left: Box::new(list[i].clone()),
                op: logic_op.clone(),
                right: Box::new(list[i + 1].clone()),
            });
            i += 2;
        }
    }
    v
}

fn list_to_eq(expr: &Expr, list: &[Expr], negated: bool) -> Vec<Expr> {
    let cmp_op = if negated {
        BinaryOperator::NotEq
    } else {
        BinaryOperator::Eq
    };

    list.iter()
        .map(|x| Expr::BinaryOp {
            left: Box::new(expr.clone()),
            op: cmp_op.clone(),
            right: Box::new(x.clone()),
        })
        .collect()
}

fn rewrite_in_list(expr: &Expr, list: &[Expr], negated: bool) -> Box<Expr> {
    //<expr> [ NOT ] IN (val1, val2, ...)
    if list.is_empty() {
        return Box::new(Expr::Value(Value::Boolean(false)));
    }
    let mut v = list_to_eq(expr, list, negated);
    loop {
        if v.len() == 1 {
            return Box::<Expr>::new(v[0].clone());
        }
        v = or_2_by_2(&v, negated);
    }
}

fn rewrite_between(expr: &Expr, negated: bool, low: &Expr, high: &Expr) -> Box<Expr> {
    //<expr> [ NOT ] BETWEEN <low> AND <high>
    if !negated {
        // low <= high
        let low_lteq_high = make_a_lteq_b(low, high);
        // low >= high
        let low_gteq_high = make_a_gteq_b(low, high);
        // expr >= low
        let expr_gteq_low = make_a_gteq_b(expr, low);
        // expr <= low
        let expr_lteq_low = make_a_lteq_b(expr, low);
        // expr >= high
        let expr_gteq_high = make_a_gteq_b(expr, high);
        // expr <= high
        let expr_lteq_high = make_a_lteq_b(expr, high);
        // (expr >= low AND expr <= high)
        let l_and_h = make_a_and_b(expr_gteq_low, expr_lteq_high);
        // (expr >= high AND expr <= low)
        let h_and_l = make_a_and_b(expr_gteq_high, expr_lteq_low);
        // (expr >= low AND expr <= high) AND (low <= high)
        let l_and_h_and_l_lteq_h = make_a_and_b(l_and_h, low_lteq_high);
        // (expr >= high AND expr <= low) AND (low >= high)
        let h_and_l_and_l_gteq_h = make_a_and_b(h_and_l, low_gteq_high);
        // ((expr >= low AND expr <= high) AND (low <= high)) OR ((expr >= high AND expr <= low) AND (high <= low))
        make_a_or_b(l_and_h_and_l_lteq_h, h_and_l_and_l_gteq_h)
    } else {
        // ((expr < low OR expr > high) OR (low > high)) AND ((expr < high OR expr > low) OR (high > low))
        // low < high
        let low_lt_high = make_a_lt_b(low, high);
        // low > high
        let low_gt_high = make_a_gt_b(low, high);
        // expr > low
        let expr_gt_low = make_a_gt_b(expr, low);
        // expr < low
        let expr_lt_low = make_a_lt_b(expr, low);
        // expr > high
        let expr_gt_high = make_a_gt_b(expr, high);
        // expr < high
        let expr_lt_high = make_a_lt_b(expr, high);
        // (expr < low OR expr > high)
        let l_or_h = make_a_or_b(expr_lt_low, expr_gt_high);
        // (expr < high OR expr > low)
        let h_or_l = make_a_or_b(expr_lt_high, expr_gt_low);
        // (expr < low OR expr > high) OR (low > high)
        let l_or_h_or_l_gt_h = make_a_or_b(l_or_h, low_gt_high);
        // (expr < high OR expr > low) OR (low < high)
        let h_or_l_or_l_lt_h = make_a_or_b(h_or_l, low_lt_high);
        make_a_and_b(l_or_h_or_l_gt_h, h_or_l_or_l_lt_h)
    }
}

fn recursive_to_bool(
    the_expr: &mut Box<Expr>,
    schema: &Schema,
    the_negated: bool,
) -> Result<(), FheSqlError> {
    match the_expr.as_mut() {
        Expr::Identifier(ident) => {
            let data_ident = DataIdent::try_from_ident(ident, schema)?;
            if data_ident.is_bool() {
                *the_expr.as_mut() = Expr::BinaryOp {
                    // toBool(BoolId) := (BoolId = true)
                    // toNotBool(BoolId) := (BoolId = false)
                    op: BinaryOperator::Eq,
                    left: the_expr.clone(),
                    right: Box::new(Expr::Value(Value::Boolean(!the_negated))),
                };
            } else if the_negated {
                // toNotBool(NumId) := (NumId = 0)
                *the_expr.as_mut() = Expr::BinaryOp {
                    op: BinaryOperator::Eq,
                    left: the_expr.clone(),
                    right: Box::new(Expr::Value(Value::Number("0".to_string(), false))),
                };
            } else {
                // toBool(NumId) := (NumId != 0)
                *the_expr.as_mut() = Expr::BinaryOp {
                    op: BinaryOperator::NotEq,
                    left: the_expr.clone(),
                    right: Box::new(Expr::Value(Value::Number("0".to_string(), false))),
                };
            }
            Ok(())
        }
        Expr::Value(value) => {
            let data_value = DataValue::try_from(&*value)?;
            let bool_value = if the_negated {
                data_value.not()
            } else {
                data_value.cast_to_bool()
            };
            *value = Value::Boolean(bool_value.get_bool());
            Ok(())
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            //<expr> [ NOT ] IN (val1, val2, ...)
            *the_expr = rewrite_in_list(expr, list, *negated);
            Ok(())
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            //<expr> [ NOT ] BETWEEN <low> AND <high>
            let mut neg = *negated;
            if the_negated {
                neg = !neg;
            }
            *the_expr = rewrite_between(expr, neg, low, high);
            recursive_to_bool(the_expr, schema, false)?;
            Ok(())
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                // Special case for :
                //  - SomeStringColumn = 'some string'
                //  - SomeStringColumn <> 'some string'
                match op {
                    BinaryOperator::Eq | BinaryOperator::NotEq => {
                        if left.is_utf8_identifier(schema) && right.is_string_value() {
                            if the_negated {
                                *the_expr = make_binary_op(&left, not_binary_op(op), &right);
                            }
                            return Ok(());
                        }
                        if right.is_utf8_identifier(schema) && left.is_string_value() {
                            if the_negated {
                                *the_expr = make_binary_op(&left, not_binary_op(op), &right);
                            }
                            return Ok(());
                        }
                    }
                    _ => (),
                }

                let num_rw = NumRewriter::new();
                num_rw.rewrite(left)?;
                num_rw.rewrite(right)?;

                if left == right {
                    match op {
                        BinaryOperator::Gt | BinaryOperator::Lt | BinaryOperator::NotEq => {
                            *the_expr = Box::new(Expr::Value(Value::Boolean(the_negated)));
                            return Ok(());
                        }
                        BinaryOperator::Eq => {
                            *the_expr = Box::new(Expr::Value(Value::Boolean(!the_negated)));
                            return Ok(());
                        }
                        _ => (),
                    }
                }

                // toBool(X) > toBool(Y) := toBool(X) AND NOT toBool(Y)
                // toNum(X)  > toNum(Y)  := toNum(X) > toNum(Y)
                // toNum(X)  > toBool(Y) := toNum(X) > 0 AND NOT toBool(Y) || toNum(X) > 1 AND toBool(Y)
                let rw = NumBinaryOpRewriter::new(schema);
                rw.recursive_rewrite(left, op, right, the_negated)
            }
            BinaryOperator::And | BinaryOperator::Or => {
                recursive_to_bool(left, schema, the_negated)?;
                recursive_to_bool(right, schema, the_negated)?;

                let mut not_left = left.clone();
                recursive_to_bool(&mut not_left, schema, true)?;

                if left == right {
                    if the_negated {
                        *the_expr = not_left;
                    } else {
                        *the_expr = left.clone();
                    }
                    return Ok(());
                }

                if &not_left == right {
                    if matches!(op, BinaryOperator::And) {
                        // column_1 AND NOT column_1
                        *the_expr = Box::new(Expr::Value(Value::Boolean(the_negated)));
                    } else {
                        // column_1 OR NOT column_1
                        *the_expr = Box::new(Expr::Value(Value::Boolean(!the_negated)));
                    }
                    return Ok(());
                }

                if the_negated {
                    *op = not_binary_op(op);
                }

                Ok(())
            }
            BinaryOperator::Xor => {
                todo!()
            }
            _ => Err(FheSqlError::unsupported_binary_op(op)),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Plus => {
                // toBool(+BoolExpr) := BoolExpr
                // toBool(+NumExpr) := toBool(NumExpr)
                // forward to_bool and ignore sign
                recursive_to_bool(expr, schema, the_negated)?;
                *the_expr = expr.clone();
                Ok(())
            }
            UnaryOperator::Minus => {
                // toBool(-BoolExpr) := BoolExpr
                // toBool(-NumExpr) := toBool(NumExpr)
                // forward to_bool and ignore sign
                recursive_to_bool(expr, schema, the_negated)?;
                *the_expr = expr.clone();
                Ok(())
            }
            UnaryOperator::Not => {
                // toNotBool(BoolExpr) := !BoolExpr
                // toNotBool(NumExpr) := !toBool(NumExpr)
                recursive_to_bool(expr, schema, !the_negated)?;
                *the_expr = expr.clone();
                Ok(())
            }
            _ => Err(FheSqlError::unsupported_expr(the_expr)),
        },
        _ => Err(FheSqlError::unsupported_expr(the_expr)),
    }
}

struct NumRewriter {}

impl NumRewriter {
    fn new() -> Self {
        NumRewriter {}
    }

    fn rewrite(&self, the_expr: &mut Expr) -> Result<(), FheSqlError> {
        self.cast_to_num(the_expr)
    }

    // caller has already interpreted the num_expr as a number
    fn cast_to_num(&self, the_expr: &mut Expr) -> Result<(), FheSqlError> {
        match the_expr {
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => {
                    // remove Plus unary op
                    self.cast_to_num(expr)?;
                    *the_expr = expr.as_ref().clone();
                    Ok(())
                }
                UnaryOperator::Minus => {
                    // remove Minus unary op if needed
                    let gob = self.rewrite_minus_num(expr)?;
                    if gob {
                        *the_expr = expr.as_ref().clone();
                    }
                    Ok(())
                }
                UnaryOperator::Not => Ok(()),
                _ => Err(FheSqlError::unsupported_expr(the_expr)),
            },
            Expr::Value(value) => {
                let data_value = DataValue::try_from(&*value)?;
                *value = Value::Number(data_value.cast_to_num().to_string(), false);
                Ok(())
            }
            Expr::Identifier(_)
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::BinaryOp { .. } => Ok(()),
            _ => Err(FheSqlError::unsupported_expr(the_expr)),
        }
    }

    fn rewrite_minus_num(&self, num_expr: &mut Expr) -> Result<bool, FheSqlError> {
        match num_expr {
            Expr::Identifier(_ident) => {
                *num_expr = Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(num_expr.clone()),
                };
                Ok(true)
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => {
                    // remove Plus unary op
                    // Minus(Plus) = Minus
                    self.rewrite_minus_num(expr)?;
                    *num_expr = expr.as_ref().clone();
                    Ok(true)
                }
                UnaryOperator::Minus => {
                    // remove Minus unary op
                    // Minus(Minus) = Plus
                    self.cast_to_num(expr)?;
                    *num_expr = expr.as_ref().clone();
                    Ok(true)
                }
                UnaryOperator::Not => Ok(false),
                _ => Err(FheSqlError::unsupported_expr(num_expr)),
            },
            Expr::Value(value) => {
                let data_value = DataValue::try_from(&*value)?;
                *value = Value::Number(data_value.minus().to_string(), false);
                Ok(true)
            }
            Expr::InList { .. } | Expr::Between { .. } | Expr::BinaryOp { .. } => Ok(false),
            _ => Err(FheSqlError::unsupported_expr(num_expr)),
        }
    }
}

// Optimisation range : Visitor push pop

#[cfg(test)]
mod test {
    use crate::sql_ast::to_parenthesized_string::*;
    use crate::sql_ast::*;

    use super::*;
    use arrow_schema::Field;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn get_schema() -> Schema {
        Schema::new(vec![
            Field::new("a_s", arrow_schema::DataType::Utf8, false),
            Field::new("b_s", arrow_schema::DataType::Utf8, false),
            Field::new("c_s", arrow_schema::DataType::Utf8, false),
            Field::new("d_s", arrow_schema::DataType::Utf8, false),
            Field::new("a_b", arrow_schema::DataType::Boolean, false),
            Field::new("b_b", arrow_schema::DataType::Boolean, false),
            Field::new("c_b", arrow_schema::DataType::Boolean, false),
            Field::new("d_b", arrow_schema::DataType::Boolean, false),
            Field::new("a_i", arrow_schema::DataType::Int32, false),
            Field::new("b_i", arrow_schema::DataType::Int32, false),
            Field::new("c_i", arrow_schema::DataType::Int32, false),
            Field::new("d_i", arrow_schema::DataType::Int32, false),
            Field::new("a_u", arrow_schema::DataType::UInt32, false),
            Field::new("b_u", arrow_schema::DataType::UInt32, false),
            Field::new("c_u", arrow_schema::DataType::UInt32, false),
            Field::new("d_u", arrow_schema::DataType::UInt32, false),
            Field::new("e_u", arrow_schema::DataType::UInt64, false),
            Field::new("f_u", arrow_schema::DataType::UInt64, false),
            Field::new("g_u", arrow_schema::DataType::UInt64, false),
            Field::new("h_u", arrow_schema::DataType::UInt64, false),
            Field::new("e_i", arrow_schema::DataType::Int64, false),
            Field::new("f_i", arrow_schema::DataType::Int64, false),
            Field::new("g_i", arrow_schema::DataType::Int64, false),
            Field::new("h_i", arrow_schema::DataType::Int64, false),
        ])
    }

    #[test]
    fn test() {
        let dialect = GenericDialect {}; // or AnsiDialect
        let schema = get_schema();
        let where_clauses = [
            "(a_b > a_u)",
            "(a_b > b_b)",
            "(a_b > b_s)",
            "(a_s > b_s)",
            "(a_s > b_i)",
            "(a_b > -a_u)",
            "(-a_b > -a_u)",
            "(-a_b > a_u)",
            "(-a_b > +a_u)",
            "(a_b > -(-a_u))",
            "(a_b > -(+(-a_u)))",
            "(a_b > -(-(+a_u)))",
            "(-(-(+a_b)) > -(-(+a_u)))",
            "(-a_b > -a_b)",
            "(a_b > a_b)",
            "(a_b > -a_b)",
            "(-a_b < -a_b)",
            "(a_b < a_b)",
            "(a_b = a_b)",
            "(a_b = -a_b)",
            "(a_b AND a_b)",
            "(a_b OR a_b)",
            "(a_i AND a_i)",
            "(a_i OR a_i)",
            "(a_i = a_i)",
            "(a_i AND NOT a_i)",
            "(NOT (a_i AND NOT a_i))",
            "((NOT a_i) AND NOT a_i)",
            "((NOT a_i) OR NOT a_i)",
            "(a_i OR NOT a_i)",
            "((a_i > b_i) OR NOT (a_i > b_i))",
            "((a_i > b_i) OR NOT (a_i > -(-b_i)))",
            "((a_i > b_i) OR NOT (a_i > -(-b_i))) AND ((a_i > b_i) OR NOT (a_i > b_i))",
            "((a_i > b_i) AND (a_i > b_i))",
            "((a_i > b_i) OR NOT (a_i > -(-b_i))) AND ((a_i > b_i) AND (a_i > b_i))",
            "a_u > (a_b > b_u)",
            "((a_u > b_u) AND a_b)",
            "(a_s > (a_b > a_u))",
            "b_u OR ((a_s > (a_b > a_u)) AND ((a_u > b_u) AND a_b))",
            "a_s = 'ab'",
            "(a_s IN ('ab', 'ac'))",
            "(a_s IN ('ab', 'ac', 'cd'))",
            "(a_s IN ('ab', 'ac', 'cd', 'ef'))",
            "(a_s IN ('ab'))",
            "(NOT(a_s = ''))",
            "((NOT (a_i NOT BETWEEN 50 AND 99)) AND NOT(a_s = ''))",
            "(NOT (a_s = b_s))",
            "(NOT (a_s <> b_s))",
            "(NOT (NOT (a_s = b_s)))",
            "(NOT(NOT(e_i >= -9223372036854775808)))",
            "(NOT(e_i >= -9223372036854775808))",
        ];
        let expected_result = [
            "(a_b > a_u)",
            "(a_b > b_b)",
            "(a_b > b_s)",
            "(a_s > b_s)",
            "(a_s > b_i)",
            "(a_b > (-a_u))",
            "(a_b < a_u)",
            "(a_b < (-a_u))",
            "(a_b < (-a_u))",
            "(a_b > a_u)",
            "(a_b > a_u)",
            "(a_b > a_u)",
            "(a_b > a_u)",
            "false",
            "false",
            "(a_b > (-a_b))",
            "false",
            "false",
            "true",
            "(a_b = (-a_b))",
            "(a_b = true)",
            "(a_b = true)",
            "(a_i <> 0)",
            "(a_i <> 0)",
            "true",
            "false",
            "true",
            "(a_i = 0)",
            "(a_i = 0)",
            "true",
            "true",
            "true",
            "true",
            "(a_i > b_i)",
            "(true AND (a_i > b_i))", //optimizer will handle the 'true'
            "((a_u > 1) OR ((a_u = 1) AND (a_b <= b_u)))",
            "((a_u > b_u) AND (a_b = true))",
            "((a_s > 1) OR ((a_s = 1) AND (a_b <= a_u)))",
            "((b_u <> 0) OR (((a_s > 1) OR ((a_s = 1) AND (a_b <= a_u))) AND ((a_u > b_u) AND (a_b = true))))",
            "(a_s = 'ab')",
            "((a_s = 'ab') OR (a_s = 'ac'))",
            "(((a_s = 'ab') OR (a_s = 'ac')) OR (a_s = 'cd'))",
            "(((a_s = 'ab') OR (a_s = 'ac')) OR ((a_s = 'cd') OR (a_s = 'ef')))",
            "(a_s = 'ab')",
            "(a_s <> '')",
            "(((((a_i >= 50) AND (a_i <= 99)) AND (50 <= 99)) OR (((a_i >= 99) AND (a_i <= 50)) AND (50 >= 99))) AND (a_s <> ''))",
            "(a_s <> b_s)",
            "(a_s = b_s)",
            "(a_s = b_s)",
            "(e_i >= -9223372036854775808)",
            "(e_i < -9223372036854775808)",
        ];
        where_clauses
            .iter()
            .zip(expected_result.iter())
            .for_each(|(w, e)| {
                let sql = format!("SELECT * FROM t WHERE {}", w);
                let statements = Parser::parse_sql(&dialect, &sql).unwrap();
                assert!(!statements.is_empty());
                let mut where_expr = statements[0].clone_where().unwrap();
                where_expr.as_mut().remove_nested_in_place().unwrap();
                recursive_to_bool(&mut where_expr, &schema, false).unwrap();
                if where_expr.to_parenthesized_string() != *e {
                    println!("FAILED {}", w);
                }
                assert_eq!(where_expr.to_parenthesized_string(), *e);
            });
    }

    #[test]
    fn test_one() {
        let _e = "";

        let w = "(NOT (a_s = b_s))";
        let _e = "(a_s <> b_s)";

        let where_str = _print_sql_test(w);
        println!("{}", where_str);

        if !_e.is_empty() {
            assert_eq!(where_str, *_e);
        }
    }

    fn _print_sql_test(where_stmt: &str) -> String {
        let dialect = GenericDialect {}; // or AnsiDialect
        let schema = get_schema();
        let sql = format!("SELECT * FROM t WHERE {}", where_stmt);
        let statements = Parser::parse_sql(&dialect, &sql).unwrap();
        assert!(!statements.is_empty());
        let mut where_expr = statements[0].clone_where().unwrap();
        where_expr.as_mut().remove_nested_in_place().unwrap();
        recursive_to_bool(&mut where_expr, &schema, false).unwrap();
        where_expr.to_parenthesized_string()
    }
}
