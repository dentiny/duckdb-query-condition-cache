#include "predicate_key_utils.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include "duckdb/common/algorithm.hpp"

namespace duckdb {

unique_ptr<Expression> BindPredicate(ClientContext &context, DuckTableEntry &table_entry, const string &predicate_sql) {
	if (predicate_sql.empty() || predicate_sql.find_first_not_of(' ') == string::npos) {
		return nullptr;
	}
	auto parsed_exprs = Parser::ParseExpressionList(predicate_sql);
	if (parsed_exprs.empty()) {
		return nullptr;
	}
	auto binder = Binder::CreateBinder(context);
	physical_index_set_t bound_columns;
	CheckBinder check_binder(*binder, context, table_entry.name, table_entry.GetColumns(), bound_columns);
	return check_binder.Bind(parsed_exprs[0]);
}

// Comparison flip replicates ComparisonSimplificationRule::Apply()
// (duckdb/src/optimizer/rule/comparison_simplification.cpp) since
// ExpressionRewriter runs after our pre-optimize pass.
// Bottom-up: normalize children before sorting conjunction children.
void NormalizeExpressionForCacheKey(Expression &expr) {
	ExpressionIterator::EnumerateChildren(expr, [](Expression &child) { NormalizeExpressionForCacheKey(child); });

	// Normalize comparison operand order:
	// - constant and column: put constant on right
	// - both same kind (both foldable or both non-foldable): sort by ToString
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		bool left_foldable = comp.left->IsFoldable();
		bool right_foldable = comp.right->IsFoldable();
		if (left_foldable && !right_foldable) {
			// Constant on left, column on right -> swap
			std::swap(comp.left, comp.right);
			comp.type = FlipComparisonExpression(comp.type);
		} else if (left_foldable == right_foldable) {
			// Both same kind -> sort by ToString for canonical ordering
			if (comp.left->ToString() > comp.right->ToString()) {
				std::swap(comp.left, comp.right);
				comp.type = FlipComparisonExpression(comp.type);
			}
		}
		return;
	}

	// Sort conjunction children for canonical ordering
	// TODO: ToString() is called O(NlogN) times during sort
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		sort(conj.children.begin(), conj.children.end(),
		     [](const unique_ptr<Expression> &a, const unique_ptr<Expression> &b) {
			     return a->ToString() < b->ToString();
		     });
	}
}

string ComputeCanonicalPredicateKey(ClientContext &context, DuckTableEntry &table_entry, const string &predicate_sql) {
	auto bound_expr = BindPredicate(context, table_entry, predicate_sql);
	if (!bound_expr) {
		return "";
	}
	NormalizeExpressionForCacheKey(*bound_expr);
	return bound_expr->ToString();
}

} // namespace duckdb
