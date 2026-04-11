#include "predicate_key_utils.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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

	// Sort conjunction children for canonical ordering.
	// Pre-compute ToString() for each child.
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		vector<pair<string, idx_t>> keyed; // (ToString() result, child index)
		keyed.reserve(conj.children.size());
		for (idx_t i = 0; i < conj.children.size(); ++i) {
			keyed.emplace_back(conj.children[i]->ToString(), i);
		}
		sort(keyed.begin(), keyed.end(),
		     [](const pair<string, idx_t> &a, const pair<string, idx_t> &b) { return a.first < b.first; });
		vector<unique_ptr<Expression>> sorted_children;
		sorted_children.reserve(keyed.size());
		for (auto &[str, idx] : keyed) {
			sorted_children.push_back(std::move(conj.children[idx]));
		}
		conj.children = std::move(sorted_children);
	}
}

namespace {

// Fill BoundReferenceExpression aliases with column names so ToString prints
// "val" instead of "#1", matching the plan binder output.
void SetReferenceAliases(Expression &expr, DuckTableEntry &table_entry) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		if (ref.alias.empty()) {
			for (const auto &col : table_entry.GetColumns().Physical()) {
				if (col.Oid() == ref.index) {
					ref.alias = col.Name();
					break;
				}
			}
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { SetReferenceAliases(child, table_entry); });
}

} // namespace

unique_ptr<Expression> CombineWithAnd(vector<unique_ptr<Expression>> children) {
	D_ASSERT(!children.empty());
	if (children.size() == 1) {
		return std::move(children[0]);
	}
	auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	conj->children = std::move(children);
	return std::move(conj);
}

string ComputeCanonicalPredicateKey(ClientContext &context, DuckTableEntry &table_entry, const string &predicate_sql) {
	auto bound_expr = BindPredicate(context, table_entry, predicate_sql);
	if (!bound_expr) {
		return "";
	}
	// Strip CheckBinder's outer INTEGER cast and restore column aliases so the
	// result matches the plan binder output for the same predicate.
	if (bound_expr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast = bound_expr->Cast<BoundCastExpression>();
		if (cast.return_type.id() == LogicalTypeId::INTEGER) {
			bound_expr = std::move(cast.child);
		}
	}
	SetReferenceAliases(*bound_expr, table_entry);
	NormalizeExpressionForCacheKey(*bound_expr);
	return bound_expr->ToString();
}

string ComputeCanonicalPredicateKey(const vector<unique_ptr<Expression>> &expressions) {
	if (expressions.empty()) {
		return "";
	}
	// Clone so we don't mutate the plan's expressions during normalization.
	vector<unique_ptr<Expression>> cloned;
	cloned.reserve(expressions.size());
	for (const auto &expr : expressions) {
		cloned.push_back(expr->Copy());
	}
	auto combined = CombineWithAnd(std::move(cloned));
	NormalizeExpressionForCacheKey(*combined);
	return combined->ToString();
}

} // namespace duckdb
