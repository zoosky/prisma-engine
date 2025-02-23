mod base_query;
mod row_number;
mod union_all;

pub use base_query::*;
pub use row_number::*;
pub use union_all::*;

use crate::ordering::Ordering;
use prisma_models::*;
use quaint::ast::{Comparable, Conjuctive, Query};

pub trait ManyRelatedRecordsQueryBuilder {
    const BASE_TABLE_ALIAS: &'static str = "prismaBaseTableAlias";
    const ROW_NUMBER_ALIAS: &'static str = "prismaRowNumberAlias";
    const ROW_NUMBER_TABLE_ALIAS: &'static str = "prismaRowNumberTableAlias";

    fn with_pagination<'a>(base: ManyRelatedRecordsBaseQuery<'a>) -> Query;

    fn without_pagination<'a>(base: ManyRelatedRecordsBaseQuery<'a>) -> Query {
        let conditions = base
            .from_field
            .relation_column()
            .table(Relation::TABLE_ALIAS)
            .in_selection(base.from_record_ids.to_owned())
            .and(base.condition)
            .and(base.cursor);

        let opposite_column = base.from_field.opposite_column().table(Relation::TABLE_ALIAS);
        let order_columns = Ordering::internal(opposite_column, base.order_by.as_ref(), base.is_reverse_order);

        order_columns
            .into_iter()
            .fold(base.query.so_that(conditions), |acc, ord| acc.order_by(ord))
            .into()
    }
}
