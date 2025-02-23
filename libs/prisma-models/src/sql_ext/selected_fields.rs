use crate::{AsColumn, RelationExt, Relation, SelectedFields};
use quaint::ast::Column;

pub trait SelectedFieldsExt {
    fn columns(&self) -> Vec<Column<'static>>;
}

impl SelectedFieldsExt for SelectedFields {
    fn columns(&self) -> Vec<Column<'static>> {
        let mut result: Vec<Column<'static>> = self.scalar_non_list().iter().map(|f| f.as_column()).collect();

        for rf in self.relation_inlined().iter() {
            result.push(rf.as_column());
        }

        if let Some(ref from_field) = self.from_field {
            let relation = from_field.relation();

            result.push(
                relation
                    .column_for_relation_side(from_field.relation_side.opposite())
                    .alias(SelectedFields::RELATED_MODEL_ALIAS)
                    .table(Relation::TABLE_ALIAS),
            );

            result.push(
                relation
                    .column_for_relation_side(from_field.relation_side)
                    .alias(SelectedFields::PARENT_MODEL_ALIAS)
                    .table(Relation::TABLE_ALIAS),
            );
        };

        result
    }
}
