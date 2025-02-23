use crate::{Field, RelationField, ScalarField};
use quaint::ast::Column;

pub trait AsColumn {
    fn as_column(&self) -> Column<'static>;
}

impl AsColumn for Field {
    fn as_column(&self) -> Column<'static> {
        match self {
            Field::Scalar(ref sf) => sf.as_column(),
            Field::Relation(ref rf) => rf.as_column(),
        }
    }
}

impl AsColumn for RelationField {
    fn as_column(&self) -> Column<'static> {
        let model = self.model();
        let internal_data_model = model.internal_data_model();
        let db_name = self.db_name();
        let parts = (
            (internal_data_model.db_name.clone(), model.db_name().to_string()),
            db_name.clone(),
        );

        parts.into()
    }
}

impl AsColumn for ScalarField {
    fn as_column(&self) -> Column<'static> {
        let db = self.internal_data_model().db_name.clone();
        let table = self.model().db_name().to_string();
        let col = self.db_name().to_string();

        Column::from(((db, table), col))
    }

}
