use crate::prelude::*;
use once_cell::sync::OnceCell;
use std::sync::{Arc, Weak};

pub type RelationRef = Arc<Relation>;
pub type RelationWeakRef = Weak<Relation>;

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OnDelete {
    SetNull,
    Cascade,
}

impl OnDelete {
    pub fn is_cascade(self) -> bool {
        match self {
            OnDelete::Cascade => true,
            OnDelete::SetNull => false,
        }
    }

    pub fn is_set_null(self) -> bool {
        match self {
            OnDelete::Cascade => false,
            OnDelete::SetNull => true,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InlineRelation {
    #[serde(rename = "inTableOfModelId")]
    pub in_table_of_model_name: String,
    pub referencing_column: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RelationTable {
    pub table: String,
    pub model_a_column: String,
    pub model_b_column: String,
    pub id_column: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case", tag = "relationManifestationType")]
pub enum RelationLinkManifestation {
    Inline(InlineRelation),
    RelationTable(RelationTable),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationTemplate {
    pub name: String,
    pub model_a_on_delete: OnDelete,
    pub model_b_on_delete: OnDelete,
    pub manifestation: Option<RelationLinkManifestation>, // TODO: remove the option after the switch to v2 is completed

    #[serde(rename = "modelAId")]
    pub model_a_name: String,
    #[serde(rename = "modelBId")]
    pub model_b_name: String,
}

/// A relation between two models. Can be either using a `RelationTable` or
/// model a direct link between two `RelationField`s.
#[derive(DebugStub)]
pub struct Relation {
    pub name: String,

    model_a_name: String,
    model_b_name: String,

    pub model_a_on_delete: OnDelete,
    pub model_b_on_delete: OnDelete,

    model_a: OnceCell<ModelWeakRef>,
    model_b: OnceCell<ModelWeakRef>,

    field_a: OnceCell<Weak<RelationField>>,
    field_b: OnceCell<Weak<RelationField>>,

    pub manifestation: Option<RelationLinkManifestation>,

    #[debug_stub = "#InternalDataModelWeakRef#"]
    pub internal_data_model: InternalDataModelWeakRef,
}

impl RelationTemplate {
    pub fn build(self, internal_data_model: InternalDataModelWeakRef) -> RelationRef {
        let relation = Relation {
            name: self.name,
            manifestation: self.manifestation,
            model_a_name: self.model_a_name,
            model_b_name: self.model_b_name,
            model_a_on_delete: self.model_a_on_delete,
            model_b_on_delete: self.model_b_on_delete,
            model_a: OnceCell::new(),
            model_b: OnceCell::new(),
            field_a: OnceCell::new(),
            field_b: OnceCell::new(),
            internal_data_model,
        };

        Arc::new(relation)
    }
}

impl Relation {
    pub const MODEL_A_DEFAULT_COLUMN: &'static str = "A";
    pub const MODEL_B_DEFAULT_COLUMN: &'static str = "B";
    pub const TABLE_ALIAS: &'static str = "RelationTable";

    /// Returns `true` only if the `Relation` is just a link between two
    /// `RelationField`s.
    pub fn is_inline_relation(&self) -> bool {
        self.manifestation
            .as_ref()
            .map(|manifestation| match manifestation {
                RelationLinkManifestation::Inline(_) => true,
                _ => false,
            })
            .unwrap_or(false)
    }

    /// Returns `true` if the `Relation` is a table linking two models.
    pub fn is_relation_table(&self) -> bool {
        !self.is_inline_relation()
    }

    /// A model that relates to itself. For example a `Person` that is a parent
    /// can relate to people that are children.
    pub fn is_self_relation(&self) -> bool {
        self.model_a_name == self.model_b_name
    }

    /// A pointer to the first `Model` in the `Relation`.
    pub fn model_a(&self) -> ModelRef {
        self.model_a
            .get_or_init(|| {
                let model = self.internal_data_model().find_model(&self.model_a_name).unwrap();
                Arc::downgrade(&model)
            })
            .upgrade()
            .expect("Model A deleted without deleting the relations in internal_data_model.")
    }

    /// A pointer to the second `Model` in the `Relation`.
    pub fn model_b(&self) -> ModelRef {
        self.model_b
            .get_or_init(|| {
                let model = self.internal_data_model().find_model(&self.model_b_name).unwrap();
                Arc::downgrade(&model)
            })
            .upgrade()
            .expect("Model B deleted without deleting the relations in internal_data_model.")
    }

    /// A pointer to the `RelationField` in the first `Model` in the `Relation`.
    pub fn field_a(&self) -> RelationFieldRef {
        self.field_a
            .get_or_init(|| {
                let field = self
                    .model_a()
                    .fields()
                    .find_from_relation(&self.name, RelationSide::A)
                    .unwrap();

                Arc::downgrade(&field)
            })
            .upgrade()
            .expect("Field A deleted without deleting the relations in internal_data_model.")
    }

    /// A pointer to the `RelationField` in the second `Model` in the `Relation`.
    pub fn field_b(&self) -> RelationFieldRef {
        self.field_b
            .get_or_init(|| {
                let field = self
                    .model_b()
                    .fields()
                    .find_from_relation(&self.name, RelationSide::B)
                    .unwrap();

                Arc::downgrade(&field)
            })
            .upgrade()
            .expect("Field B deleted without deleting the relations in internal_data_model.")
    }

    /// Practically deprecated with Prisma 2.
    pub fn is_many_to_many(&self) -> bool {
        self.field_a().is_list && self.field_b().is_list
    }

    pub fn is_one_to_one(&self) -> bool {
        !self.field_a().is_list && !self.field_b().is_list
    }

    pub fn is_one_to_many(&self) -> bool {
        !self.is_many_to_many() && !self.is_one_to_one()
    }

    pub fn relation_table_has_3_columns(&self) -> bool {
        use RelationLinkManifestation::*;

        match self.manifestation {
            None => true,
            Some(RelationTable(ref m)) => m.id_column.as_ref().map(|_| true).unwrap_or(false),
            _ => false,
        }
    }

    pub fn contains_the_model(&self, model: ModelRef) -> bool {
        self.model_a().name == model.name || self.model_b().name == model.name
    }

    pub fn get_field_on_model(&self, model_id: &str) -> DomainResult<Arc<RelationField>> {
        if model_id == self.model_a().name {
            Ok(self.field_a())
        } else if model_id == self.model_b().name {
            Ok(self.field_b())
        } else {
            Err(DomainError::ModelForRelationNotFound {
                model_id: model_id.to_string(),
                relation: self.name.clone(),
            })
        }
    }

    pub fn inline_manifestation(&self) -> Option<&InlineRelation> {
        use RelationLinkManifestation::*;

        match self.manifestation {
            Some(Inline(ref m)) => Some(m),
            _ => None,
        }
    }

    pub fn internal_data_model(&self) -> InternalDataModelRef {
        self.internal_data_model
            .upgrade()
            .expect("InternalDataModel does not exist anymore. Parent internal_data_model is deleted without deleting the child internal_data_model.")
    }
}
