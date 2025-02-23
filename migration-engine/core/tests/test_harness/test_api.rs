use super::{
    command_helpers::{run_infer_command, InferOutput},
    misc_helpers::{
        mysql_8_url, mysql_migration_connector, mysql_url, postgres_migration_connector, postgres_url,
        sqlite_migration_connector, test_api,
    },
    InferAndApplyOutput, SCHEMA_NAME,
};
use migration_connector::{MigrationPersistence, MigrationStep};
use migration_core::{
    api::GenericApi,
    commands::{ApplyMigrationInput, InferMigrationStepsInput, UnapplyMigrationInput, UnapplyMigrationOutput},
};
use quaint::prelude::SqlFamily;
use sql_connection::SyncSqlConnection;
use sql_schema_describer::*;
use std::sync::Arc;

/// A handle to all the context needed for end-to-end testing of the migration engine across
/// connectors.
pub struct TestApi {
    sql_family: SqlFamily,
    database: Arc<dyn SyncSqlConnection + Send + Sync + 'static>,
    api: Box<dyn GenericApi>,
}

impl TestApi {
    pub fn database(&self) -> &Arc<dyn SyncSqlConnection + Send + Sync + 'static> {
        &self.database
    }

    pub fn is_sqlite(&self) -> bool {
        self.sql_family == SqlFamily::Sqlite
    }

    pub fn migration_persistence(&self) -> Arc<dyn MigrationPersistence> {
        self.api.migration_persistence()
    }

    pub fn sql_family(&self) -> SqlFamily {
        self.sql_family
    }

    pub async fn apply_migration(&self, steps: Vec<MigrationStep>, migration_id: &str) -> InferAndApplyOutput {
        let input = ApplyMigrationInput {
            migration_id: migration_id.to_string(),
            steps,
            force: None,
        };

        let migration_output = self.api.apply_migration(&input).expect("ApplyMigration failed");

        assert!(
            migration_output.general_errors.is_empty(),
            format!(
                "ApplyMigration returned unexpected errors: {:?}",
                migration_output.general_errors
            )
        );

        InferAndApplyOutput {
            sql_schema: self.introspect_database(),
            migration_output,
        }
    }

    pub async fn infer_and_apply(&self, datamodel: &str) -> InferAndApplyOutput {
        let migration_id = "the-migration-id";

        self.infer_and_apply_with_migration_id(datamodel, migration_id).await
    }

    pub async fn infer_and_apply_with_migration_id(&self, datamodel: &str, migration_id: &str) -> InferAndApplyOutput {
        let input = InferMigrationStepsInput {
            migration_id: migration_id.to_string(),
            datamodel: datamodel.to_string(),
            assume_to_be_applied: Vec::new(),
        };

        let steps = self.run_infer_command(input).await.0.datamodel_steps;

        self.apply_migration(steps, migration_id).await
    }

    pub async fn run_infer_command(&self, input: InferMigrationStepsInput) -> InferOutput {
        run_infer_command(self.api.as_ref(), input)
    }

    pub async fn unapply_migration(&self) -> UnapplyOutput {
        let input = UnapplyMigrationInput {};
        let output = self.api.unapply_migration(&input).unwrap();

        let sql_schema = self.introspect_database();

        UnapplyOutput { sql_schema, output }
    }

    pub fn barrel(&self) -> BarrelMigrationExecutor {
        BarrelMigrationExecutor {
            inspector: self.inspector(),
            database: Arc::clone(&self.database),
            sql_variant: match self.sql_family {
                SqlFamily::Mysql => barrel::SqlVariant::Mysql,
                SqlFamily::Postgres => barrel::SqlVariant::Pg,
                SqlFamily::Sqlite => barrel::SqlVariant::Sqlite,
            },
        }
    }

    fn inspector(&self) -> Box<dyn SqlSchemaDescriberBackend> {
        match self.api.connector_type() {
            "postgresql" => Box::new(sql_schema_describer::postgres::SqlSchemaDescriber::new(Arc::clone(
                &self.database,
            ))),
            "sqlite" => Box::new(sql_schema_describer::sqlite::SqlSchemaDescriber::new(Arc::clone(
                &self.database,
            ))),
            "mysql" => Box::new(sql_schema_describer::mysql::SqlSchemaDescriber::new(Arc::clone(
                &self.database,
            ))),
            _ => unimplemented!(),
        }
    }

    fn introspect_database(&self) -> SqlSchema {
        let mut result = self
            .inspector()
            .describe(&SCHEMA_NAME.to_string())
            .expect("Introspection failed");

        // the presence of the _Migration table makes assertions harder. Therefore remove it from the result.
        result.tables = result.tables.into_iter().filter(|t| t.name != "_Migration").collect();

        result
    }
}

pub fn mysql_8_test_api() -> TestApi {
    let connector = mysql_migration_connector(&mysql_8_url());

    TestApi {
        sql_family: SqlFamily::Mysql,
        database: Arc::clone(&connector.database),
        api: Box::new(test_api(connector)),
    }
}

pub fn mysql_test_api() -> TestApi {
    let connector = mysql_migration_connector(&mysql_url());

    TestApi {
        sql_family: SqlFamily::Mysql,
        database: Arc::clone(&connector.database),
        api: Box::new(test_api(connector)),
    }
}

pub fn postgres_test_api() -> TestApi {
    let connector = postgres_migration_connector(&postgres_url());

    TestApi {
        sql_family: SqlFamily::Postgres,
        database: Arc::clone(&connector.database),
        api: Box::new(test_api(connector)),
    }
}

pub fn sqlite_test_api() -> TestApi {
    let connector = sqlite_migration_connector();

    TestApi {
        sql_family: SqlFamily::Sqlite,
        database: Arc::clone(&connector.database),
        api: Box::new(test_api(connector)),
    }
}

pub struct BarrelMigrationExecutor {
    inspector: Box<dyn SqlSchemaDescriberBackend>,
    database: Arc<dyn SyncSqlConnection + Send + Sync>,
    sql_variant: barrel::backend::SqlVariant,
}

impl BarrelMigrationExecutor {
    pub fn execute<F>(&self, mut migration_fn: F) -> SqlSchema
    where
        F: FnMut(&mut barrel::Migration) -> (),
    {
        use barrel::Migration;

        let mut migration = Migration::new().schema(SCHEMA_NAME);
        migration_fn(&mut migration);
        let full_sql = migration.make_from(self.sql_variant);
        run_full_sql(&self.database, &full_sql);
        let mut result = self
            .inspector
            .describe(&SCHEMA_NAME.to_string())
            .expect("Introspection failed");

        // The presence of the _Migration table makes assertions harder. Therefore remove it.
        result.tables = result.tables.into_iter().filter(|t| t.name != "_Migration").collect();
        result
    }
}

fn run_full_sql(database: &Arc<dyn SyncSqlConnection + Send + Sync>, full_sql: &str) {
    for sql in full_sql.split(";").filter(|sql| !sql.is_empty()) {
        database.query_raw(&sql, &[]).unwrap();
    }
}

#[derive(Debug)]
pub struct UnapplyOutput {
    pub sql_schema: SqlSchema,
    pub output: UnapplyMigrationOutput,
}
