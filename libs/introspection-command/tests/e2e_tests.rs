#[macro_use]
extern crate lazy_static;

use std::path::Path;
use log::debug;
use database_introspection::sqlite::IntrospectionConnector;
use std::sync::Mutex;
use prisma_query::{ast::ParameterizedValue, connector::Queryable};
use std::sync::Arc;

mod common;
use common::*;

struct SqliteConnection {
    client: Mutex<prisma_query::connector::Sqlite>,
}

impl database_introspection::IntrospectionConnection for SqliteConnection {
    fn query_raw(
        &self,
        sql: &str,
        _schema: &str,
        params: &[ParameterizedValue],
    ) -> prisma_query::Result<prisma_query::connector::ResultSet> {
        self.client.lock().expect("self.client.lock").query_raw(sql, params)
    }
}

pub fn get_sqlite_connector() -> IntrospectionConnector {
    let db_fpath = Path::new(module_path!()).parent().unwrap().join("resources/dev.db");
    debug!("Database file path: {:?}", db_fpath);

    let queryable =
        prisma_query::connector::Sqlite::new(db_fpath).expect("opening prisma_query::connector::Sqlite");
    let int_conn = Arc::new(SqliteConnection {
        client: Mutex::new(queryable),
    });
    IntrospectionConnector::new(int_conn)
}

#[test]
fn a_sqlite_database_can_be_introspected() {
    setup();


}
