// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_types::seq_value::SeqV;

use super::CreateOption;
use crate::schema::database_id::DatabaseId;
use crate::schema::database_name_ident::DatabaseNameIdent;
use crate::share::share_name_ident::ShareNameIdentRaw;
use crate::share::ShareCredential;
use crate::share::ShareCredentialHmac;
use crate::share::ShareObject;
use crate::share::ShareSpec;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;
use crate::KeyWithTenant;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseInfo {
    pub database_id: DatabaseId,
    pub name_ident: DatabaseNameIdent,
    pub meta: SeqV<DatabaseMeta>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseIdToName {
    pub db_id: u64,
}

impl Display for DatabaseIdToName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.db_id)
    }
}

impl DatabaseIdToName {
    pub fn new(db_id: u64) -> Self {
        DatabaseIdToName { db_id }
    }
}

// see `ShareGrantObjectPrivilege`
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShareDbId {
    Usage(u64),
    Reference(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseMeta {
    pub engine: String,
    pub engine_options: BTreeMap<String, String>,
    pub options: BTreeMap<String, String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub comment: String,

    // if used in CreateDatabaseReq, this field MUST set to None.
    pub drop_on: Option<DateTime<Utc>>,
    // shared by share_id
    pub shared_by: BTreeSet<u64>,
    // from tenant.share_name
    pub from_share: Option<ShareNameIdentRaw>,
    // share endpoint name, create with `create share endpoint` ddl
    pub using_share_endpoint: Option<String>,
    // from share db id
    pub from_share_db_id: Option<ShareDbId>,
}

impl Default for DatabaseMeta {
    fn default() -> Self {
        DatabaseMeta {
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
            created_on: Utc::now(),
            updated_on: Utc::now(),
            comment: "".to_string(),
            drop_on: None,
            shared_by: BTreeSet::new(),
            from_share: None,
            using_share_endpoint: None,
            from_share_db_id: None,
        }
    }
}

impl Display for DatabaseMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Engine: {}={:?}, Options: {:?}, CreatedOn: {:?}",
            self.engine, self.engine_options, self.options, self.created_on
        )
    }
}

impl DatabaseInfo {
    pub fn engine(&self) -> &str {
        &self.meta.engine
    }

    /// Create a new database info without id or meta seq.
    ///
    /// Usually such an instance is used for an external database, whose metadata is not stored in databend meta-service.
    pub fn without_id_seq(name_ident: DatabaseNameIdent, meta: DatabaseMeta) -> Self {
        Self {
            database_id: DatabaseId::new(0),
            name_ident,
            meta: SeqV::new(0, meta),
        }
    }
}

/// Save db name id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, Default, PartialEq)]
pub struct DbIdList {
    pub id_list: Vec<u64>,
}

impl DbIdList {
    pub fn new() -> DbIdList {
        DbIdList::default()
    }

    pub fn len(&self) -> usize {
        self.id_list.len()
    }

    pub fn id_list(&self) -> &Vec<u64> {
        &self.id_list
    }

    pub fn append(&mut self, table_id: u64) {
        self.id_list.push(table_id);
    }

    pub fn is_empty(&self) -> bool {
        self.id_list.is_empty()
    }

    pub fn pop(&mut self) -> Option<u64> {
        self.id_list.pop()
    }

    pub fn last(&mut self) -> Option<&u64> {
        self.id_list.last()
    }
}

impl Display for DbIdList {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DB id list: {:?}", self.id_list)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDatabaseReq {
    pub create_option: CreateOption,
    pub name_ident: DatabaseNameIdent,
    pub meta: DatabaseMeta,
}

impl Display for CreateDatabaseReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.create_option {
            CreateOption::Create => write!(
                f,
                "create_db:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.database_name(),
                self.meta
            ),
            CreateOption::CreateIfNotExists => write!(
                f,
                "create_db_if_not_exists:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.database_name(),
                self.meta
            ),

            CreateOption::CreateOrReplace => write!(
                f,
                "create_or_replace_db:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.database_name(),
                self.meta
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDatabaseReply {
    pub db_id: DatabaseId,
    // if `share_specs` is not empty, it means that create database with replace option,
    // and `share_specs` vector save the share spec of original database
    pub share_specs: Option<Vec<ShareSpec>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameDatabaseReq {
    pub if_exists: bool,
    pub name_ident: DatabaseNameIdent,
    pub new_db_name: String,
}

impl Display for RenameDatabaseReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "rename_database:{}/{}=>{}",
            self.name_ident.tenant_name(),
            self.name_ident.database_name(),
            self.new_db_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ReplyShareObject {
    // (db id)
    Database(u64),
    // (db_id, table id)
    Table(u64, u64),
}

impl From<ShareObject> for ReplyShareObject {
    fn from(object: ShareObject) -> Self {
        match object {
            ShareObject::Database(_, db_id) => ReplyShareObject::Database(db_id),
            ShareObject::Table(_, db_id, table_id) => ReplyShareObject::Table(db_id, table_id),
            ShareObject::View(_, db_id, table_id) => ReplyShareObject::Table(db_id, table_id),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RenameDatabaseReply {
    pub share_spec: Option<(Vec<ShareSpec>, ReplyShareObject)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDatabaseReq {
    pub if_exists: bool,
    pub name_ident: DatabaseNameIdent,
}

impl Display for DropDatabaseReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "drop_db(if_exists={}):{}/{}",
            self.if_exists,
            self.name_ident.tenant_name(),
            self.name_ident.database_name(),
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropDatabaseReply {
    pub db_id: u64,
    // if `share_specs` is not empty, it means that create database with replace option,
    // and `share_specs` vector save the share spec of original database
    pub share_specs: Option<Vec<ShareSpec>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropDatabaseReq {
    pub name_ident: DatabaseNameIdent,
}

impl Display for UndropDatabaseReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "undrop_db:{}/{}",
            self.name_ident.tenant_name(),
            self.name_ident.database_name(),
        )
    }
}

impl UndropDatabaseReq {
    pub fn tenant(&self) -> &Tenant {
        self.name_ident.tenant()
    }
    pub fn db_name(&self) -> &str {
        self.name_ident.database_name()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UndropDatabaseReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDatabaseReq {
    pub inner: DatabaseNameIdent,
}

impl Deref for GetDatabaseReq {
    type Target = DatabaseNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GetDatabaseReq {
    pub fn new(tenant: impl ToTenant, db_name: impl ToString) -> GetDatabaseReq {
        GetDatabaseReq {
            inner: DatabaseNameIdent::new(tenant, db_name),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DatabaseInfoFilter {
    // include all dropped databases
    IncludeDropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDatabaseReq {
    pub tenant: Tenant,
    pub filter: Option<DatabaseInfoFilter>,
}

impl ListDatabaseReq {
    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ShareDBParams {
    pub share_ident: ShareNameIdentRaw,
    pub share_endpoint_url: String,
    pub share_endpoint_credential: ShareCredential,
}

impl ShareDBParams {
    pub fn new(share_ident: ShareNameIdentRaw) -> Self {
        Self {
            share_ident,
            share_endpoint_url: "".to_string(),
            share_endpoint_credential: ShareCredential::HMAC(ShareCredentialHmac {
                key: "".to_string(),
            }),
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::database_name_ident::DatabaseNameIdentRaw;
    use crate::schema::DatabaseId;
    use crate::schema::DatabaseIdToName;

    impl kvapi::KeyCodec for DatabaseIdToName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.db_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let db_id = parser.next_u64()?;
            Ok(Self { db_id })
        }
    }

    /// "__fd_database_id_to_name/<db_id> -> DatabaseNameIdent"
    impl kvapi::Key for DatabaseIdToName {
        const PREFIX: &'static str = "__fd_database_id_to_name";

        type ValueType = DatabaseNameIdentRaw;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.db_id).to_string_key())
        }
    }

    impl kvapi::Value for DatabaseNameIdentRaw {
        type KeyType = DatabaseIdToName;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
