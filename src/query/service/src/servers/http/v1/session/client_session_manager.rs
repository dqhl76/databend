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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Thread;
use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::client_session::ClientSession;
use databend_common_meta_app::principal::user_token::QueryTokenInfo;
use databend_common_meta_app::principal::user_token::TokenType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_storages_common_session::drop_all_temp_tables;
use databend_storages_common_session::TempTblMgrRef;
use parking_lot::Mutex;
use parking_lot::RwLock;
use sha2::Digest;
use sha2::Sha256;
use tokio::time::Instant;

use crate::servers::http::v1::session::token::unix_ts;
use crate::servers::http::v1::SessionClaim;
use crate::sessions::Session;
use crate::sessions::SessionPrivilegeManager;

/// target TTL
pub const REFRESH_TOKEN_VALIDITY: Duration = Duration::from_hours(4);
pub const SESSION_TOKEN_VALIDITY: Duration = Duration::from_hours(1);

/// to cove network latency, retry and time skew
const TOKEN_TTL_DELAY: Duration = Duration::from_secs(300);
/// in case of client retry, shorted the ttl instead of drop at once
/// only required for refresh token.
const TOKEN_DROP_DELAY: Duration = Duration::from_secs(90);

pub struct TokenPair {
    pub refresh: String,
    pub session: String,
}

fn hash_token(token: &[u8]) -> String {
    hex::encode_upper(Sha256::digest(token))
}

enum QueryState {
    InUse,
    Idle(Instant),
}

struct SessionState {
    pub query_state: QueryState,
    pub temp_tbl_mgr: TempTblMgrRef,
}

impl QueryState {
    pub fn has_expired(&self, now: &Instant) -> bool {
        match self {
            QueryState::InUse => false,
            QueryState::Idle(t) => (*now - *t) > SESSION_TOKEN_VALIDITY,
        }
    }
}

pub struct ClientSessionManager {
    /// store hash only for hit ratio with limited memory, feasible because:
    /// - token contain all info in itself.
    /// - for eviction, LRU itself is enough, no need to check expired tokens specifically.
    session_tokens: RwLock<LruCache<String, Option<String>>>,
    refresh_tokens: RwLock<LruCache<String, Option<String>>>,

    /// add: write temp table
    /// rm:
    ///  - all temp table deleted
    ///  - session closed
    ///  - timeout
    session_state: Mutex<BTreeMap<String, SessionState>>,
}

impl ClientSessionManager {
    pub fn instance() -> Arc<ClientSessionManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    pub async fn init(_cfg: &InnerConfig) -> Result<()> {
        let mgr = Arc::new(Self {
            session_tokens: RwLock::new(LruCache::with_items_capacity(1024)),
            refresh_tokens: RwLock::new(LruCache::with_items_capacity(1024)),
            session_state: Default::default(),
        });
        GlobalInstance::set(mgr.clone());
        Thread::spawn(move || Self::check_timeout(mgr));
        Ok(())
    }

    async fn check_timeout(self: Arc<Self>) {
        loop {
            let now = Instant::now();
            let expired = {
                let guard = self.session_state.lock();
                guard
                    .iter()
                    .filter(|(_, state)| state.query_state.has_expired(&now))
                    .map(|(id, state)| (id.clone(), state.temp_tbl_mgr.clone()))
                    .collect::<Vec<_>>()
            };
            {
                let mut guard = self.session_state.lock();
                for (id, _) in expired.iter() {
                    guard.remove(id);
                }
            }
            for (id, mgr) in expired {
                drop_all_temp_tables(&id, mgr).await.ok();
            }
            tokio::time::sleep(SESSION_TOKEN_VALIDITY / 4).await;
        }
    }

    /// used for both issue token for new session and renew token for existing session.
    /// currently, when renewing, always return a new refresh token instead of update the TTL,
    /// since now we include expire time in token, which can not be updated.
    pub async fn new_token_pair(
        &self,
        session: &Arc<Session>,
        old_token_pair: Option<TokenPair>,
    ) -> Result<(String, TokenPair)> {
        // those infos are set when the request is authed
        let tenant = session.get_current_tenant();
        let tenant_name = tenant.tenant_name().to_string();
        let user = session.get_current_user()?.name;
        let auth_role = session.privilege_mgr().get_auth_role();

        let client_session_id = if let Some(old) = &old_token_pair {
            let claim = SessionClaim::decode(&old.refresh)?;
            assert_eq!(tenant_name, claim.tenant);
            assert_eq!(user, claim.user);
            assert_eq!(auth_role, claim.auth_role);
            claim.session_id
        } else {
            uuid::Uuid::new_v4().to_string()
        };

        let client_session_api = UserApiProvider::instance().client_session_api(&tenant);

        let now = unix_ts();
        let mut claim = SessionClaim::new(
            None,
            &tenant_name,
            &user,
            &auth_role,
            REFRESH_TOKEN_VALIDITY + SESSION_TOKEN_VALIDITY,
        );
        let refresh_token = if let Some(old) = &old_token_pair {
            old.refresh.clone()
        } else {
            claim.encode()
        };
        let refresh_token_hash = hash_token(refresh_token.as_bytes());
        let token_info = QueryTokenInfo {
            token_type: TokenType::Refresh,
            parent: None,
        };

        // by adding SESSION_TOKEN_VALIDITY_IN_SECS, avoid touch refresh token for each request.
        // note the ttl is not accurate, the TTL is in fact longer (by 0 to 1 hour) then expected.
        client_session_api
            .upsert_token(
                &refresh_token_hash,
                token_info.clone(),
                REFRESH_TOKEN_VALIDITY + SESSION_TOKEN_VALIDITY + TOKEN_TTL_DELAY,
                false,
            )
            .await?;
        client_session_api
            .upsert_client_session_id(
                &client_session_id,
                ClientSession {
                    user_name: claim.user.clone(),
                },
                REFRESH_TOKEN_VALIDITY + SESSION_TOKEN_VALIDITY + TOKEN_TTL_DELAY,
            )
            .await?;
        if let Some(old) = old_token_pair {
            client_session_api
                .upsert_token(
                    &old.refresh,
                    token_info,
                    REFRESH_TOKEN_VALIDITY + TOKEN_DROP_DELAY,
                    true,
                )
                .await?;
        };
        self.refresh_tokens
            .write()
            .insert(refresh_token_hash.clone(), None);

        claim.expire_at_in_secs = (now + SESSION_TOKEN_VALIDITY).as_secs();
        claim.nonce = uuid::Uuid::new_v4().to_string();

        let session_token = claim.encode();
        let session_token_hash = hash_token(session_token.as_bytes());
        let token_info = QueryTokenInfo {
            token_type: TokenType::Session,
            parent: Some(refresh_token_hash.clone()),
        };
        client_session_api
            .upsert_token(
                &session_token_hash,
                token_info,
                REFRESH_TOKEN_VALIDITY + TOKEN_TTL_DELAY,
                false,
            )
            .await?;
        self.session_tokens
            .write()
            .insert(session_token_hash, Some(refresh_token_hash));

        Ok((client_session_id, TokenPair {
            refresh: refresh_token,
            session: session_token,
        }))
    }

    pub(crate) async fn verify_token(
        self: &Arc<Self>,
        token: &str,
        token_type: TokenType,
    ) -> Result<SessionClaim> {
        let claim = SessionClaim::decode(token)?;
        let now = unix_ts().as_secs();
        if now > claim.expire_at_in_secs {
            return match token_type {
                TokenType::Refresh => Err(ErrorCode::RefreshTokenExpired("refresh token expired")),
                TokenType::Session => Err(ErrorCode::SessionTokenExpired("session token expired")),
            };
        }

        let hash = hash_token(token.as_bytes());
        let cache = match token_type {
            TokenType::Refresh => &self.refresh_tokens,
            TokenType::Session => &self.session_tokens,
        };

        if !cache.read().contains(&hash) {
            let tenant = Tenant::new_literal(&claim.tenant);
            match UserApiProvider::instance()
                .client_session_api(&tenant)
                .get_token(&hash)
                .await?
            {
                Some(info) if info.token_type == token_type => {
                    cache.write().insert(hash, info.parent.clone())
                }
                _ => {
                    return match token_type {
                        TokenType::Refresh => {
                            Err(ErrorCode::RefreshTokenNotFound("refresh token not found"))
                        }
                        TokenType::Session => {
                            Err(ErrorCode::SessionTokenNotFound("session token not found"))
                        }
                    };
                }
            };
        };
        Ok(claim)
    }
    pub async fn drop_client_session(self: &Arc<Self>, session_token: &str) -> Result<()> {
        let claim = SessionClaim::decode(session_token)?;
        let now = unix_ts().as_secs();

        let client_session_api =
            UserApiProvider::instance().client_session_api(&Tenant::new_literal(&claim.tenant));

        if now < claim.expire_at_in_secs {
            let session_token_hash = hash_token(session_token.as_bytes());
            // should exist after `verify_token`
            let refresh_token_hash =
                if let Some(Some(hash)) = self.session_tokens.write().pop(&session_token_hash) {
                    self.refresh_tokens.write().pop(&hash);
                    Some(hash)
                } else {
                    None
                };
            if let Some(refresh_token_hash) = refresh_token_hash {
                client_session_api
                    .drop_token(&refresh_token_hash)
                    .await
                    .ok();
            }
            client_session_api
                .drop_token(&session_token_hash)
                .await
                .ok();
            client_session_api
                .drop_client_session_id(&claim.session_id)
                .await
                .ok();
            let state = self.session_state.lock().remove(&claim.session_id);
            if let Some(state) = state {
                drop_all_temp_tables(&claim.session_id, state.temp_tbl_mgr).await?;
            }
        };
        Ok(())
    }

    pub fn on_query_start(&self, client_session_id: &str, session: &Arc<Session>) {
        let mut guard = self.session_state.lock();
        guard.entry(client_session_id.to_string()).and_modify(|e| {
            e.query_state = QueryState::InUse;
            session.set_temp_tbl_mgr(e.temp_tbl_mgr.clone())
        });
    }
    pub fn on_query_finish(&self, client_session_id: &str, session: &Arc<Session>) {
        let temp_tbl_mgr = session.temp_tbl_mgr();
        let (is_empty, just_changed) = temp_tbl_mgr.lock().is_empty();
        if !is_empty || just_changed {
            let mut guard = self.session_state.lock();
            match guard.entry(client_session_id.to_string()) {
                Entry::Vacant(e) => {
                    if !is_empty {
                        e.insert(SessionState {
                            query_state: QueryState::Idle(Instant::now()),
                            temp_tbl_mgr,
                        });
                    }
                }
                Entry::Occupied(mut e) => {
                    if !is_empty {
                        e.get_mut().query_state = QueryState::Idle(Instant::now())
                    } else {
                        e.remove();
                    }
                }
            }
        }
    }
}
