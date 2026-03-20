mod base;
mod delete;
mod get;
mod head;
mod options;
mod patch;
mod post;

use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};

use base64::Engine;
pub use delete::delete_handler;
pub use get::get_handler;
pub use head::head_handler;
pub use options::options_handler;
pub use patch::patch_handler;
pub use post::post_handler;
use salvo_core::http::{HeaderMap, HeaderValue};
use salvo_core::{Request, Response};

use crate::error::{ProtocolError, TusError};
use crate::options::TusOptions;
use crate::stores::{DataStore, Extension};
use crate::utils::{check_tus_version, parse_u64};
use crate::{
    H_CONTENT_LENGTH, H_TUS_RESUMABLE, H_TUS_VERSION, H_UPLOAD_EXPIRES, TUS_VERSION,
};

pub(crate) const EXPOSE_HEADERS: &str = "Location, Upload-Offset, Upload-Length, Upload-Metadata, Upload-Expires, Tus-Resumable, Tus-Version, Tus-Extension, Tus-Max-Size";

fn apply_cors_headers(headers: &mut HeaderMap, req_headers: &HeaderMap, opts: &TusOptions) {
    // Access-Control-Allow-Origin
    if opts.allowed_origins.is_empty() {
        if opts.allowed_credentials {
            // Cannot use `*` with credentials; echo back request Origin if present
            if let Some(origin) = req_headers.get("origin").and_then(|v| v.to_str().ok()) {
                if let Ok(v) = HeaderValue::from_str(origin) {
                    headers.insert("access-control-allow-origin", v);
                }
            }
        } else {
            headers.insert(
                "access-control-allow-origin",
                HeaderValue::from_static("*"),
            );
        }
    } else if let Some(origin) = req_headers.get("origin").and_then(|v| v.to_str().ok()) {
        if opts.allowed_origins.iter().any(|o| o == origin) {
            if let Ok(v) = HeaderValue::from_str(origin) {
                headers.insert("access-control-allow-origin", v);
            }
        }
    }

    // Access-Control-Allow-Credentials
    if opts.allowed_credentials {
        headers.insert(
            "access-control-allow-credentials",
            HeaderValue::from_static("true"),
        );
    }

    // Access-Control-Expose-Headers
    if opts.exposed_headers.is_empty() {
        headers.insert(
            "access-control-expose-headers",
            HeaderValue::from_static(EXPOSE_HEADERS),
        );
    } else {
        let mut expose = EXPOSE_HEADERS.to_string();
        expose.push_str(", ");
        expose.push_str(&opts.exposed_headers.join(", "));
        if let Ok(v) = HeaderValue::from_str(&expose) {
            headers.insert("access-control-expose-headers", v);
        }
    }
}

pub(crate) fn apply_common_headers(
    headers: &mut HeaderMap,
    req_headers: &HeaderMap,
    opts: &TusOptions,
) {
    headers.insert(H_TUS_RESUMABLE, HeaderValue::from_static(TUS_VERSION));
    apply_cors_headers(headers, req_headers, opts);
    headers.insert("cache-control", HeaderValue::from_static("no-store"));
}

pub(crate) fn apply_options_headers(
    headers: &mut HeaderMap,
    req_headers: &HeaderMap,
    opts: &TusOptions,
) {
    apply_cors_headers(headers, req_headers, opts);
    headers.insert("cache-control", HeaderValue::from_static("no-store"));
}

/// Check TUS version header. Returns `false` and sets error response if invalid.
pub(crate) fn check_tus_version_or_respond(req: &Request, res: &mut Response) -> bool {
    if let Err(e) = check_tus_version(
        req.headers()
            .get(H_TUS_RESUMABLE)
            .and_then(|v| v.to_str().ok()),
    ) {
        if matches!(e, ProtocolError::UnsupportedTusVersion(_)) {
            res.headers
                .insert(H_TUS_VERSION, HeaderValue::from_static(TUS_VERSION));
        }
        res.status_code = Some(TusError::Protocol(e).status());
        return false;
    }
    true
}

/// Calculate the expiration datetime for an upload.
pub(crate) fn calculate_expiration(
    store: &dyn DataStore,
    creation_date: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if !store.has_extension(Extension::Expiration) {
        return None;
    }
    let expiration = store.get_expiration()?;
    if expiration <= std::time::Duration::from_secs(0) || creation_date.is_empty() {
        return None;
    }
    let created_at = chrono::DateTime::parse_from_rfc3339(creation_date).ok()?;
    let delta = chrono::Duration::from_std(expiration).ok()?;
    Some(created_at.with_timezone(&chrono::Utc) + delta)
}

/// Set the Upload-Expires header on the response if the upload is unfinished.
pub(crate) fn set_expiration_header(
    headers: &mut HeaderMap,
    expires_at: chrono::DateTime<chrono::Utc>,
    offset: Option<u64>,
    size: Option<u64>,
) {
    let is_finished = matches!((offset, size), (Some(o), Some(s)) if o == s);
    if !is_finished {
        let expires_value = expires_at.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        if let Ok(v) = HeaderValue::from_str(&expires_value) {
            headers.insert(H_UPLOAD_EXPIRES, v);
        }
    }
}

/// Parse Content-Length header from request.
pub(crate) fn parse_content_length(req: &Request) -> Result<Option<u64>, TusError> {
    match req.headers().get(H_CONTENT_LENGTH) {
        Some(value) => match value.to_str() {
            Ok(v) => match parse_u64(Some(v), H_CONTENT_LENGTH) {
                Ok(size) => Ok(Some(size)),
                Err(e) => Err(TusError::Protocol(e)),
            },
            Err(_) => Err(TusError::Protocol(ProtocolError::InvalidInt(
                H_CONTENT_LENGTH,
            ))),
        },
        None => Ok(None),
    }
}

/// Apply the on_upload_finish hook and merge its result into the response.
pub(crate) async fn apply_upload_finish_hook(
    req: &Request,
    opts: &TusOptions,
    upload: crate::stores::UploadInfo,
    res: &mut Response,
) {
    if let Some(on_upload_finish) = &opts.on_upload_finish {
        match on_upload_finish(req, upload).await {
            Ok(patch) => {
                if let Some(status) = patch.status_code {
                    res.status_code = Some(status);
                }
                if let Some(body) = patch.body {
                    if res.write_body(body).is_err() {
                        res.status_code = Some(
                            TusError::Internal("failed to write response body".into()).status(),
                        );
                        return;
                    }
                }
                if let Some(headers) = patch.headers {
                    for (key, value) in headers {
                        if let Some(key) = key {
                            if !res.headers.contains_key(&key) {
                                res.headers.insert(key, value);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                res.status_code = Some(e.status());
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Metadata(pub HashMap<String, Option<String>>);

impl Metadata {
    pub fn parse_metadata(raw: &str) -> Result<Metadata, ProtocolError> {
        if raw.trim().is_empty() {
            return Err(ProtocolError::InvalidMetadata);
        }

        let mut map = HashMap::new();
        let mut seen = HashSet::new();

        for item in raw.split(',') {
            let tokens: Vec<&str> = item.split(' ').collect();
            if tokens.is_empty() || tokens.len() > 2 {
                return Err(ProtocolError::InvalidMetadata);
            }

            let key = tokens[0];
            if !validate_key(key) || !seen.insert(key.to_string()) {
                return Err(ProtocolError::InvalidMetadata);
            }

            if tokens.len() == 1 {
                map.insert(key.to_string(), None);
                continue;
            }

            let value = tokens[1];
            if !validate_value(value) {
                return Err(ProtocolError::InvalidMetadata);
            }

            let decoded = base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|_| ProtocolError::InvalidMetadata)?;
            let decoded_value = String::from_utf8_lossy(&decoded).to_string();

            map.insert(key.to_string(), Some(decoded_value));
        }

        Ok(Metadata(map))
    }

    pub fn stringify(metadata: Metadata) -> String {
        metadata
            .0
            .iter()
            .map(|(key, value)| match value {
                Some(value) => {
                    let encoded =
                        base64::engine::general_purpose::STANDARD.encode(value.as_bytes());
                    format!("{} {}", key, encoded)
                }
                None => key.to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn validate_key(key: &str) -> bool {
    !key.is_empty() && !key.contains(' ') && !key.contains(',')
}

fn validate_value(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .is_ok()
}

impl Deref for Metadata {
    type Target = HashMap<String, Option<String>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Metadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GenerateUrlCtx<'a> {
    pub proto: &'a str,
    pub host: &'a str,
    pub path: &'a str,
    pub id: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub struct HostProto<'a> {
    pub proto: &'a str,
    pub host: &'a str,
}
