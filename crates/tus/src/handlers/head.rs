use std::sync::Arc;

use salvo_core::http::{HeaderValue, StatusCode};
use salvo_core::{Depot, Request, Response, Router, handler};

use crate::error::TusError;
use crate::handlers::{
    Metadata, apply_common_headers, calculate_expiration, check_tus_version_or_respond,
    set_expiration_header,
};
use crate::{
    CancellationContext, H_UPLOAD_DEFER_LENGTH, H_UPLOAD_LENGTH, H_UPLOAD_METADATA, H_UPLOAD_OFFSET,
    Tus,
};

#[handler]
async fn head(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<Arc<Tus>>().expect("missing tus state");
    let opts = &state.options;
    let store = &state.store;
    apply_common_headers(&mut res.headers, req.headers(), opts);

    if !check_tus_version_or_respond(req, res) {
        return;
    }

    let id = match opts.get_file_id_from_request(req) {
        Ok(id) => id,
        Err(e) => {
            res.status_code = Some(e.status());
            return;
        }
    };

    if let Some(on_incoming_request) = &opts.on_incoming_request {
        on_incoming_request(req, id.clone()).await;
    }
    let upload_info = {
        let _lock = match opts
            .acquire_read_lock(req, &id, CancellationContext::new())
            .await
        {
            Ok(lock) => lock,
            Err(e) => {
                res.status_code = Some(e.status());
                return;
            }
        };

        match store.get_upload_file_info(&id).await {
            Ok(info) => info,
            Err(e) => {
                res.status_code = Some(e.status());
                return;
            }
        }
    };

    // Check expiration
    let expires_at = calculate_expiration(store.as_ref(), &upload_info.creation_date);
    if let Some(expires_at) = expires_at {
        if chrono::Utc::now() > expires_at {
            res.status_code = Some(TusError::FileNoLongerExists.status());
            return;
        }
    }

    res.status_code = Some(StatusCode::OK);

    let Some(offset) = &upload_info.offset else {
        res.status_code =
            Some(TusError::Internal("Upload file's offset value not found!".into()).status());
        return;
    };
    res.headers.insert(
        H_UPLOAD_OFFSET,
        HeaderValue::from_str(&offset.to_string()).unwrap(),
    );

    if upload_info.get_size_is_deferred() {
        res.headers
            .insert(H_UPLOAD_DEFER_LENGTH, HeaderValue::from_static("1"));
    } else if let Some(size) = &upload_info.size {
        res.headers.insert(
            H_UPLOAD_LENGTH,
            HeaderValue::from_str(&size.to_string()).unwrap(),
        );
    }

    if let Some(metadata) = upload_info.metadata {
        res.headers.insert(
            H_UPLOAD_METADATA,
            HeaderValue::from_str(&Metadata::stringify(metadata)).unwrap(),
        );
    }

    // Set expiration header if applicable
    if let Some(expires_at) = expires_at {
        set_expiration_header(
            &mut res.headers,
            expires_at,
            upload_info.offset,
            upload_info.size,
        );
    }
}

pub fn head_handler() -> Router {
    let head_router = Router::with_path("{id}").head(head);
    head_router
}
