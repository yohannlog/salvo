use std::sync::Arc;

use futures_util::StreamExt;
use salvo_core::http::{HeaderValue, StatusCode};
use salvo_core::{Depot, Request, Response, Router, handler};

use crate::error::{ProtocolError, TusError};
use crate::handlers::{
    apply_common_headers, apply_upload_finish_hook, calculate_expiration,
    check_tus_version_or_respond, parse_content_length, set_expiration_header,
};
use crate::stores::Extension;
use crate::utils::parse_u64;
use crate::{
    CT_OFFSET_OCTET_STREAM, CancellationContext, H_CONTENT_TYPE, H_UPLOAD_LENGTH, H_UPLOAD_OFFSET,
    Tus,
};

#[handler]
async fn patch(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<Arc<Tus>>().expect("missing tus state");
    let opts = &state.options;
    let store = &state.store;
    apply_common_headers(&mut res.headers, req.headers(), opts);

    let id = match opts.get_file_id_from_request(req) {
        Ok(id) => id,
        Err(e) => {
            res.status_code = Some(e.status());
            return;
        }
    };

    if !check_tus_version_or_respond(req, res) {
        return;
    }

    // Check Content Type. The request MUST include a Content-Type header
    let content_type = req
        .headers()
        .get(H_CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    if content_type != Some(CT_OFFSET_OCTET_STREAM) {
        res.status_code = Some(TusError::Protocol(ProtocolError::InvalidContentType).status());
        return;
    }

    // Check Upload-Offset. The request MUST include a Upload-Offset header
    let offset = match parse_u64(
        req.headers()
            .get(H_UPLOAD_OFFSET)
            .and_then(|v| v.to_str().ok()),
        H_UPLOAD_OFFSET,
    ) {
        Ok(offset) => offset,
        Err(e) => {
            res.status_code = Some(TusError::Protocol(e).status());
            return;
        }
    };

    if let Some(on_incoming_request) = &opts.on_incoming_request {
        on_incoming_request(req, id.clone()).await;
    }

    let max_file_size = opts
        .get_configured_max_size(req, Some(id.to_string()))
        .await;
    let _lock = match opts
        .acquire_write_lock(req, &id, CancellationContext::new())
        .await
    {
        Ok(lock) => lock,
        Err(e) => {
            res.status_code = Some(e.status());
            return;
        }
    };

    let mut already_uploaded_info = match store.get_upload_file_info(&id).await {
        Ok(info) => info,
        Err(e) => {
            res.status_code = Some(e.status());
            return;
        }
    };

    // Check expiration
    let expires_at = calculate_expiration(store.as_ref(), &already_uploaded_info.creation_date);
    if let Some(expires_at) = expires_at {
        if chrono::Utc::now() > expires_at {
            res.status_code = Some(TusError::FileNoLongerExists.status());
            return;
        }
    }

    let Some(uploaded_info_offset) = already_uploaded_info.offset else {
        res.status_code = Some(TusError::InvalidOffset.status());
        return;
    };

    if uploaded_info_offset != offset {
        tracing::info!(
            "Incorrect offset - {:?} sent but file is {:?}",
            offset,
            uploaded_info_offset
        );
        res.status_code = Some(TusError::InvalidOffset.status());
        return;
    }

    if let Some(raw_length) = req.headers().get(H_UPLOAD_LENGTH) {
        let size = match raw_length.to_str() {
            Ok(value) => match parse_u64(Some(value), H_UPLOAD_LENGTH) {
                Ok(size) => size,
                Err(e) => {
                    res.status_code = Some(TusError::Protocol(e).status());
                    return;
                }
            },
            Err(_) => {
                res.status_code =
                    Some(TusError::Protocol(ProtocolError::InvalidInt(H_UPLOAD_LENGTH)).status());
                return;
            }
        };

        if !store.has_extension(Extension::CreationDeferLength) {
            res.status_code = Some(
                TusError::Protocol(ProtocolError::UnsupportedCreationDeferLengthExtension).status(),
            );
            return;
        }
        // Return if upload-length is already set.
        if already_uploaded_info.size.is_some() {
            res.status_code = Some(TusError::Protocol(ProtocolError::InvalidLength).status());
            return;
        }

        if size < uploaded_info_offset {
            res.status_code = Some(TusError::Protocol(ProtocolError::InvalidLength).status());
            return;
        }

        if max_file_size > 0 && size > max_file_size {
            res.status_code = Some(TusError::Protocol(ProtocolError::ErrMaxSizeExceeded).status());
            return;
        }

        // Update
        let _ = store.declare_upload_length(&id, size).await;
        already_uploaded_info.size = Some(size);
    }

    let content_length = match parse_content_length(req) {
        Ok(cl) => cl,
        Err(e) => {
            res.status_code = Some(e.status());
            return;
        }
    };

    let max_allowed = match (already_uploaded_info.size, max_file_size) {
        (Some(size), max) if max > 0 => Some(size.min(max)),
        (Some(size), _) => Some(size),
        (None, max) if max > 0 => Some(max),
        _ => None,
    };

    if let (Some(incoming), Some(max_allowed)) = (content_length, max_allowed) {
        if offset + incoming > max_allowed {
            res.status_code = Some(TusError::Protocol(ProtocolError::ErrMaxSizeExceeded).status());
            return;
        }
    }

    let body = req.take_body();
    let stream = body.map(|frame| frame.map(|frame| frame.into_data().unwrap_or_default()));
    let written = match store.write(&id, offset, Box::pin(stream)).await {
        Ok(written) => written,
        Err(e) => {
            res.status_code = Some(e.status());
            return;
        }
    };

    let new_offset = offset + written;

    // Set expiration header if applicable
    if let Some(expires_at) = expires_at {
        set_expiration_header(
            &mut res.headers,
            expires_at,
            Some(new_offset),
            already_uploaded_info.size,
        );
    }

    // The Server MUST acknowledge successful PATCH requests with the 204 No Content status.
    // It MUST include the Upload-Offset header containing the new offset.
    res.status_code = Some(StatusCode::NO_CONTENT);
    res.headers.insert(
        H_UPLOAD_OFFSET,
        HeaderValue::from_str(&new_offset.to_string()).unwrap(),
    );

    // Call on_upload_finish hook when upload is complete
    let is_complete = already_uploaded_info
        .size
        .is_some_and(|size| new_offset == size);
    if is_complete {
        already_uploaded_info.offset = Some(new_offset);
        apply_upload_finish_hook(req, opts, already_uploaded_info, res).await;
    }
}

pub fn patch_handler() -> Router {
    let patch_router = Router::with_path("{id}").patch(patch);
    patch_router
}
