use std::sync::Arc;

use salvo_core::http::StatusCode;
use salvo_core::{Depot, Request, Response, Router, handler};

use crate::error::{ProtocolError, TusError};
use crate::handlers::{apply_common_headers, check_tus_version_or_respond};
use crate::stores::Extension;
use crate::{CancellationContext, Tus};

#[handler]
async fn delete(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<Arc<Tus>>().expect("missing tus state");
    let opts = &state.options;
    let store = &state.store;
    apply_common_headers(&mut res.headers, req.headers(), opts);

    if !check_tus_version_or_respond(req, res) {
        return;
    }

    if !store.has_extension(Extension::Termination) {
        res.status_code =
            Some(TusError::Protocol(ProtocolError::UnsupportedTerminationExtension).status());
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

    {
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

        if opts.disable_termination_for_finished_uploads {
            if let Ok(info) = store.get_upload_file_info(&id).await {
                if let (Some(size), Some(offset)) = (info.size, info.offset) {
                    if size == offset {
                        res.status_code = Some(StatusCode::FORBIDDEN);
                        return;
                    }
                }
            }
        }

        match store.remove(&id).await {
            Ok(_) => res.status_code = Some(StatusCode::NO_CONTENT),
            Err(e) => {
                res.status_code = Some(e.status());
                return;
            }
        }
    }

    // Clean up lock entry after successful deletion to prevent memory leaks
    opts.locker.remove_lock(&id).await;
}

pub fn delete_handler() -> Router {
    Router::with_path("{id}").delete(delete)
}
