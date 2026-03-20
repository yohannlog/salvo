use std::sync::Arc;

use salvo_core::http::StatusCode;
use salvo_core::{Depot, Request, Response, Router, handler};

use crate::error::TusError;
use crate::handlers::{apply_common_headers, check_tus_version_or_respond};
use crate::{CancellationContext, Tus};

#[handler]
async fn get(req: &mut Request, depot: &mut Depot, res: &mut Response) {
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

    let storage = {
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

        let info = match store.get_upload_file_info(&id).await {
            Ok(info) => info,
            Err(e) => {
                res.status_code = Some(e.status());
                return;
            }
        };

        // Prevent serving partially uploaded files
        match (info.offset, info.size) {
            (Some(offset), Some(size)) if offset < size => {
                res.status_code = Some(StatusCode::FORBIDDEN);
                return;
            }
            (_, None) => {
                // Deferred length upload not yet complete
                res.status_code = Some(StatusCode::FORBIDDEN);
                return;
            }
            _ => {}
        }

        let storage = match info.storage {
            Some(storage) => storage,
            None => {
                res.status_code =
                    Some(TusError::Internal("upload storage info missing".into()).status());
                return;
            }
        };

        if storage.type_name != "file" {
            res.status_code = Some(
                TusError::Internal(format!("unsupported storage type: {}", storage.type_name))
                    .status(),
            );
            return;
        }

        storage
    };

    res.send_file(storage.path, req.headers()).await;
}

pub fn get_handler() -> Router {
    Router::with_path("{id}").get(get)
}
