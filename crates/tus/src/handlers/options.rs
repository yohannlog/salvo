use std::sync::Arc;

use salvo_core::http::{HeaderValue, StatusCode};
use salvo_core::{Depot, Request, Response, Router, handler};

use crate::handlers::apply_options_headers;
use crate::stores::Extension;
use crate::{
    H_ACCESS_CONTROL_ALLOW_HEADERS, H_ACCESS_CONTROL_ALLOW_METHODS, H_ACCESS_CONTROL_MAX_AGE,
    H_ACCESS_CONTROL_REQUEST_HEADERS, H_TUS_EXTENSION, H_TUS_MAX_SIZE, H_TUS_VERSION, TUS_VERSION,
    Tus,
};

#[handler]
/// https://tus.io/protocols/resumable-upload#options
async fn options(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let state = depot.obtain::<Arc<Tus>>().expect("missing tus state");
    let store = &state.store;
    let opts = &state.options;
    let max_size = opts.get_configured_max_size(req, None).await;
    apply_options_headers(&mut res.headers, req.headers(), opts);

    res.headers
        .insert(H_TUS_VERSION, HeaderValue::from_static(TUS_VERSION));
    if let Some(ext_header) = Extension::to_header_value(&store.extensions()) {
        res.headers.insert(H_TUS_EXTENSION, ext_header);
    }

    if max_size > 0 {
        res.headers.insert(
            H_TUS_MAX_SIZE,
            HeaderValue::from_str(max_size.to_string().as_str()).expect("invalid header value"),
        );
    }

    res.headers.insert(
        H_ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("OPTIONS, POST, HEAD, PATCH, DELETE, GET"),
    );

    if let Some(h) = req
        .headers()
        .get(H_ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|v| v.to_str().ok())
    {
        if let Ok(v) = HeaderValue::from_str(h) {
            res.headers.insert(H_ACCESS_CONTROL_ALLOW_HEADERS, v);
        }
    } else {
        // Build allow headers list, including any configured additional headers
        let mut allow_headers = "Tus-Resumable, Upload-Length, Upload-Offset, Upload-Metadata, Content-Type, Content-Length".to_string();
        if !opts.allowed_headers.is_empty() {
            allow_headers.push_str(", ");
            allow_headers.push_str(&opts.allowed_headers.join(", "));
        }
        if let Ok(v) = HeaderValue::from_str(&allow_headers) {
            res.headers.insert(H_ACCESS_CONTROL_ALLOW_HEADERS, v);
        }
    }

    res.headers
        .insert(H_ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
    res.status_code = Some(StatusCode::NO_CONTENT);
}

pub fn options_handler() -> Router {
    let options_router = Router::new()
        .options(options)
        .push(Router::with_path("{id}").options(options));
    options_router
}
