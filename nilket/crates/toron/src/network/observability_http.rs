use std::time::Duration;

pub const READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub use observability_http::{
	HttpResponse, MAX_REQUEST_HEADER_BYTES, MAX_RESPONSE_BODY_BYTES, MAX_RESPONSE_HEADER_BYTES,
	RequestLine, parse_request_line, read_request, read_response, write_request, write_response,
};
