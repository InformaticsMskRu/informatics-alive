"""API errors shared across rmatics.

An error with an error_code gets it in the response body next to the
message (see rmatics.view.handle_api_exception), so clients can tell
errors apart without parsing the message.
"""
from werkzeug.exceptions import BadRequest


class LanguageNotSupported(BadRequest):
    """No judge the problem routes to accepts the language."""
    error_code = 'language_not_supported'


class LanguageNotAllowed(BadRequest):
    """The statement (contest) doesn't allow the language."""
    error_code = 'language_not_allowed'
