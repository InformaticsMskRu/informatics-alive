from flask import current_app
from flask.views import MethodView

from rmatics.utils.response import jsonify


class JudgesApi(MethodView):
    """Expose the configured judges to trusted internal services.

    Only public routing metadata (name, url, lang_map, langs) is returned —
    the secret token and sender_user_id must never leave the service.
    """
    def get(self):
        judges = current_app.extensions.get('judges', {})
        data = {
            str(judge_id): {
                'name': cfg.name,
                'url': cfg.url,
                'lang_map': cfg.lang_map,
                'langs': None if cfg.langs is None else {
                    lang_id: {'name': lang.name, 'ejudge_lang_id': lang.ejudge_lang_id}
                    for lang_id, lang in cfg.langs.items()
                },
            }
            for judge_id, cfg in judges.items()
        }
        return jsonify(data)
