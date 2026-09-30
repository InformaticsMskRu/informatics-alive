"""The Polygon statement of a problem, through the ejudge API.

The ejudge Polygon import with enable_iframe_statement copies the HTML
statement of the package (problem.html with its images) into the
problem's attachments/, which the privileged get-file action serves.
Problems imported otherwise have no statement to take.

The statement is processed like the filesystem reload processes it: the
header block goes, $$$ math becomes \\( \\), the samples block becomes
sample_tests_html. Images are returned for pynformatics to store in
/moodle_probpics/<problem_id>/, where the content links them.
"""
import posixpath
from typing import Dict, List, NamedTuple, Optional

from bs4 import BeautifulSoup

from rmatics.ejudge import ejudge_api
from rmatics.ejudge.import_log import ImportLog
from rmatics.ejudge.judges_config import JudgeConfig

# the Russian HTML statement of a Polygon package
STATEMENT_FILE = 'problem.html'
PROBPICS_URL = '/moodle_probpics'

# stands for PROBPICS_URL/<problem_id> until the problem id is known
_PICS_MARKER = '__probpics__'


class Statement(NamedTuple):
    content: str
    sample_tests_html: str
    images: Dict[str, bytes]
    missing_images: List[str]

    def content_for(self, problem_id: int) -> str:
        return self.content.replace(_PICS_MARKER, f'{PROBPICS_URL}/{problem_id}')


def _image_name(src: Optional[str]) -> Optional[str]:
    """The attachments/ file an image links, None for external images.

    ejudge copies the statement files into attachments/ flat.
    """
    if not src or src.startswith(('/', 'data:')) or '://' in src:
        return None
    name = posixpath.basename(src.split('?', 1)[0].split('#', 1)[0])
    if not name or name in ('.', '..'):
        return None
    return name


def _convert_math(html: str) -> str:
    """Polygon's $$$...$$$ to MathJax \\( ... \\)."""
    parts = html.split('$$$')
    closing_opening = ['\\)', '\\(']
    content = parts[0]
    for i in range(1, len(parts)):
        content += closing_opening[i % 2] + parts[i]
    return content


def _describe_reply(content_type: str, body: bytes) -> str:
    """What get-file replied instead of the expected file."""
    error = ejudge_api.describe_error(body) if content_type == 'application/json' else None
    if error:
        return f'replied error {error}'
    return f'replied {content_type or "without a content type"} ({len(body)} bytes)'


def fetch_statement(judge: JudgeConfig, contest_id: int, prob_id: int,
                    log: ImportLog) -> Optional[Statement]:
    content_type, body = ejudge_api.get_file(judge, contest_id, prob_id, STATEMENT_FILE)
    node = None
    if content_type == 'text/html':
        soup = BeautifulSoup(body.decode('utf-8', 'replace'), 'html.parser')
        node = soup.find('div', class_='problem-statement')
    if node is None:
        log.info(f'problem {prob_id}: no statement, get-file {STATEMENT_FILE} '
                 f'{_describe_reply(content_type, body)}')
        return None
    log.info(f'problem {prob_id}: {STATEMENT_FILE} fetched ({len(body)} bytes)')

    for header in node.find_all('div', class_='header'):
        header.decompose()

    sample_tests_html = ''
    samples = node.find('div', class_='sample-tests')
    if samples is not None:
        samples.extract()
        sample_tests_html = f'<div class="problem-statement">{samples}</div>'

    images, missing = {}, []
    for img in node.find_all('img'):
        name = _image_name(img.get('src'))
        if name is None:
            continue
        if name not in images and name not in missing:
            image_type, data = ejudge_api.get_file(judge, contest_id, prob_id, name)
            if image_type.startswith('image/'):
                images[name] = data
                log.info(f'problem {prob_id}: image {name} fetched ({image_type}, {len(data)} bytes)')
            else:
                missing.append(name)
                log.warning(f'problem {prob_id}: image {name} is not in attachments '
                            f'(get-file {_describe_reply(image_type, data)})')
        img['src'] = f'{_PICS_MARKER}/{name}'

    log.info(f'problem {prob_id}: statement processed, {len(images)} image(s), '
             f'{len(missing)} missing, samples: {"yes" if sample_tests_html else "no"}')
    return Statement(
        content=_convert_math(str(node)),
        sample_tests_html=sample_tests_html,
        images=images,
        missing_images=missing,
    )
