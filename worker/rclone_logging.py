"""전송 오류에서 인증 값과 URL을 가리고 표시 길이를 제한한다."""

import re
from html.parser import HTMLParser


class _ErrorPageText(HTMLParser):
    """HTML 머리말과 실행 코드를 제외하고 화면의 오류 문구만 모은다."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.ignored = []
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag == 'body':
            self.ignored.clear()
        if tag in ('head', 'script', 'style', 'noscript', 'template'):
            self.ignored.append(tag)

    def handle_endtag(self, tag):
        if tag in self.ignored:
            self.ignored = self.ignored[:self.ignored.index(tag)]

    def handle_data(self, data):
        if not self.ignored and data.strip():
            self.parts.append(data.strip())


def _html_error_text(text):
    marker = re.search(r'(?i)<(?:!doctype\s+html\b|html\b|head\b|body\b)', text)
    if marker is None:
        return text
    parser = _ErrorPageText()
    parser.feed(text[marker.start():])
    parser.close()
    body = ' '.join(parser.parts) or 'HTML 오류 응답 (본문 없음)'
    # 파일명·실행 문맥이 길더라도 실제 오류 본문을 표시할 공간을 남긴다.
    prefix = text[:marker.start()].strip()
    if len(prefix) > 160:
        prefix = prefix[:157] + '...'
    return (prefix + ' ' + body).strip()


def redact_log(value, secrets=(), limit=400):
    text = _html_error_text(str(value))
    for secret in sorted((str(s) for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, '[인증 정보 숨김]')
    text = re.sub(r'(?i)https?://[^\s<>"\']+', '[URL 숨김]', text)
    text = re.sub(r'(?i)\b(?:Bearer|Basic)\s+[^\s,;"\']+', '[인증 정보 숨김]', text)
    text = re.sub(
        r'''(?ix)(["']?(?:access_token|refresh_token|api[_-]?key|token|password|passwd|authorization|client_secret)["']?\s*[:=]\s*)(?:"[^"\n]*"|'[^'\n]*'|[^\s,;]+)''',
        r'\1[인증 정보 숨김]', text)
    text = ''.join(c if ord(c) >= 32 and ord(c) != 127 else ' ' for c in text)
    return text[:limit]
