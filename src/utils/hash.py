import re, html, unicodedata
import hmac
import struct
try:
    import blake3
    def h_digest(b: bytes) -> bytes: return blake3.blake3(b).digest(16)  # 128 бит
except ImportError:
    try:
        import xxhash
        def h_digest(b: bytes) -> bytes: return xxhash.xxh3_128(b).digest()
    except ImportError:
        import hashlib
        def h_digest(b: bytes) -> bytes: return hashlib.sha256(b).digest()  # 256 бит

SPACE_RE = re.compile(r"[ \t]+")
ZW_RE = re.compile(r"[\u200B-\u200D\uFEFF]")  # zero-width

def canon_text(s: str | None, strip_html: bool = True) -> str:
    s = "" if s is None else s
    if strip_html:
        # удалим теги и декодируем сущности
        s = re.sub(r"<[^>]+>", "", s)
    # декодируем HTML-сущности
    s = html.unescape(s)
    # нормализация
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = unicodedata.normalize("NFC", s)
    s = ZW_RE.sub("", s)
    s = SPACE_RE.sub(" ", s)
    s = s.strip()
    s = s.casefold()
    return s

def hash_text(text: str, *, return_hex=True) -> str | bytes:
    text = canon_text(text)
    d = h_digest(text.encode("utf-8"))
    return d.hex() if return_hex else d

def hash_seq(ints: list[int]) -> str:
    payload = bytearray()
    payload += struct.pack("<Q", len(ints))          # длина
    for x in ints:
        payload += struct.pack("<q", int(x))         # signed 64-bit LE
    return blake3.blake3(payload).hexdigest(16)  # 128 бит


def hash_attachment_id(ints: list[int]) -> str:
    uniq_sorted = sorted(set(ints))
    return hash_seq(uniq_sorted)

def hashes_equal(h1: str | bytes | None, h2: str | bytes | None) -> bool:
    """Если хэш равны, то возвращаем True, в противном случае False"""
    a = (h1 or b"")
    b = (h2 or b"")
    if isinstance(a, str): a = a.encode()
    if isinstance(b, str): b = b.encode()
    return hmac.compare_digest(a, b)