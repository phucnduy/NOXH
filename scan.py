#!/usr/bin/env python3
"""
scan.py - NOXH Monitor
Quét toàn quốc (34 tỉnh thành theo địa giới hành chính mới từ 12/6/2025),
gửi email tóm tắt DA mới, lưu báo cáo HTML, đồng bộ sang web-app data.json.

Biến môi trường:
  DEEPSEEK_API_KEY  - bắt buộc (DeepSeek V3)
  GEMINI_API_KEY    - tùy chọn (fallback khi DeepSeek thất bại)
  SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS / EMAIL_TO
  PAGES_URL         - URL trang báo cáo HTML
  WEBAPP_DATA_PATH  - đường dẫn tới web-app/public/data.json (tùy chọn)
  FULL_SCAN         - "true" để quét tất cả 6 vùng trong 1 lần chạy (mặc định true)
  FORCE_EMAIL / DRY_RUN
"""
import os, sys, json, re, smtplib, logging, html, time, shutil
import html as _html_lib
from datetime import datetime
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

BASE    = Path(__file__).parent
DATA    = BASE / "data"
DOCS    = BASE / "docs"
DATA.mkdir(exist_ok=True)
DOCS.mkdir(exist_ok=True)
DB_FILE = DATA / "projects.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("noxh")

def cfg(k, default=""):
    return os.environ.get(k, default).strip()

def h(text):
    """Escape HTML entities."""
    return html.escape(str(text)) if text and text != "-" else (text or "-")

def safe_url(url):
    u = (url or "").strip()
    return u if u.startswith(("http://", "https://")) else ""

# -- 34 don vi hanh chinh cap tinh moi (NQ 202/2025/QH15, 12/6/2025) ----------
# Phan vung dia ly -> moi vung la 1 lan goi DeepSeek
# Moi batch gom: ten vung, danh sach tinh, danh sach URL nguon

SCAN_BATCHES = [
    (
        "Dong bang Bac Bo",
        ["Ha Noi", "Hai Phong", "Bac Ninh", "Hung Yen", "Quang Ninh"],
        [
            # So Xay dung chinh thuc
            "soxaydung.hanoi.gov.vn",
            "sxd.haiphong.gov.vn",
            "sxd.bacninh.gov.vn",
            "sxd.hungyen.gov.vn",
            "sxd.quangninh.gov.vn",
            # Bao dia phuong
            "kinhtedothi.vn",
            "hanoimoi.com.vn",
            # Tong hop quoc gia
            "baoxaydung.vn/nha-o-xa-hoi",
            "annhome.vn/nha-o-xa-hoi-ha-noi/",
            "cafef.vn/bat-dong-san/nha-o-xa-hoi-p1.chn",
        ],
    ),
    (
        "Trung du va Mien nui phia Bac",
        ["Phu Tho", "Thai Nguyen", "Lao Cai", "Son La", "Tuyen Quang",
         "Lang Son", "Cao Bang", "Lai Chau", "Dien Bien"],
        [
            "sxd.phutho.gov.vn",
            "sxd.thainguyen.gov.vn",
            "sxd.laocai.gov.vn",
            "sxd.sonla.gov.vn",
            "sxd.tuyenquang.gov.vn",
            "sxd.langson.gov.vn",
            "sxd.caobang.gov.vn",
            "sxd.laichau.gov.vn",
            "sxd.dienbien.gov.vn",
            "baoxaydung.vn/nha-o-xa-hoi",
        ],
    ),
    (
        "Bac Trung Bo",
        ["Ninh Binh", "Thanh Hoa", "Nghe An", "Ha Tinh",
         "Quang Tri", "Thanh pho Hue"],
        [
            "sxd.ninhbinh.gov.vn",
            "sxd.thanhhoa.gov.vn",
            "sxd.nghean.gov.vn",
            "sxd.hatinh.gov.vn",
            "sxd.quangtri.gov.vn",
            "sxd.thuathienhue.gov.vn",
            "baoxaydung.vn/nha-o-xa-hoi",
            "annhome.vn/nha-o-xa-hoi-mien-trung/",
        ],
    ),
    (
        "Nam Trung Bo va Tay Nguyen",
        ["Da Nang", "Quang Ngai", "Gia Lai", "Dak Lak",
         "Khanh Hoa", "Lam Dong"],
        [
            "soxaydung.danang.gov.vn",
            "sxd.quangngai.gov.vn",
            "sxd.gialai.gov.vn",
            "sxd.daklak.gov.vn",
            "sxd.khanhhoa.gov.vn",
            "sxd.lamdong.gov.vn",
            "baoxaydung.vn/nha-o-xa-hoi",
            "annhome.vn/nha-o-xa-hoi-mien-trung/",
        ],
    ),
    (
        "Dong Nam Bo",
        ["TP. Ho Chi Minh", "Dong Nai", "Tay Ninh"],
        [
            "soxaydung.hochiminhcity.gov.vn",
            "soxaydung.hochiminhcity.gov.vn/thong-tin-nha-o-xa-hoi",
            "sxd.dongnai.gov.vn/vi/news/thong-tin-mo-ban-nha-o-xa-hoi/",
            "sxd.tayninh.gov.vn",
            "annhome.vn/danh-sach-nha-o-xa-hoi-tp-ho-chi-minh/",
            "annhome.vn/nha-o-xa-hoi-binh-duong/",
            "thuvienphapluat.vn/phap-luat/ho-tro-phap-luat/danh-sach-cac-du-an-nha-o-xa-hoi-o-tphcm-cap-nhat-2025-moi-nhat-gia-thue-mua-nha-o-xa-hoi-tu-101020-236847.html",
            "cafef.vn/bat-dong-san/nha-o-xa-hoi-p1.chn",
            "baoxaydung.vn/nha-o-xa-hoi",
        ],
    ),
    (
        "Dong bang song Cuu Long",
        ["Dong Thap", "An Giang", "Vinh Long", "Can Tho", "Ca Mau"],
        [
            "sxd.dongthap.gov.vn",
            "sxd.angiang.gov.vn",
            "sxd.vinhlong.gov.vn",
            "soxaydung.cantho.gov.vn",
            "sxd.camau.gov.vn",
            "baoxaydung.vn/nha-o-xa-hoi",
            "annhome.vn/nha-o-xa-hoi-mien-tay/",
        ],
    ),
]

# Nguon quoc gia - them vao moi batch khi scan
NATIONAL_SOURCES = [
    "moc.gov.vn/tintuc/Pages/nha-o-xa-hoi.aspx",
    "vnexpress.net/bat-dong-san",
    "batdongsan.com.vn/nha-o-xa-hoi",
    "reatimes.vn/nha-o-xa-hoi",
]

ALL_PROVINCES = [p for _, provinces, _ in SCAN_BATCHES for p in provinces]

# -- Bang chuan hoa ten tinh cu -> ten moi ------------------------------------------
PROVINCE_NORMALIZE: dict[str, str] = {
    "Ha Noi": "Ha Noi",
    "TP Ho Chi Minh": "TP. Ho Chi Minh",
    "Ho Chi Minh": "TP. Ho Chi Minh",
    "Da Nang": "Da Nang",
    "Hai Phong": "Hai Phong",
    "Can Tho": "Can Tho",
    "Hue": "Thanh pho Hue",
    # TP truc thuoc TW
    "Ha Noi": "Ha Noi",
    "TP. Ho Chi Minh": "TP. Ho Chi Minh",
    "Ho Chi Minh": "TP. Ho Chi Minh",
    "TP.HCM": "TP. Ho Chi Minh",
    "TPHCM": "TP. Ho Chi Minh",
    "Tp.HCM": "TP. Ho Chi Minh",
    "Hai Phong": "Hai Phong",
    "TP. Hai Phong": "Hai Phong",
    "Da Nang": "Da Nang",
    "TP. Da Nang": "Da Nang",
    "Thanh pho Hue": "Thanh pho Hue",
    "Hue": "Thanh pho Hue",
    "TT. Hue": "Thanh pho Hue",
    "Thua Thien Hue": "Thanh pho Hue",
    "Thua Thien - Hue": "Thanh pho Hue",
    "Can Tho": "Can Tho",
    "TP. Can Tho": "Can Tho",
    # Tinh giu nguyen
    "Cao Bang": "Cao Bang",
    "Lai Chau": "Lai Chau",
    "Dien Bien": "Dien Bien",
    "Lang Son": "Lang Son",
    "Son La": "Son La",
    "Quang Ninh": "Quang Ninh",
    "Thanh Hoa": "Thanh Hoa",
    "Nghe An": "Nghe An",
    "Ha Tinh": "Ha Tinh",
    # Tinh da sap nhap
    "Ha Giang": "Tuyen Quang",
    "Tuyen Quang": "Tuyen Quang",
    "Lao Cai": "Lao Cai",
    "Yen Bai": "Lao Cai",
    "Bac Kan": "Thai Nguyen",
    "Thai Nguyen": "Thai Nguyen",
    "Hoa Binh": "Phu Tho",
    "Vinh Phuc": "Phu Tho",
    "Phu Tho": "Phu Tho",
    "Bac Giang": "Bac Ninh",
    "Bac Ninh": "Bac Ninh",
    "Hai Duong": "Hai Phong",
    "Thai Binh": "Hung Yen",
    "Hung Yen": "Hung Yen",
    "Ha Nam": "Ninh Binh",
    "Nam Dinh": "Ninh Binh",
    "Ninh Binh": "Ninh Binh",
    "Quang Binh": "Quang Tri",
    "Quang Tri": "Quang Tri",
    "Quang Nam": "Da Nang",
    "Kon Tum": "Quang Ngai",
    "Quang Ngai": "Quang Ngai",
    "Binh Dinh": "Gia Lai",
    "Gia Lai": "Gia Lai",
    "Phu Yen": "Dak Lak",
    "Dak Lak": "Dak Lak",
    "Khanh Hoa": "Khanh Hoa",
    "Ninh Thuan": "Khanh Hoa",
    "Dak Nong": "Lam Dong",
    "Lam Dong": "Lam Dong",
    "Binh Thuan": "Lam Dong",
    "Binh Phuoc": "Dong Nai",
    "Dong Nai": "Dong Nai",
    "Ba Ria - Vung Tau": "TP. Ho Chi Minh",
    "Ba Ria-Vung Tau": "TP. Ho Chi Minh",
    "BR-VT": "TP. Ho Chi Minh",
    "Binh Duong": "TP. Ho Chi Minh",
    "Long An": "Tay Ninh",
    "Tay Ninh": "Tay Ninh",
    "Tien Giang": "Dong Thap",
    "Dong Thap": "Dong Thap",
    "An Giang": "An Giang",
    "Kien Giang": "An Giang",
    "Ben Tre": "Vinh Long",
    "Vinh Long": "Vinh Long",
    "Tra Vinh": "Vinh Long",
    "Soc Trang": "Can Tho",
    "Hau Giang": "Can Tho",
    "Bac Lieu": "Ca Mau",
    "Ca Mau": "Ca Mau",
}

# Bang tieng Viet co dau -> khong dau (de normalize)
PROVINCE_VN: dict[str, str] = {
    "Ha Noi": "Ha Noi",
    "Hai Phong": "Hai Phong",
    "Bac Ninh": "Bac Ninh",
    "Hung Yen": "Hung Yen",
    "Quang Ninh": "Quang Ninh",
    "Phu Tho": "Phu Tho",
    "Thai Nguyen": "Thai Nguyen",
    "Lao Cai": "Lao Cai",
    "Son La": "Son La",
    "Tuyen Quang": "Tuyen Quang",
    "Lang Son": "Lang Son",
    "Cao Bang": "Cao Bang",
    "Lai Chau": "Lai Chau",
    "Dien Bien": "Dien Bien",
    "Ninh Binh": "Ninh Binh",
    "Thanh Hoa": "Thanh Hoa",
    "Nghe An": "Nghe An",
    "Ha Tinh": "Ha Tinh",
    "Quang Tri": "Quang Tri",
    "Thanh pho Hue": "Thanh pho Hue",
    "Da Nang": "Da Nang",
    "Quang Ngai": "Quang Ngai",
    "Gia Lai": "Gia Lai",
    "Dak Lak": "Dak Lak",
    "Khanh Hoa": "Khanh Hoa",
    "Lam Dong": "Lam Dong",
    "TP. Ho Chi Minh": "TP. Ho Chi Minh",
    "Dong Nai": "Dong Nai",
    "Tay Ninh": "Tay Ninh",
    "Dong Thap": "Dong Thap",
    "An Giang": "An Giang",
    "Vinh Long": "Vinh Long",
    "Can Tho": "Can Tho",
    "Ca Mau": "Ca Mau",
}

FORCE_EMAIL  = cfg("FORCE_EMAIL", "false").lower() == "true"
DRY_RUN      = cfg("DRY_RUN",     "false").lower() == "true"
FULL_SCAN    = cfg("FULL_SCAN",   "true").lower()  == "true"
SMTP_HOST    = cfg("SMTP_HOST",   "localhost")
SMTP_PORT    = int(cfg("SMTP_PORT", "25"))
WEBAPP_DATA  = Path(cfg("WEBAPP_DATA_PATH", str(BASE / "../web-app/public/data.json")))

# HTTP session dung chung (keep-alive, retry)
_session: requests.Session | None = None

def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi,en-US;q=0.7,en;q=0.3",
        })
    return _session

def normalize_province(raw: str) -> str:
    """Chuan hoa ten tinh/thanh -> ten moi theo dia gioi 2025."""
    s = (raw or "").strip()
    if s in PROVINCE_NORMALIZE:
        return PROVINCE_NORMALIZE[s]
    for k, v in PROVINCE_NORMALIZE.items():
        if k.lower() == s.lower():
            return v
    for k, v in PROVINCE_NORMALIZE.items():
        if k.lower() in s.lower() or s.lower() in k.lower():
            return v
    return s

# -- Database -------------------------------------------------------------------
def load_db() -> dict:
    if DB_FILE.exists():
        try:
            return json.loads(DB_FILE.read_text("utf-8"))
        except Exception:
            bak = DB_FILE.with_suffix(".json.bak")
            if bak.exists():
                try:
                    log.warning("projects.json bi loi, thu load tu backup...")
                    return json.loads(bak.read_text("utf-8"))
                except Exception:
                    pass
            log.error("Khong load duoc DB va backup, khoi tao moi")
    return {"projects": [], "scans": [], "updated": "", "last_batch_index": 0}

def save_db(db: dict) -> None:
    db["updated"] = datetime.now().isoformat()
    if DB_FILE.exists():
        shutil.copy2(DB_FILE, DB_FILE.with_suffix(".json.bak"))
    DB_FILE.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

def sync_webapp(projects: list) -> None:
    try:
        path = WEBAPP_DATA.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"projects": projects, "updated": datetime.now().isoformat()}
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info(f"Dong bo web-app OK: {path} ({len(projects)} du an)")
    except Exception as e:
        log.warning(f"Khong sync duoc web-app data: {e}")


# -- DeepSeek API ---------------------------------------------------------------

def deepseek_chat(
    messages: list[dict],
    max_tokens: int = 4096,
    temperature: float = 0.1,
    timeout: int = 120,
) -> str:
    """Goi DeepSeek V3 API (OpenAI-compatible REST, khong can package openai)."""
    api_key = cfg("DEEPSEEK_API_KEY")
    if not api_key:
        raise ValueError("Thieu DEEPSEEK_API_KEY")
    resp = requests.post(
        "https://api.deepseek.com/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": "deepseek-chat",
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False,
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"] or ""


# -- Web fetching ---------------------------------------------------------------

def fetch_url_content(url: str, timeout: int = 12, max_chars: int = 8000) -> str:
    """Tai trang web, trich xuat text thuan (bo script/style/nav)."""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        sess = get_session()
        resp = sess.get(url, timeout=timeout, allow_redirects=True)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.content, "lxml")
        # Bo cac the nhieu
        for tag in soup(["script", "style", "nav", "footer", "header",
                          "aside", "noscript", "iframe", "form"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        # Don dep khoang trang thua
        text = re.sub(r"[ \t]{3,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text[:max_chars]
    except Exception as e:
        log.debug(f"fetch '{url}': {e}")
        return ""


def search_web(query: str, max_chars: int = 5000) -> str:
    """Tim kiem DuckDuckGo HTML (khong can API key)."""
    try:
        params = {"q": query, "kl": "vn-vi", "ia": "web"}
        sess = get_session()
        resp = sess.get(
            "https://html.duckduckgo.com/html/",
            params=params,
            timeout=15,
        )
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.content, "lxml")
        # Lay cac doan ket qua
        results = []
        for result in soup.select(".result__body, .result__snippet"):
            text = result.get_text(separator=" ", strip=True)
            if text:
                results.append(text)
        combined = "\n".join(results)
        return combined[:max_chars]
    except Exception as e:
        log.debug(f"search '{query}': {e}")
        return ""


# -- Geocoding ------------------------------------------------------------------

PROVINCE_CAPITALS: dict[str, tuple[float, float]] = {
    "Ha Noi": (21.0285, 105.8542),
    "Hai Phong": (20.8449, 106.6881),
    "Bac Ninh": (21.1861, 106.0763),
    "Hung Yen": (20.6464, 106.0511),
    "Quang Ninh": (21.0064, 107.2925),
    "Phu Tho": (21.3450, 105.2230),
    "Thai Nguyen": (21.5928, 105.8442),
    "Lao Cai": (22.4809, 103.9754),
    "Son La": (21.3272, 103.9144),
    "Tuyen Quang": (21.8239, 105.2180),
    "Lang Son": (21.8537, 106.7615),
    "Cao Bang": (22.6657, 106.2639),
    "Lai Chau": (22.3964, 103.4580),
    "Dien Bien": (21.3860, 103.0230),
    "Ninh Binh": (20.2541, 105.9765),
    "Thanh Hoa": (19.8067, 105.7852),
    "Nghe An": (18.6796, 105.6813),
    "Ha Tinh": (18.3428, 105.9057),
    "Quang Tri": (16.7403, 107.1857),
    "Thanh pho Hue": (16.4637, 107.5909),
    "Da Nang": (16.0544, 108.2022),
    "Quang Ngai": (15.1200, 108.7922),
    "Gia Lai": (13.9833, 108.0000),
    "Dak Lak": (12.6667, 108.0500),
    "Khanh Hoa": (12.2388, 109.1967),
    "Lam Dong": (11.9465, 108.4419),
    "TP. Ho Chi Minh": (10.8231, 106.6297),
    "Dong Nai": (10.9455, 107.1684),
    "Tay Ninh": (11.3352, 106.1099),
    "Dong Thap": (10.4937, 105.6882),
    "An Giang": (10.5216, 105.1259),
    "Vinh Long": (10.2538, 106.0000),
    "Can Tho": (10.0452, 105.7469),
    "Ca Mau": (9.1769, 105.1500),
}


def geocode_projects(projects: list) -> bool:
    """Geocode cac du an con thieu toa do bang Nominatim OSM.
    Mutates projects in-place. Returns True neu co it nhat 1 du an duoc geocode.
    """
    import urllib.request as _ureq
    import urllib.parse as _uparse

    needs_geocode = [
        p for p in projects
        if not p.get("toado_lat") or p.get("toado_lat") == 0
    ]
    if not needs_geocode:
        log.info("Geocoding: tat ca du an da co toa do")
        return False

    log.info(f"Geocoding: {len(needs_geocode)} du an can toa do")
    geocoded_count = 0
    headers = {"User-Agent": "NOXH-Monitor/1.0 (noxh-monitor)"}

    def nominatim_search(query: str) -> tuple[float, float] | None:
        params = _uparse.urlencode({
            "q": query, "format": "json", "limit": "1", "countrycodes": "vn",
        })
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        req = _ureq.Request(url, headers=headers)
        try:
            with _ureq.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception as e:
            log.debug(f"Nominatim loi '{query}': {e}")
        return None

    for p in needs_geocode:
        tinh = p.get("tinh_tp", "")
        vi_tri = p.get("vi_tri", "")
        quan_huyen = p.get("quan_huyen", "")
        result: tuple[float, float] | None = None

        if vi_tri and tinh:
            result = nominatim_search(f"{vi_tri}, {tinh}, Viet Nam")
            time.sleep(1.2)
        if not result and quan_huyen and tinh:
            result = nominatim_search(f"{quan_huyen}, {tinh}, Viet Nam")
            time.sleep(1.2)
        if not result and tinh in PROVINCE_CAPITALS:
            result = PROVINCE_CAPITALS[tinh]
            log.debug(f"Dung toa do tinh ly cho: {p.get('ten_du_an')}")

        if result:
            p["toado_lat"], p["toado_lng"] = result[0], result[1]
            geocoded_count += 1
            log.info(f"  OK {p.get('ten_du_an', '?')[:40]} -> {result[0]:.4f}, {result[1]:.4f}")
        else:
            log.warning(f"  X Khong tim duoc toa do: {p.get('ten_du_an', '?')[:40]}")

    log.info(f"Geocoding xong: {geocoded_count}/{len(needs_geocode)} thanh cong")
    return geocoded_count > 0


def dedup(existing: list, new_list: list) -> tuple[list, list]:
    def key(p: dict) -> str:
        return (p.get("ten_du_an", "") + p.get("tinh_tp", "")).lower().strip()
    ex_keys = {key(p) for p in existing}
    truly_new = [p for p in new_list if key(p) not in ex_keys]
    return existing + truly_new, truly_new


# -- Prompt & scan --------------------------------------------------------------

_SCHEMA = (
    '[{"ten_du_an":"","ten_thuong_mai":"","tinh_tp":"ten tinh moi 2025",'
    '"quan_huyen":"","vi_tri":"dia chi day du","chu_dau_tu":"",'
    '"so_toa":"so toa/block","tong_can":"tong so can","can_ban":"so can ban",'
    '"dien_tich_can":"m2 tu-den","gia_ban_m2":"X trieu/m2","gia_can_tu":"X ty/can",'
    '"khoi_cong":"DD/MM/YYYY","nhan_ho_so_tu":"DD/MM/YYYY hoac Quy X/YYYY",'
    '"nhan_ho_so_den":"","du_kien_ban_giao":"","doi_tuong_uu_tien":"",'
    '"dieu_kien_mua":"","dia_diem_nop_ho_so":"","website_chu_dau_tu":"",'
    '"quy_mo_dan_so":"","lien_he":"SDT hoac email","anh_phoi_canh":"URL anh",'
    '"trang_thai":"Dang nhan HS|Sap nhan HS|Vua khoi cong|Dang thi cong|Ke hoach",'
    '"nguon":"ten bao + ngay","url_nguon":"URL bai bao","ghi_chu":""}]'
)

_SYSTEM_PROMPT = (
    "Ban la chuyen gia phan tich bat dong san Viet Nam chuyen ve nha o xa hoi (NOXH). "
    "Nhiem vu: doc noi dung duoc cung cap va trich xuat TOAN BO thong tin du an NOXH. "
    "Tra ve JSON array hop le duy nhat. "
    "Khong giai thich, khong markdown code block, khong them text ngoai JSON."
)


def build_prompt(
    provinces: list[str],
    sources: list[str],
    fetched_contents: dict[str, str] | None = None,
) -> str:
    year = datetime.now().year
    prov_str = ", ".join(provinces)

    parts: list[str] = []

    # Phan noi dung da thu thap
    if fetched_contents:
        parts.append("=== NOI DUNG DA THU THAP TU CAC NGUON ===")
        for url, content in fetched_contents.items():
            if content and len(content) > 150:
                parts.append(f"--- {url} ---")
                parts.append(content[:6000])
                parts.append("")

    parts += [
        "=== NHIEM VU ===",
        f"Trich xuat TAT CA du an nha o xa hoi (NOXH) tai: {prov_str}",
        f"Bao gom moi giai doan: dang trien khai, sap mo ban, da nhan HS, dang thi cong, vua khoi cong, ke hoach ({year}-{year+2})",
        f"Nguon tham khao them (neu noi dung tren chua du): {', '.join(sources[:5])}",
        "",
        "=== QUY TAC BAT BUOC ===",
        "1. Trich xuat CANG NHIEU du an CANG TOT - khong bo sot bat ky du an nao",
        "2. Ten tinh/thanh PHAI theo dia gioi MOI 2025:",
        "   Bac Giang -> Bac Ninh | Hai Duong -> Hai Phong | Thai Binh -> Hung Yen",
        "   Ha Nam/Nam Dinh -> Ninh Binh | Quang Binh -> Quang Tri | Quang Nam -> Da Nang",
        "   Binh Dinh -> Gia Lai | Phu Yen -> Dak Lak | Binh Duong/BR-VT -> TP. Ho Chi Minh",
        "   Long An -> Tay Ninh | Tien Giang -> Dong Thap | Kien Giang -> An Giang",
        "   Ben Tre/Tra Vinh -> Vinh Long | Soc Trang/Hau Giang -> Can Tho | Bac Lieu -> Ca Mau",
        "3. Gia ghi dang '15 trieu/m2' hoac '1,2 ty/can'",
        "4. Ngay ghi dang 'DD/MM/YYYY' hoac 'Quy II/2025'",
        "5. Neu khong tim thay thong tin cho 1 truong -> de trong ('')",
        "6. Chi tra ve JSON array, khong giai thich, khong markdown",
        "",
        f"=== SCHEMA ===\n{_SCHEMA}",
        "",
        "Neu khong co du an nao -> tra ve: []",
    ]
    return "\n".join(parts)


def gemini_fallback(prompt_text: str) -> list[dict]:
    """Fallback sang Gemini 2.0 Flash khi DeepSeek that bai."""
    api_key = cfg("GEMINI_API_KEY")
    if not api_key:
        log.warning("  Gemini fallback: thieu GEMINI_API_KEY")
        return []
    try:
        from google import genai as ggenai
        from google.genai import types as gtypes
    except ImportError:
        log.warning("  Gemini fallback: chua cai thu vien (pip install google-genai)")
        return []
    try:
        client = ggenai.Client(api_key=api_key)
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt_text,
            config=gtypes.GenerateContentConfig(
                tools=[gtypes.Tool(google_search=gtypes.GoogleSearch())],
            ),
        )
        full = response.text or ""
        log.info(f"  Gemini fallback response: {len(full)} ky tu")
        for pat in [r'\[\s*\{[\s\S]*?\}\s*\]', r'\[\s*\]']:
            m = re.search(pat, full)
            if m:
                try:
                    data = json.loads(m.group())
                    for p in data:
                        if p.get("tinh_tp"):
                            p["tinh_tp"] = normalize_province(p["tinh_tp"])
                    log.info(f"  Gemini fallback OK: {len(data)} du an")
                    return data
                except Exception:
                    pass
        log.warning("  Gemini fallback: khong parse duoc JSON")
        return []
    except Exception as e:
        log.error(f"  Gemini fallback loi: {e}")
        return []


def _parse_json_response(text: str, batch_name: str = "") -> list | None:
    """Robust JSON array extractor - xu ly markdown fence, non-greedy, bracket-scan."""
    # 1. Strip markdown code fence neu co
    stripped = re.sub(r"```(?:json)?\s*", "", text).replace("```", "").strip()

    # 2. Thu parse truc tiep
    for candidate in [stripped, text]:
        try:
            result = json.loads(candidate)
            if isinstance(result, list):
                return result
        except Exception:
            pass

    # 3. Tim vi tri [ dau tien va ] cuoi cung (greedy - lay toan bo mang)
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end > start:
        try:
            result = json.loads(text[start:end + 1])
            if isinstance(result, list):
                return result
        except Exception:
            pass

    # 4. Neu van loi - thu bracket-counting de lay JSON array hoan chinh
    depth = 0
    in_str = False
    escape = False
    arr_start = None
    for i, ch in enumerate(text):
        if escape:
            escape = False
            continue
        if ch == "\\" and in_str:
            escape = True
            continue
        if ch == '"' and not escape:
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "[" and depth == 0:
            arr_start = i
            depth = 1
        elif ch == "[":
            depth += 1
        elif ch == "]" and depth > 0:
            depth -= 1
            if depth == 0 and arr_start is not None:
                try:
                    result = json.loads(text[arr_start:i + 1])
                    if isinstance(result, list):
                        return result
                except Exception:
                    pass
                arr_start = None

    # 5. Truncated JSON repair - response bi cat giua chung (max_tokens)
    # Tim object hoan chinh cuoi cung truoc diem cat, dong array lai
    start = text.find("[")
    if start != -1:
        # Lay tat ca vi tri ket thuc object hoan chinh "  }" o depth 1
        # Tim "}," hoac "}\n]" la object boundaries
        candidate_text = text[start:]
        # Tim vi tri "}" cuoi cung o level 1 (khong nam trong nested object)
        last_complete = None
        d = 0
        in_s = False
        esc = False
        for i, ch in enumerate(candidate_text):
            if esc:
                esc = False
                continue
            if ch == "\\" and in_s:
                esc = True
                continue
            if ch == '"':
                in_s = not in_s
                continue
            if in_s:
                continue
            if ch in "{[":
                d += 1
            elif ch in "}]":
                d -= 1
                if d == 1 and ch == "}":
                    # object hoan chinh trong array
                    last_complete = start + i
        if last_complete is not None:
            repaired = text[start:last_complete + 1] + "]"
            try:
                result = json.loads(repaired)
                if isinstance(result, list) and result:
                    log.warning(f"  JSON bi truncate - da repair: {len(result)} du an (co the thieu 1 so)")
                    return result
            except Exception:
                pass

    log.warning(f"  Khong parse duoc JSON tu [{batch_name}]")
    return None


def run_batch(batch_name: str, provinces: list[str], sources: list[str]) -> list[dict]:
    """
    Quet 1 vung:
      1. Thu thap noi dung song song tu cac URL nguon + DuckDuckGo search
      2. Truyen noi dung that vao DeepSeek de trich xuat JSON
    """
    log.info(f"[{batch_name}] {', '.join(provinces)}")

    # 1. Xay dung danh sach fetch jobs: URL + search queries
    url_jobs: list[str] = []
    all_sources = sources + NATIONAL_SOURCES
    for s in all_sources:
        url_jobs.append(s if s.startswith("http") else f"https://{s}")

    # Them search queries cho moi batch
    search_jobs: list[str] = []
    prov_short = " ".join(provinces[:2])
    search_jobs += [
        f"du an nha o xa hoi {prov_short} {datetime.now().year} {datetime.now().year + 1}",
        f"mo ban nhan ho so NOXH {prov_short} 2025 2026",
        f"nha o xa hoi {provinces[0]} khoi cong trien khai moi nhat",
    ]

    # 2. Fetch song song
    fetched: dict[str, str] = {}

    def _fetch_url(url: str) -> tuple[str, str]:
        return url, fetch_url_content(url, timeout=14, max_chars=8000)

    def _search(q: str) -> tuple[str, str]:
        return f"SEARCH: {q}", search_web(q, max_chars=5000)

    total_jobs = len(url_jobs) + len(search_jobs)
    log.info(f"  Thu thap {total_jobs} nguon ({len(url_jobs)} URL + {len(search_jobs)} search)...")

    with ThreadPoolExecutor(max_workers=8) as ex:
        url_futures = {ex.submit(_fetch_url, u): u for u in url_jobs}
        search_futures = {ex.submit(_search, q): q for q in search_jobs}
        all_futures = {**url_futures, **search_futures}
        for fut in as_completed(all_futures, timeout=90):
            try:
                key, content = fut.result()
                if content and len(content) > 100:
                    fetched[key] = content
            except Exception:
                pass

    total_chars = sum(len(v) for v in fetched.values())
    log.info(f"  Da thu thap: {len(fetched)}/{total_jobs} nguon co noi dung | {total_chars:,} ky tu")

    # 3. Goi DeepSeek
    prompt_text = build_prompt(provinces, sources, fetched)
    log.info(f"  Prompt: {len(prompt_text):,} ky tu")

    last_err = None
    for attempt in range(3):
        if attempt > 0:
            wait = 30 * attempt
            log.info(f"  Retry {attempt}/2, cho {wait}s...")
            time.sleep(wait)
        try:
            full = deepseek_chat(
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": prompt_text},
                ],
                max_tokens=8192,
                temperature=0.1,
            )
            log.info(f"  DeepSeek response: {len(full)} ky tu | head={full[:80]!r} | tail={full[-80:]!r}")
            # Parse JSON - thu nhieu cach de xu ly markdown fence va non-greedy
            data = _parse_json_response(full, batch_name)
            if data is not None:
                for p in data:
                    if p.get("tinh_tp"):
                        p["tinh_tp"] = normalize_province(p["tinh_tp"])
                log.info(f"  Parse OK: {len(data)} du an")
                return data
        except Exception as e:
            last_err = e
            log.error(f"  Loi attempt {attempt + 1}/3: {e}")

    log.warning(f"  DeepSeek that bai -> thu Gemini fallback...")
    return gemini_fallback(build_prompt(provinces, sources))


def run_scan(db: dict) -> tuple[list[dict], list[str]]:
    """Quet theo vung, tra ve (tat ca du an moi gop, danh sach vung da quet)."""
    api_key = cfg("DEEPSEEK_API_KEY")
    if not api_key:
        raise ValueError("Thieu DEEPSEEK_API_KEY")

    if FULL_SCAN:
        batches_to_scan = list(range(len(SCAN_BATCHES)))
        log.info(f"FULL_SCAN=true -> quet toan bo {len(SCAN_BATCHES)} vung")
    else:
        idx = db.get("last_batch_index", 0) % len(SCAN_BATCHES)
        batches_to_scan = [idx]
        log.info(f"FULL_SCAN=false -> quet vung {idx}: {SCAN_BATCHES[idx][0]}")

    all_raw: list[dict] = []
    scanned_names: list[str] = []

    for i in batches_to_scan:
        name, provinces, sources = SCAN_BATCHES[i]
        try:
            results = run_batch(name, provinces, sources)
            all_raw.extend(results)
            scanned_names.append(name)
        except Exception as e:
            log.error(f"Vung [{name}] that bai: {e}")
        # Nghi giua cac batch tranh rate-limit
        if len(batches_to_scan) > 1 and i != batches_to_scan[-1]:
            log.info("  Nghi 30s giua cac batch...")
            time.sleep(30)

    last = db.get("last_batch_index", 0)
    db["last_batch_index"] = (last + len(batches_to_scan)) % len(SCAN_BATCHES)
    return all_raw, scanned_names


# -- HTML Report ----------------------------------------------------------------
NAV, GOLD, GRN = "#0B2545", "#C9932A", "#1A6B3A"

def badge(text, color):
    pal = {
        "green": ("#D4EDDA", "#1A6B3A"),
        "amber": ("#FEF3C7", "#D97706"),
        "navy":  ("#E8EDF5", "#1B3A6B"),
        "gray":  ("#F1F3F4", "#5F6368"),
        "rose":  ("#FFE4E6", "#BE123C"),
    }
    bg, fg = pal.get(color, pal["gray"])
    return (
        f'<span style="background:{bg};color:{fg};font-size:11px;'
        f'font-weight:600;padding:2px 7px;border-radius:4px;white-space:nowrap">'
        f'{text}</span>'
    )

def status_color(ts):
    t = (ts or "").lower()
    if "nhan" in t or "mo" in t:   return "green"
    if "sap" in t or "du kien" in t: return "amber"
    if "khoi" in t: return "navy"
    return "gray"

def prov_badge_color(p: str) -> str:
    if p in ("Ha Noi", "TP. Ho Chi Minh"): return "navy"
    if p in ("Hai Phong", "Da Nang", "Can Tho", "Thanh pho Hue"): return "rose"
    if p in ("Bac Ninh", "Hung Yen", "Quang Ninh"): return "green"
    return "amber"

def tbl_row(p, i):
    bg  = "#ffffff" if i % 2 == 0 else "#f8f9fa"
    ten = h(p.get("ten_du_an", "-"))
    tm  = h(p.get("ten_thuong_mai", ""))
    tm_html = (
        f'<div style="font-weight:400;color:#9aa0a6;font-size:10px;'
        f'margin-top:2px;white-space:normal">{tm}</div>'
    ) if tm and tm != "-" else ""

    hs = "-"
    if p.get("nhan_ho_so_tu"):
        hs = h(p["nhan_ho_so_tu"])
        if p.get("nhan_ho_so_den"):
            hs += " -><br>" + h(p["nhan_ho_so_den"])
    elif p.get("khoi_cong"):
        hs = "KC: " + h(p["khoi_cong"])

    src = h(p.get("nguon", "-"))
    url = safe_url(p.get("url_nguon", ""))
    src_html = (
        f'<a href="{url}" target="_blank" rel="noopener noreferrer" '
        f'style="color:{GOLD};text-decoration:none">{src[:40]}</a>'
    ) if url else src[:40]

    web = safe_url(p.get("website_chu_dau_tu", ""))
    web_html = (
        f'<a href="{web}" target="_blank" rel="noopener noreferrer" '
        f'style="color:{GOLD};text-decoration:none;font-size:11px">Link</a>'
    ) if web else "-"

    prov = p.get("tinh_tp", "-")
    return (
        f'<tr style="background:{bg};border-bottom:1px solid #e8eaed">'
        f'<td style="padding:7px 8px;color:#9aa0a6;font-size:11px;text-align:center">{i+1}</td>'
        f'<td style="padding:7px 8px">{badge(h(prov), prov_badge_color(prov))}</td>'
        f'<td style="padding:7px 8px;font-weight:600;color:{NAV};font-size:12px">{ten}{tm_html}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368">{h(p.get("vi_tri","-"))}'
        f'<div style="color:#9aa0a6;font-size:10px">{h(p.get("quan_huyen",""))}</div></td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368">{h(p.get("chu_dau_tu","-"))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;text-align:center">{h(p.get("tong_can","-"))}</td>'
        f'<td style="padding:7px 8px;font-size:12px;font-weight:600;color:{GOLD};text-align:center">{h(p.get("gia_ban_m2","-"))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368;text-align:center">{h(p.get("gia_can_tu","-"))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#3c4043">{hs}</td>'
        f'<td style="padding:7px 8px">{badge(h(p.get("trang_thai","-")), status_color(p.get("trang_thai","")))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#9aa0a6">{src_html}</td>'
        f'<td style="padding:7px 8px;text-align:center">{web_html}</td>'
        f'</tr>'
    )


def fill_needs_scan_projects(projects: list) -> bool:
    """Tim va fill cac truong con thieu cho du an co needs_scan=True."""
    api_key = cfg("DEEPSEEK_API_KEY")
    if not api_key:
        log.warning("fill_needs_scan: thieu DEEPSEEK_API_KEY, bo qua")
        return False

    pending = [p for p in projects if p.get("needs_scan")]
    if not pending:
        log.info("fill_needs_scan: khong co du an nao can fill")
        return False

    log.info(f"fill_needs_scan: {len(pending)} du an can fill")
    filled_count = 0

    FILL_FIELDS = [
        "tinh_tp", "quan_huyen", "vi_tri", "chu_dau_tu", "so_toa", "tong_can",
        "can_ban", "dien_tich_can", "gia_ban_m2", "gia_can_tu",
        "khoi_cong", "nhan_ho_so_tu", "nhan_ho_so_den", "du_kien_ban_giao",
        "doi_tuong_uu_tien", "dieu_kien_mua", "dia_diem_nop_ho_so",
        "website_chu_dau_tu", "lien_he", "quy_mo_dan_so", "anh_phoi_canh",
        "trang_thai", "nguon", "url_nguon",
    ]

    for p in pending:
        name = p.get("ten_du_an", "?")
        tinh = p.get("tinh_tp", "")
        missing = [f for f in FILL_FIELDS if not p.get(f)]
        if not missing:
            p["needs_scan"] = False
            continue

        # Thu thap noi dung ve du an nay
        search_content = search_web(
            f"du an nha o xa hoi {name} {tinh} thong tin chi tiet",
            max_chars=5000,
        )

        prompt_text = f"""Tim kiem thong tin chi tiet ve du an nha o xa hoi sau:

Ten du an: {name}
Tinh/Thanh pho: {tinh}

{"NOI DUNG TU SEARCH:" + chr(10) + search_content if search_content else ""}

Tra ve 1 object JSON duy nhat voi cac truong sau (bo trong neu khong tim duoc):
{json.dumps(missing, ensure_ascii=False)}

Quy tac:
- Gia ghi dang "X trieu/m2" hoac "X ty/can"
- Ngay ghi dang "DD/MM/YYYY" hoac "Quy X/YYYY"
- Chi tra ve JSON object, khong giai thich
- Neu khong co thong tin -> {{}}

Vi du: {{"gia_ban_m2": "15 trieu/m2", "trang_thai": "Dang mo ban", "tong_can": "500"}}"""

        result_data: list[dict] = []
        last_err = None

        for attempt in range(2):
            try:
                full = deepseek_chat(
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "Ban la chuyen gia NOXH Viet Nam. "
                                "Tra ve JSON object duy nhat voi thong tin tim duoc. "
                                "Khong giai thich, khong markdown."
                            ),
                        },
                        {"role": "user", "content": prompt_text},
                    ],
                    max_tokens=1500,
                    temperature=0.1,
                )
                for pat in [r'\{[\s\S]*?\}', r'\[\s*\{[\s\S]*?\}\s*\]']:
                    m = re.search(pat, full)
                    if m:
                        raw = m.group()
                        parsed = json.loads(raw)
                        result_data = [parsed] if isinstance(parsed, dict) else parsed
                        break
                if result_data:
                    break
            except Exception as e:
                last_err = e
                log.debug(f"  fill attempt {attempt+1} error: {e}")
                time.sleep(10)

        if not result_data and last_err:
            log.warning(f"  DeepSeek that bai cho '{name}' -> thu Gemini fallback...")
            result_data = gemini_fallback(prompt_text)

        if result_data:
            found = result_data[0] if result_data else {}
            updated = False
            for field in missing:
                val = found.get(field)
                if val and isinstance(val, (str, int, float)) and str(val).strip():
                    p[field] = str(val).strip()
                    updated = True
            if updated:
                required = ["ten_du_an", "tinh_tp", "chu_dau_tu", "vi_tri", "trang_thai"]
                if all(p.get(f) and str(p[f]).strip() for f in required):
                    p["needs_scan"] = False
                filled_count += 1
                filled_fields = [f for f in missing if p.get(f)]
                log.info(f"  OK Fill: {name[:40]} ({len(filled_fields)} truong)")
            else:
                log.warning(f"  X Khong fill duoc: {name[:40]}")
        else:
            log.warning(f"  X Khong tim thay du lieu: {name[:40]}")

        time.sleep(3)

    log.info(f"fill_needs_scan xong: {filled_count}/{len(pending)} du an duoc fill")
    return filled_count > 0


def build_html(new_ps, all_ps, db, ts, scanned_names):
    region_stats = []
    for name, provinces, _ in SCAN_BATCHES:
        count = sum(1 for p in all_ps if p.get("tinh_tp") in provinces)
        region_stats.append((name, count))

    from collections import Counter
    top_provinces = Counter(p.get("tinh_tp", "-") for p in all_ps).most_common(6)

    stat_cards = ""
    for prov, cnt in top_provinces:
        stat_cards += (
            f'<div class="stat">'
            f'<div style="font-size:10px;color:#9aa0a6;margin-bottom:3px;'
            f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{h(prov)}</div>'
            f'<div style="font-size:22px;font-weight:700;color:{NAV}">{cnt}</div>'
            f'</div>'
        )

    scans = db.get("scans", [])[-20:]
    scan_rows = "".join(
        f'<tr style="background:{"#fff" if i%2==0 else "#f8f9fa"}">'
        f'<td style="padding:5px 9px;font-size:11px;color:#5f6368">{s.get("ts","")[:16]}</td>'
        f'<td style="padding:5px 9px;text-align:center;font-size:12px;font-weight:600;'
        f'color:{"#1A6B3A" if s.get("new",0)>0 else "#9aa0a6"}">{s.get("new",0)}</td>'
        f'<td style="padding:5px 9px;text-align:center;font-size:11px">{s.get("total",0)}</td>'
        f'<td style="padding:5px 9px;font-size:11px;color:#5f6368">{h(s.get("vung","-"))}</td>'
        f'<td style="padding:5px 9px;font-size:10px;color:#e74c3c">{s.get("error","")[:60]}</td>'
        f'</tr>'
        for i, s in enumerate(reversed(scans))
    )

    new_rows = (
        "".join(tbl_row(p, i) for i, p in enumerate(new_ps))
        if new_ps else
        '<tr><td colspan="12" style="padding:28px;text-align:center;'
        'color:#9aa0a6;font-size:13px">Khong co du an moi trong lan quet nay</td></tr>'
    )
    all_rows = "".join(tbl_row(p, i) for i, p in enumerate(all_ps))

    thead = (
        '<tr>'
        '<th style="width:34px">STT</th>'
        '<th style="width:90px">Tinh/TP</th>'
        '<th style="width:200px">Ten du an</th>'
        '<th style="width:140px">Vi tri</th>'
        '<th style="width:140px">Chu dau tu</th>'
        '<th style="width:46px">Can</th>'
        '<th style="width:90px">Gia/m2</th>'
        '<th style="width:90px">Gia tu</th>'
        '<th style="width:130px">Lich nhan HS</th>'
        '<th style="width:110px">Trang thai</th>'
        '<th style="width:130px">Nguon</th>'
        '<th style="width:50px">Web</th>'
        '</tr>'
    )

    css = (
        "* {box-sizing:border-box;margin:0;padding:0}"
        "body {font-family:'Segoe UI',Arial,sans-serif;background:#f1f3f4;color:#3c4043}"
        ".wrap {max-width:1400px;margin:0 auto;padding:20px}"
        ".card {background:#fff;border-radius:10px;border:1px solid #e8eaed;"
        "overflow:hidden;margin-bottom:18px}"
        ".card-h {padding:12px 18px;border-bottom:1px solid #e8eaed;font-weight:600;"
        "font-size:14px;color:" + NAV + ";display:flex;align-items:center;gap:8px}"
        ".stat {background:#fff;border-radius:8px;border:1px solid #e8eaed;"
        "padding:12px 14px;text-align:center}"
        "table {width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed}"
        "th {padding:7px 8px;text-align:left;font-size:11px;font-weight:500;"
        "color:#5f6368;border-bottom:1px solid #e8eaed;background:#f8f9fa;"
        "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
        "td {overflow:hidden;text-overflow:ellipsis;vertical-align:top;word-wrap:break-word}"
        "a {text-decoration:none}"
        ".region-tag {display:inline-block;background:#E8EDF5;color:#1B3A6B;"
        "font-size:10px;font-weight:600;padding:2px 6px;border-radius:3px;margin-right:4px}"
        "@media(max-width:700px){.g-stat{grid-template-columns:1fr 1fr 1fr!important}}"
    )

    scanned_html = " - ".join(
        f'<span class="region-tag">{h(n)}</span>' for n in scanned_names
    ) if scanned_names else "-"

    return f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NOXH Monitor - {ts}</title>
<style>{css}</style>
</head>
<body>
<div class="wrap">

  <!-- Header -->
  <div style="background:{NAV};border-radius:10px;padding:16px 22px;
    margin-bottom:20px;display:flex;align-items:center;gap:14px;flex-wrap:wrap">
    <div style="width:38px;height:38px;background:{GOLD};border-radius:8px;
      display:flex;align-items:center;justify-content:center;flex-shrink:0">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none"
        stroke="white" stroke-width="2">
        <path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/>
        <polyline points="9 22 9 12 15 12 15 22"/>
      </svg>
    </div>
    <div style="flex:1">
      <div style="color:#fff;font-weight:700;font-size:17px">NOXH Monitor - Toan Quoc</div>
      <div style="color:#8fa8c8;font-size:11px;margin-top:3px">
        34 tinh/thanh - Dia gioi 2025 - {ts} - DeepSeek V3
      </div>
      <div style="margin-top:6px">{scanned_html}</div>
    </div>
    <div style="color:#8fa8c8;font-size:12px;text-align:right">
      {"<span style='background:#1A6B3A;color:#fff;padding:3px 10px;border-radius:12px;font-weight:600'>" + str(len(new_ps)) + " DA moi</span>" if new_ps else "<span>Khong co DA moi</span>"}
      <div style="font-size:11px;margin-top:3px">Tong: {len(all_ps)} du an</div>
    </div>
  </div>

  <!-- Stats top tinh -->
  <div class="g-stat" style="display:grid;grid-template-columns:repeat(6,1fr);
    gap:10px;margin-bottom:18px">
    {stat_cards}
  </div>

  <!-- Du an moi -->
  <div class="card">
    <div class="card-h" style="background:{'#D4EDDA' if new_ps else '#f8f9fa'}">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
        stroke="{'#1A6B3A' if new_ps else '#9aa0a6'}" stroke-width="2">
        <circle cx="12" cy="12" r="10"/>
        <line x1="12" y1="8" x2="12" y2="16"/>
        <line x1="8" y1="12" x2="16" y2="12"/>
      </svg>
      <span style="color:{'#1A6B3A' if new_ps else '#5f6368'}">
        Du an moi phat hien lan nay ({len(new_ps)})
      </span>
    </div>
    <div style="overflow-x:auto">
      <table><thead>{thead}</thead><tbody>{new_rows}</tbody></table>
    </div>
  </div>

  <!-- Tat ca du an -->
  <div class="card">
    <div class="card-h">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
        stroke="{NAV}" stroke-width="2">
        <rect x="3" y="3" width="7" height="7"/>
        <rect x="14" y="3" width="7" height="7"/>
        <rect x="14" y="14" width="7" height="7"/>
        <rect x="3" y="14" width="7" height="7"/>
      </svg>
      Tat ca du an dang theo doi ({len(all_ps)}) - 34 tinh/thanh pho
    </div>
    <div style="overflow-x:auto">
      <table><thead>{thead}</thead><tbody>{all_rows}</tbody></table>
    </div>
  </div>

  <!-- Lich su quet -->
  <div class="card">
    <div class="card-h">Lich su quet ({len(scans)} lan gan nhat)</div>
    <div style="overflow-x:auto">
      <table>
        <thead>
          <tr>
            <th style="width:140px">Thoi gian</th>
            <th style="width:70px;text-align:center">DA moi</th>
            <th style="width:70px;text-align:center">Tong DB</th>
            <th style="width:200px">Vung quet</th>
            <th>Loi</th>
          </tr>
        </thead>
        <tbody>
          {scan_rows if scan_rows else
           '<tr><td colspan="5" style="padding:18px;text-align:center;'
           'color:#9aa0a6;font-size:12px">Chua co</td></tr>'}
        </tbody>
      </table>
    </div>
  </div>

  <!-- Footer -->
  <div style="text-align:center;color:#9aa0a6;font-size:11px;padding:10px 0">
    NOXH Monitor - 34 tinh/thanh - Dia gioi hanh chinh 2025 (NQ 202/2025/QH15) - {ts}
  </div>

</div>
</body>
</html>"""


# -- Email ----------------------------------------------------------------------
def send_email(new_ps, all_ps, ts, scanned_names):
    su    = cfg("SMTP_USER")
    sp    = cfg("SMTP_PASS")
    eto   = [e.strip() for e in cfg("EMAIL_TO").split(",") if e.strip()]
    pages = cfg("PAGES_URL")

    if not su:
        log.warning("Thieu SMTP_USER"); return False
    if not eto:
        log.warning("Thieu EMAIL_TO"); return False

    subj = (
        f"[NOXH] {len(new_ps)} du an moi - {ts}"
        if new_ps else
        f"[NOXH] Khong co du an moi - {ts}"
    )

    if new_ps:
        cards = ""
        for p in new_ps:
            hs = h(p.get("nhan_ho_so_tu", "") or p.get("khoi_cong", "") or "-")
            if p.get("nhan_ho_so_den"):
                hs += " -> " + h(p["nhan_ho_so_den"])
            gia = h(p.get("gia_ban_m2", ""))
            gia_tu = h(p.get("gia_can_tu", ""))
            gia_html = (
                f'<tr><td style="color:#9aa0a6;padding:3px 0;width:110px;font-size:12px">Gia</td>'
                f'<td style="font-weight:600;color:{GOLD};font-size:12px">'
                f'{gia}{"  |  Tu " + gia_tu if gia_tu and gia_tu != "-" else ""}</td></tr>'
            ) if (gia and gia != "-") or (gia_tu and gia_tu != "-") else ""

            trang_thai = h(p.get("trang_thai", ""))
            tt_html = (
                f'<tr><td style="color:#9aa0a6;padding:3px 0;font-size:12px">Trang thai</td>'
                f'<td><span style="background:#D4EDDA;color:#1A6B3A;font-size:11px;'
                f'padding:2px 8px;border-radius:4px;font-weight:600">{trang_thai}</span></td></tr>'
            ) if trang_thai and trang_thai != "-" else ""

            nguon = h(p.get("nguon", ""))
            ng_html = (
                f'<tr><td style="color:#9aa0a6;padding:3px 0;font-size:12px">Nguon</td>'
                f'<td style="font-size:11px;color:#9aa0a6">{nguon}</td></tr>'
            ) if nguon and nguon != "-" else ""

            ten_tm = (
                f'<div style="color:#9aa0a6;font-size:12px;margin-bottom:8px">'
                f'{h(p["ten_thuong_mai"])}</div>'
            ) if p.get("ten_thuong_mai") else ""

            prov = h(p.get("tinh_tp", "-"))
            cards += f"""
<div style="border:1px solid #e8eaed;border-radius:8px;padding:14px 16px;
  margin-bottom:14px;background:#fff">
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
    <span style="background:#E8EDF5;color:#1B3A6B;font-size:10px;font-weight:600;
      padding:2px 7px;border-radius:4px">{prov}</span>
    <span style="font-weight:700;color:{NAV};font-size:14px">{h(p.get("ten_du_an","-"))}</span>
  </div>
  {ten_tm}
  <table style="width:100%;border-collapse:collapse">
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;width:110px;font-size:12px">Vi tri</td>
      <td style="color:#3c4043;font-size:12px">
        {h(p.get("vi_tri","-"))}
        {" - " + h(p["quan_huyen"]) if p.get("quan_huyen") else ""}
      </td>
    </tr>
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px">Chu dau tu</td>
      <td style="color:#3c4043;font-size:12px">{h(p.get("chu_dau_tu","-"))}</td>
    </tr>
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px">Quy mo</td>
      <td style="color:#3c4043;font-size:12px">
        {h(p.get("tong_can","-"))} can
        {" | " + h(p["dien_tich_can"]) if p.get("dien_tich_can") else ""}
      </td>
    </tr>
    {gia_html}
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px">Lich nhan HS</td>
      <td style="font-weight:600;color:#1A6B3A;font-size:12px">{hs}</td>
    </tr>
    {tt_html}
    {ng_html}
  </table>
</div>"""

        body_content = (
            f'<p style="color:#3c4043;font-size:14px;margin-bottom:16px">'
            f'Tim thay <strong style="color:{NAV}">{len(new_ps)} du an moi</strong>'
            f' trong lan quet hom nay:</p>\n{cards}'
        )
    else:
        body_content = (
            '<p style="color:#5f6368;font-size:14px;text-align:center;padding:30px 0">'
            'Khong tim thay du an moi trong lan quet hom nay.<br>'
            '<span style="font-size:12px;color:#9aa0a6">'
            'He thong se tiep tuc theo doi va thong bao khi co thong tin moi.'
            '</span></p>'
        )

    btn_html = ""
    if pages:
        btn_html = (
            f'<div style="text-align:center;margin:24px 0 8px">'
            f'<a href="{safe_url(pages)}" style="background:{NAV};color:#fff;'
            f'text-decoration:none;padding:12px 32px;border-radius:7px;'
            f'font-weight:600;font-size:14px;display:inline-block">'
            f'Xem toan bo {len(all_ps)} du an</a></div>'
        )

    scanned_str = " - ".join(scanned_names) if scanned_names else "-"
    body_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;background:#f1f3f4;margin:0;padding:20px">
  <div style="max-width:600px;margin:0 auto">
    <div style="background:{NAV};border-radius:10px 10px 0 0;padding:16px 22px;
      display:flex;align-items:center;gap:12px">
      <div style="width:36px;height:36px;background:{GOLD};border-radius:7px;
        display:flex;align-items:center;justify-content:center;font-size:18px">*</div>
      <div>
        <div style="color:#fff;font-weight:700;font-size:16px">NOXH Monitor - Toan Quoc</div>
        <div style="color:#8fa8c8;font-size:11px">Bao cao tu dong - {ts}</div>
        <div style="color:#8fa8c8;font-size:10px;margin-top:2px">Vung quet: {scanned_str}</div>
      </div>
    </div>
    <div style="background:#fff;padding:22px;border-radius:0 0 10px 10px;
      border:1px solid #e8eaed;border-top:none">
      {body_content}
      {btn_html}
      <hr style="border:none;border-top:1px solid #e8eaed;margin:20px 0">
      <p style="color:#9aa0a6;font-size:11px;text-align:center;margin:0">
        NOXH Monitor - 34 tinh/thanh pho - Dia gioi hanh chinh 2025<br>
        Quet tu dong hang tuan
      </p>
    </div>
  </div>
</body></html>"""

    msg = MIMEMultipart("alternative")
    msg["From"]    = f"NOXH Monitor <{su}>"
    msg["To"]      = ", ".join(eto)
    msg["Subject"] = subj
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            if SMTP_HOST not in ("localhost", "127.0.0.1"):
                s.starttls()
                if su and sp:
                    s.login(su, sp)
            s.sendmail(su or f"noxh@{SMTP_HOST}", eto, msg.as_string())
        log.info(f"Email OK -> {', '.join(eto)}")
        return True
    except Exception as e:
        log.error(f"Email loi: {e}")
        return False


# -- Main -----------------------------------------------------------------------
def main():
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    log.info("=" * 60)
    log.info(f"NOXH Monitor - Toan Quoc - {ts}")
    log.info(f"34 tinh/thanh - FULL_SCAN={FULL_SCAN} - DRY_RUN={DRY_RUN} - DeepSeek V3")
    log.info("=" * 60)

    db = load_db()

    # 1. Quet
    try:
        raw, scanned_names = run_scan(db)
    except Exception as e:
        log.error(f"Scan loi: {e}")
        db.setdefault("scans", []).append({
            "ts": ts, "found": 0, "new": 0,
            "total": len(db.get("projects", [])),
            "vung": "-", "error": str(e),
        })
        save_db(db)
        sys.exit(1)

    # 2. Merge vao DB
    all_ps, new_ps = dedup(db.get("projects", []), raw)
    db["projects"] = all_ps
    db.setdefault("scans", []).append({
        "ts": ts, "found": len(raw),
        "new": len(new_ps), "total": len(all_ps),
        "vung": " - ".join(scanned_names),
    })
    db["scans"] = db["scans"][-100:]
    save_db(db)
    log.info(f"raw={len(raw)} | new={len(new_ps)} | total={len(all_ps)}")

    # 2b. Geocoding tu dong
    if not DRY_RUN:
        if geocode_projects(all_ps):
            save_db(db)

    # 2c. Fill du an them thu cong con thieu thong tin
    if not DRY_RUN:
        if fill_needs_scan_projects(all_ps):
            db["projects"] = all_ps
            save_db(db)
            log.info("fill_needs_scan: da luu du lieu da fill")

    # 3. Dong bo sang web-app
    sync_webapp(all_ps)

    # 4. Tao HTML report
    report_html = build_html(new_ps, all_ps, db, ts, scanned_names)
    (DOCS / "index.html").write_text(report_html, encoding="utf-8")
    archive = f"report_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
    (DOCS / archive).write_text(report_html, encoding="utf-8")
    for old in sorted(DOCS.glob("report_*.html"), reverse=True)[30:]:
        old.unlink()
    log.info(f"HTML -> docs/index.html + docs/{archive}")

    # 5. Gui email
    if DRY_RUN:
        log.info("DRY_RUN -> bo qua email")
    elif FORCE_EMAIL or new_ps:
        send_email(new_ps, all_ps, ts, scanned_names)
    else:
        log.info("Khong co DA moi -> bo qua email")

    log.info("Hoan thanh.")


if __name__ == "__main__":
    main()
