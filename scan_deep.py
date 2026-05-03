#!/usr/bin/env python3
"""
scan_deep.py – Quét chuyên sâu 1 lần, toàn bộ NOXH Hà Nội
Chạy nhiều query khác nhau, gộp + dedup kết quả vào database.
Dùng để bootstrap database lần đầu hoặc cập nhật toàn diện.
"""
import os, sys, json, re, time, logging, html, shutil
from datetime import datetime
from pathlib import Path
import anthropic

def cfg(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

WEBAPP_DATA = Path(cfg("WEBAPP_DATA_PATH", str(Path(__file__).parent / "../web-app/public/data.json")))


def h(text):
    """Escape HTML entities để tránh XSS."""
    return html.escape(str(text)) if text and text != "—" else text

def safe_url(url):
    """Chỉ cho phép http/https để tránh javascript: URI injection."""
    u = (url or "").strip()
    return u if u.startswith(("http://", "https://")) else ""

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
log = logging.getLogger("noxh-deep")

# ─── Province normalization (địa giới 2025 theo NQ 202/2025/QH15) ─────────────
PROVINCE_NORMALIZE: dict[str, str] = {
    # ASCII / không dấu fallback
    "Ha Noi": "Hà Nội",
    "TP Ho Chi Minh": "TP. Hồ Chí Minh",
    "Ho Chi Minh": "TP. Hồ Chí Minh",
    "Da Nang": "Đà Nẵng",
    "Hai Phong": "Hải Phòng",
    "Can Tho": "Cần Thơ",
    "Hue": "Thành phố Huế",
    # TP trực thuộc TW giữ nguyên / viết lại
    "Hà Nội": "Hà Nội",
    "TP. Hồ Chí Minh": "TP. Hồ Chí Minh",
    "Hồ Chí Minh": "TP. Hồ Chí Minh",
    "TP.HCM": "TP. Hồ Chí Minh",
    "TPHCM": "TP. Hồ Chí Minh",
    "Tp.HCM": "TP. Hồ Chí Minh",
    "Hải Phòng": "Hải Phòng",
    "TP. Hải Phòng": "Hải Phòng",
    "Đà Nẵng": "Đà Nẵng",
    "TP. Đà Nẵng": "Đà Nẵng",
    "Thành phố Huế": "Thành phố Huế",
    "Huế": "Thành phố Huế",
    "TT. Huế": "Thành phố Huế",
    "Thừa Thiên Huế": "Thành phố Huế",
    "Thừa Thiên - Huế": "Thành phố Huế",
    "Cần Thơ": "Cần Thơ",
    "TP. Cần Thơ": "Cần Thơ",
    # Tỉnh giữ nguyên
    "Cao Bằng": "Cao Bằng",
    "Lai Châu": "Lai Châu",
    "Điện Biên": "Điện Biên",
    "Lạng Sơn": "Lạng Sơn",
    "Sơn La": "Sơn La",
    "Quảng Ninh": "Quảng Ninh",
    "Thanh Hóa": "Thanh Hóa",
    "Nghệ An": "Nghệ An",
    "Hà Tĩnh": "Hà Tĩnh",
    # Tỉnh đã sáp nhập
    "Hà Giang": "Tuyên Quang",
    "Tuyên Quang": "Tuyên Quang",
    "Lào Cai": "Lào Cai",
    "Yên Bái": "Lào Cai",
    "Bắc Kạn": "Thái Nguyên",
    "Thái Nguyên": "Thái Nguyên",
    "Hòa Bình": "Phú Thọ",
    "Vĩnh Phúc": "Phú Thọ",
    "Phú Thọ": "Phú Thọ",
    "Bắc Giang": "Bắc Ninh",
    "Bắc Ninh": "Bắc Ninh",
    "Hải Dương": "Hải Phòng",
    "Thái Bình": "Hưng Yên",
    "Hưng Yên": "Hưng Yên",
    "Hà Nam": "Ninh Bình",
    "Nam Định": "Ninh Bình",
    "Ninh Bình": "Ninh Bình",
    "Quảng Bình": "Quảng Trị",
    "Quảng Trị": "Quảng Trị",
    "Quảng Nam": "Đà Nẵng",
    "Kon Tum": "Quảng Ngãi",
    "Quảng Ngãi": "Quảng Ngãi",
    "Bình Định": "Gia Lai",
    "Gia Lai": "Gia Lai",
    "Phú Yên": "Đắk Lắk",
    "Đắk Lắk": "Đắk Lắk",
    "Khánh Hòa": "Khánh Hòa",
    "Ninh Thuận": "Khánh Hòa",
    "Đắk Nông": "Lâm Đồng",
    "Lâm Đồng": "Lâm Đồng",
    "Bình Thuận": "Lâm Đồng",
    "Bình Phước": "Đồng Nai",
    "Đồng Nai": "Đồng Nai",
    "Bà Rịa - Vũng Tàu": "TP. Hồ Chí Minh",
    "Bà Rịa-Vũng Tàu": "TP. Hồ Chí Minh",
    "BR-VT": "TP. Hồ Chí Minh",
    "Bình Dương": "TP. Hồ Chí Minh",
    "Long An": "Tây Ninh",
    "Tây Ninh": "Tây Ninh",
    "Tiền Giang": "Đồng Tháp",
    "Đồng Tháp": "Đồng Tháp",
    "An Giang": "An Giang",
    "Kiên Giang": "An Giang",
    "Bến Tre": "Vĩnh Long",
    "Vĩnh Long": "Vĩnh Long",
    "Trà Vinh": "Vĩnh Long",
    "Sóc Trăng": "Cần Thơ",
    "Hậu Giang": "Cần Thơ",
    "Bạc Liêu": "Cà Mau",
    "Cà Mau": "Cà Mau",
}


def normalize_province(raw: str) -> str:
    """Chuẩn hoá tên tỉnh/thành → tên mới theo địa giới 2025."""
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


def sync_webapp(projects: list) -> None:
    """Đồng bộ danh sách dự án đã chuẩn hoá sang web-app/public/data.json."""
    try:
        path = WEBAPP_DATA.resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"projects": projects, "updated": datetime.now().isoformat()}
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info(f"Đồng bộ web-app OK: {path} ({len(projects)} dự án)")
    except Exception as e:
        log.warning(f"Không sync được web-app data: {e}")


# ─── Geocoding ─────────────────────────────────────────────────────────────────

PROVINCE_CAPITALS: dict[str, tuple[float, float]] = {
    "Hà Nội": (21.0285, 105.8542),
    "Hải Phòng": (20.8449, 106.6881),
    "Bắc Ninh": (21.1861, 106.0763),
    "Hưng Yên": (20.6464, 106.0511),
    "Quảng Ninh": (21.0064, 107.2925),
    "Phú Thọ": (21.3450, 105.2230),
    "Thái Nguyên": (21.5928, 105.8442),
    "Lào Cai": (22.4809, 103.9754),
    "Sơn La": (21.3272, 103.9144),
    "Tuyên Quang": (21.8239, 105.2180),
    "Lạng Sơn": (21.8537, 106.7615),
    "Cao Bằng": (22.6657, 106.2639),
    "Lai Châu": (22.3964, 103.4580),
    "Điện Biên": (21.3860, 103.0230),
    "Ninh Bình": (20.2541, 105.9765),
    "Thanh Hóa": (19.8067, 105.7852),
    "Nghệ An": (18.6796, 105.6813),
    "Hà Tĩnh": (18.3428, 105.9057),
    "Quảng Trị": (16.7403, 107.1857),
    "Thành phố Huế": (16.4637, 107.5909),
    "Đà Nẵng": (16.0544, 108.2022),
    "Quảng Ngãi": (15.1200, 108.7922),
    "Gia Lai": (13.9833, 108.0000),
    "Đắk Lắk": (12.6667, 108.0500),
    "Khánh Hòa": (12.2388, 109.1967),
    "Lâm Đồng": (11.9465, 108.4419),
    "TP. Hồ Chí Minh": (10.8231, 106.6297),
    "Đồng Nai": (10.9455, 107.1684),
    "Tây Ninh": (11.3352, 106.1099),
    "Đồng Tháp": (10.4937, 105.6882),
    "An Giang": (10.5216, 105.1259),
    "Vĩnh Long": (10.2538, 106.0000),
    "Cần Thơ": (10.0452, 105.7469),
    "Cà Mau": (9.1769, 105.1500),
}


def geocode_projects(projects: list) -> bool:
    """Geocode các dự án còn thiếu tọa độ bằng Nominatim OSM.
    Mutates projects in-place. Returns True nếu có ít nhất 1 dự án được geocode.
    """
    import urllib.request
    import urllib.parse

    needs_geocode = [
        p for p in projects
        if not p.get("toado_lat") or p.get("toado_lat") == 0
    ]
    if not needs_geocode:
        log.info("Geocoding: tất cả dự án đã có tọa độ")
        return False

    log.info(f"Geocoding: {len(needs_geocode)} dự án cần tọa độ")
    geocoded_count = 0
    headers = {"User-Agent": "NOXH-Monitor/1.0 (noxh-monitor)"}

    def nominatim_search(query: str) -> tuple[float, float] | None:
        params = urllib.parse.urlencode({
            "q": query, "format": "json", "limit": "1", "countrycodes": "vn",
        })
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception as e:
            log.debug(f"Nominatim lỗi '{query}': {e}")
        return None

    for p in needs_geocode:
        tinh = p.get("tinh_tp", "")
        vi_tri = p.get("vi_tri", "")
        quan_huyen = p.get("quan_huyen", "")
        result: tuple[float, float] | None = None

        # Tier 1: địa chỉ đầy đủ
        if vi_tri and tinh:
            result = nominatim_search(f"{vi_tri}, {tinh}, Việt Nam")
            time.sleep(1.2)

        # Tier 2: quận/huyện + tỉnh
        if not result and quan_huyen and tinh:
            result = nominatim_search(f"{quan_huyen}, {tinh}, Việt Nam")
            time.sleep(1.2)

        # Tier 3: tọa độ tỉnh lỵ (fallback)
        if not result and tinh in PROVINCE_CAPITALS:
            result = PROVINCE_CAPITALS[tinh]
            log.debug(f"Dùng tọa độ tỉnh lỵ cho: {p.get('ten_du_an')}")

        if result:
            p["toado_lat"], p["toado_lng"] = result[0], result[1]
            geocoded_count += 1
            log.info(f"  ✓ {p.get('ten_du_an', '?')[:40]} → {result[0]:.4f}, {result[1]:.4f}")
        else:
            log.warning(f"  ✗ Không tìm được tọa độ: {p.get('ten_du_an', '?')[:40]}")

    log.info(f"Geocoding xong: {geocoded_count}/{len(needs_geocode)} thành công")
    return geocoded_count > 0


# ─── Danh sách query chuyên sâu ───────────────────────────────────────────────
# Mỗi query tập trung vào 1 góc độ khác nhau để bao phủ tối đa
QUERIES = [
    # 1. Sở Xây dựng Hà Nội – trang chính thức
    {
        "id": "SXD_official",
        "desc": "Sở XD HN – trang chính thức nhà ở xã hội",
        "prompt": """Truy cập và đọc nội dung trang https://soxaydung.hanoi.gov.vn/vi-vn/trang/nha-o-xa-hoi/536304
Liệt kê TẤT CẢ dự án nhà ở xã hội được công bố tại đây: tên dự án, địa điểm, chủ đầu tư, số căn, giá bán, thời gian nhận hồ sơ.
Sau đó tìm kiếm thêm: site:soxaydung.hanoi.gov.vn "nhà ở xã hội" 2025 2026
Trả về JSON array đầy đủ."""
    },
    # 2. SXD HN – thông báo mở bán gần đây
    {
        "id": "SXD_announcements",
        "desc": "SXD HN – thông báo mở bán / tiếp nhận hồ sơ 2025-2026",
        "prompt": """Tìm kiếm: site:soxaydung.hanoi.gov.vn "tiếp nhận hồ sơ" OR "mở bán" nhà ở xã hội 2025 2026
Và: sở xây dựng hà nội thông báo tiếp nhận hồ sơ nhà ở xã hội 2026
Liệt kê TẤT CẢ thông báo tiếp nhận hồ sơ mua nhà ở xã hội tại Hà Nội từ tháng 1/2025 đến nay.
Trả về JSON array đầy đủ."""
    },
    # 3. Các quận nội thành
    {
        "id": "inner_districts",
        "desc": "NOXH nội thành HN: Thanh Xuân, Hoàng Mai, Long Biên, Nam Từ Liêm",
        "prompt": """Tìm kiếm tất cả dự án nhà ở xã hội tại các quận nội thành Hà Nội:
- Quận Thanh Xuân: N01 Hạ Đình (UDIC Ecotower), 275 Nguyễn Trãi
- Quận Hoàng Mai: Hacinco Nguyễn Xiển, 321 Vĩnh Hưng, FS Thịnh Liệt, X2 Lĩnh Nam, 393 Lĩnh Nam
- Quận Long Biên: Rice City Long Châu, Him Lam Phúc Lợi, CT2A Thạch Bàn
- Quận Nam Từ Liêm: NHS Trung Văn, NHS Phương Canh, Tây Nam Mễ Trì
- Quận Cầu Giấy: bất kỳ dự án NOXH nào
- Quận Hà Đông: Kiến Hưng, Phú Lãm
Tìm thông tin mới nhất 2025-2026 về từng dự án.
Trả về JSON array."""
    },
    # 4. Các huyện ngoại thành
    {
        "id": "outer_districts",
        "desc": "NOXH ngoại thành HN: Đông Anh, Mê Linh, Thanh Trì, Hoài Đức",
        "prompt": """Tìm kiếm tất cả dự án nhà ở xã hội tại các huyện ngoại thành Hà Nội:
- Huyện Đông Anh: CT3/CT4 Kim Chung (Thăng Long Green City), 319 Uy Nỗ (Calyx Residence), 5.B2 Đông Hội (Bamboo Garden/CEO Group)
- Huyện Mê Linh: Kim Hoa Garden City, CT-05/CT-06 HUD Thanh Lâm, Sunrise Home Ngọc Hồi
- Huyện Thanh Trì: N01 Hạ Đình, Hồng Hà Eco City, Valenta Nguyễn Xiển, F3 Thanh Liệt
- Huyện Hoài Đức: Tân Lập Ô Diên, các DA khác
- Huyện Tây Hồ: X1 Phú Thượng
Tìm thông tin mới nhất 2025-2026 về từng dự án: giá, tiến độ, lịch nhận hồ sơ.
Trả về JSON array."""
    },
    # 5. Cafef + Báo xây dựng – tin tức mới nhất
    {
        "id": "news_cafef",
        "desc": "Tin tức NOXH HN từ CafeF, Báo XD, VnExpress",
        "prompt": """Tìm kiếm tin tức mới nhất (2025-2026) về nhà ở xã hội Hà Nội từ các nguồn:
cafef.vn, baoxaydung.vn, vnexpress.net, tuoitre.vn, tienphong.vn
Query: "nhà ở xã hội" "hà nội" "mở bán" OR "khởi công" OR "nhận hồ sơ" 2025 2026

Đặc biệt tìm các dự án:
- Vừa được SXD HN công bố giá bán
- Vừa khởi công trong 6 tháng gần đây
- Sắp nhận hồ sơ trong 3 tháng tới
Trả về JSON array với nguồn và ngày đăng cụ thể."""
    },
    # 6. Thuvienphapluat + Kinhtedothi
    {
        "id": "news_legal",
        "desc": "NOXH HN từ thuvienphapluat, kinhtedothi, laodong",
        "prompt": """Tìm kiếm từ các nguồn: thuvienphapluat.vn, kinhtedothi.vn, laodong.vn, markettimes.vn
Query: nhà ở xã hội hà nội 2025 2026 mở bán tiếp nhận hồ sơ

Tìm thêm danh sách 39 dự án NOXH Hà Nội trong Quyết định 02/QĐ-UBND năm 2025 (UBND TP Hà Nội).
Liệt kê tên dự án, địa điểm, quy mô nếu có.
Trả về JSON array."""
    },
    # 7. NOXH Lực lượng vũ trang tại HN
    {
        "id": "military_police",
        "desc": "NOXH dành cho lực lượng vũ trang, Công an, Quân đội tại HN",
        "prompt": """Tìm tất cả dự án nhà ở xã hội dành cho lực lượng vũ trang tại Hà Nội năm 2025-2026:
- Bộ Công an: 275 Nguyễn Trãi, X1 Phú Thượng, Valenta Nguyễn Xiển, F3 Thanh Liệt và các DA khác
- Bộ Quốc phòng: CT2A Thạch Bàn, 319 Uy Nỗ và các DA khác
Tìm: "nhà ở xã hội" "công an" OR "quân đội" OR "lực lượng vũ trang" hà nội 2025 2026
Trả về JSON array với đầy đủ thông tin."""
    },
    # 8. Danh sách 148 dự án NOXH HN giai đoạn 2026-2030
    {
        "id": "pipeline_2026_2030",
        "desc": "Pipeline 148 DA NOXH HN giai đoạn 2026-2030",
        "prompt": """Sở Xây dựng Hà Nội đã rà soát 148 dự án nhà ở xã hội với tổng 135.602 căn hộ giai đoạn 2026-2030.
UBND TP Hà Nội ban hành Quyết định 02/QĐ-UBND năm 2025 công bố 39 dự án đợt 1.

Tìm kiếm: "148 dự án" OR "39 dự án" nhà ở xã hội hà nội 2026 2030
Và: quyết định 02/QĐ-UBND 2025 nhà ở xã hội hà nội

Liệt kê tất cả dự án có tên, địa điểm, quy mô căn hộ.
Trả về JSON array."""
    },
]

# ─── JSON schema cho response ─────────────────────────────────────────────────
JSON_SCHEMA = """[
  {
    "ten_du_an": "Tên đầy đủ theo văn bản chính thức",
    "ten_thuong_mai": "",
    "tinh_tp": "Hà Nội",
    "quan_huyen": "",
    "vi_tri": "Địa chỉ chi tiết",
    "chu_dau_tu": "Tên đầy đủ chủ đầu tư",
    "so_toa": "",
    "tong_can": "",
    "can_ban": "",
    "dien_tich_can": "",
    "gia_ban_m2": "",
    "gia_can_tu": "",
    "khoi_cong": "",
    "nhan_ho_so_tu": "",
    "nhan_ho_so_den": "",
    "du_kien_ban_giao": "",
    "doi_tuong_uu_tien": "",
    "dia_diem_nop_ho_so": "",
    "website_chu_dau_tu": "",
    "quy_mo_dan_so": "",
    "lien_he": "",
    "anh_phoi_canh": "",
    "trang_thai": "Đang nhận HS / Sắp nhận HS / Vừa khởi công / Đang thi công / Pipeline 2026-2030",
    "nguon": "Tên báo/website + ngày đăng",
    "url_nguon": "",
    "ghi_chu": ""
  }
]"""

FULL_PROMPT_TEMPLATE = """{specific_query}

Trả về DUY NHẤT một JSON array hợp lệ theo schema sau (không có text gì khác):
{schema}

Quan trọng:
- Chỉ điền thông tin bạn TÌM THẤY ĐƯỢC, để trống ("") nếu không có
- Ghi rõ nguồn và URL trong trường "nguon" và "url_nguon"
- Nếu không tìm thấy thông tin gì, trả về: []"""

# ─── Helpers ─────────────────────────────────────────────────────────────────
def load_db():
    if DB_FILE.exists():
        try:
            return json.loads(DB_FILE.read_text("utf-8"))
        except Exception:
            bak = DB_FILE.with_suffix(".json.bak")
            if bak.exists():
                try:
                    log.warning("projects.json bị lỗi, thử load từ backup...")
                    return json.loads(bak.read_text("utf-8"))
                except Exception:
                    pass
            log.error("Không load được DB và backup, khởi tạo mới (dữ liệu cũ đã corrupt)")
    return {"projects": [], "scans": [], "updated": ""}

def save_db(db):
    db["updated"] = datetime.now().isoformat()
    if DB_FILE.exists():
        shutil.copy2(DB_FILE, DB_FILE.with_suffix(".json.bak"))
    DB_FILE.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

def parse_json(text):
    for pat in [r'\[\s*\{[\s\S]*?\}\s*\]', r'\[\s*\]']:
        m = re.search(pat, text)
        if m:
            try:
                return json.loads(m.group())
            except Exception:
                pass
    return []

def dedup_key(p):
    return (p.get("ten_du_an","") + p.get("quan_huyen","")).lower().strip()

def merge(existing, new_list):
    ex_keys = {dedup_key(p) for p in existing}
    truly_new = [p for p in new_list if dedup_key(p) not in ex_keys]
    # Với DA đã có, cập nhật trường còn trống nếu có data mới hơn
    updated = 0
    for new_p in new_list:
        k = dedup_key(new_p)
        if k in ex_keys:
            for ep in existing:
                if dedup_key(ep) == k:
                    for field, val in new_p.items():
                        if val and not ep.get(field):
                            ep[field] = val
                            updated += 1
                    break
    return existing + truly_new, truly_new, updated

# ─── Single query runner ──────────────────────────────────────────────────────
def gemini_fallback(prompt_text: str) -> list[dict]:
    """Fallback sang Gemini 2.0 Flash với Google Search Grounding khi Claude thất bại."""
    api_key = cfg("GEMINI_API_KEY")
    if not api_key:
        log.warning("  Gemini fallback: thiếu GEMINI_API_KEY trong .env")
        return []
    try:
        from google import genai as ggenai
        from google.genai import types as gtypes
    except ImportError:
        log.warning("  Gemini fallback: chưa cài thư viện (pip install google-genai)")
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
        log.info(f"  Gemini fallback response: {len(full)} ký tự")
        for pat in [r'\[\s*\{[\s\S]*?\}\s*\]', r'\[\s*\]']:
            m = re.search(pat, full)
            if m:
                try:
                    data = json.loads(m.group())
                    for p in data:
                        if p.get("tinh_tp"):
                            p["tinh_tp"] = normalize_province(p["tinh_tp"])
                    log.info(f"  Gemini fallback OK: {len(data)} dự án")
                    return data
                except Exception:
                    pass
        log.warning("  Gemini fallback: không parse được JSON")
        return []
    except Exception as e:
        log.error(f"  Gemini fallback lỗi: {e}")
        return []


def run_query(client, query_config, delay=3):
    qid   = query_config["id"]
    desc  = query_config["desc"]
    prompt_text = FULL_PROMPT_TEMPLATE.format(
        specific_query=query_config["prompt"],
        schema=JSON_SCHEMA,
    )

    log.info(f"  [{qid}] {desc}")

    # Rate limit safety
    time.sleep(delay)

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4000,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": prompt_text}],
        )
        full = "".join(b.text for b in msg.content if b.type == "text")
        results = parse_json(full)
        for p in results:
            if p.get("tinh_tp"):
                p["tinh_tp"] = normalize_province(p["tinh_tp"])
        log.info(f"  [{qid}] → {len(results)} dự án")
        return results
    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg or "rate_limit" in err_msg:
            log.warning(f"  [{qid}] Rate limit, chờ 90s rồi thử lại...")
            time.sleep(90)
            try:
                msg = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=4000,
                    tools=[{"type": "web_search_20250305", "name": "web_search"}],
                    messages=[{"role": "user", "content": prompt_text}],
                )
                full = "".join(b.text for b in msg.content if b.type == "text")
                results = parse_json(full)
                for p in results:
                    if p.get("tinh_tp"):
                        p["tinh_tp"] = normalize_province(p["tinh_tp"])
                log.info(f"  [{qid}] Retry OK → {len(results)} DA")
                return results
            except Exception as e2:
                log.error(f"  [{qid}] Claude retry thất bại → Gemini fallback: {e2}")
                return gemini_fallback(prompt_text)
        log.error(f"  [{qid}] Claude lỗi → Gemini fallback: {e}")
        return gemini_fallback(prompt_text)

# ─── Build summary report ─────────────────────────────────────────────────────
NAV, GOLD, GRN = "#0B2545", "#C9932A", "#1A6B3A"

def badge(text, color):
    p = {"green":("#D4EDDA","#1A6B3A"),"amber":("#FEF3C7","#D97706"),
         "navy":("#E8EDF5","#1B3A6B"),"gray":("#F1F3F4","#5F6368")}
    bg,fg = p.get(color, p["gray"])
    return f'<span style="background:{bg};color:{fg};font-size:11px;font-weight:600;padding:2px 8px;border-radius:4px">{text}</span>'

def sc(ts):
    t = (ts or "").lower()
    if "nhận" in t or "mở" in t: return "green"
    if "sắp" in t or "dự kiến" in t: return "amber"
    if "khởi" in t: return "navy"
    return "gray"

def build_report(all_ps, scan_log, ts):
    rows = ""
    for i,p in enumerate(all_ps):
        bg = "#fff" if i%2==0 else "#f8f9fa"
        if p.get('nhan_ho_so_tu') and p.get('nhan_ho_so_den'):
            hs = h(p['nhan_ho_so_tu']) + " → " + h(p['nhan_ho_so_den'])
        else:
            hs = h(p.get('nhan_ho_so_tu') or p.get('khoi_cong') or "—")
        url = safe_url(p.get("url_nguon", ""))
        nguon_text = h(p.get("nguon","—"))[:50]
        sl = f'<a href="{url}" target="_blank" rel="noopener noreferrer" style="color:{GOLD}">{nguon_text}</a>' if url else nguon_text
        ten_tm_html = f'<br><small style="font-weight:400;color:#9aa0a6">{h(p["ten_thuong_mai"])}</small>' if p.get('ten_thuong_mai') else ''
        rows += f"""<tr style="background:{bg};border-bottom:1px solid #e8eaed">
          <td style="padding:7px 9px;color:#9aa0a6;font-size:11px;text-align:center">{i+1}</td>
          <td style="padding:7px 9px">{badge(h(p.get('tinh_tp','—')),'navy')}</td>
          <td style="padding:7px 9px;font-weight:600;color:{NAV};font-size:12px;max-width:220px;line-height:1.35">{h(p.get('ten_du_an','—'))}{ten_tm_html}</td>
          <td style="padding:7px 9px;font-size:11px;color:#5f6368">{h(p.get('quan_huyen','—'))}</td>
          <td style="padding:7px 9px;font-size:11px;color:#5f6368;max-width:150px">{h(p.get('chu_dau_tu','—'))}</td>
          <td style="padding:7px 9px;font-size:11px;text-align:center">{h(p.get('tong_can','—'))}</td>
          <td style="padding:7px 9px;font-size:11px;font-weight:600;color:{GOLD};text-align:center">{h(p.get('gia_ban_m2','—'))}</td>
          <td style="padding:7px 9px;font-size:11px;color:#5f6368;white-space:nowrap">{hs}</td>
          <td style="padding:7px 9px">{badge(h(p.get('trang_thai','—')),sc(p.get('trang_thai','')))}</td>
          <td style="padding:7px 9px;font-size:10px;color:#9aa0a6">{sl}</td>
        </tr>"""

    scan_rows = "".join(
        f'<tr style="background:{"#fff" if i%2==0 else "#f8f9fa"}">'
        f'<td style="padding:5px 9px;font-size:11px;color:#5f6368">{s["id"]}</td>'
        f'<td style="padding:5px 9px;font-size:11px;color:#5f6368">{s["desc"]}</td>'
        f'<td style="padding:5px 9px;text-align:center;font-size:12px;font-weight:600;color:{"#1A6B3A" if s["found"]>0 else "#9aa0a6"}">{s["found"]}</td>'
        f'<td style="padding:5px 9px;text-align:center;font-size:12px;font-weight:600;color:#1B3A6B">{s["new"]}</td>'
        f'<td style="padding:5px 9px;font-size:10px;color:#e74c3c">{s.get("error","")}</td></tr>'
        for i,s in enumerate(scan_log)
    )

    return f"""<!DOCTYPE html><html lang="vi"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NOXH HN – Quét chuyên sâu {ts}</title>
<style>*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:'Segoe UI',Arial,sans-serif;background:#f1f3f4;color:#3c4043}}.wrap{{max-width:1280px;margin:0 auto;padding:20px}}.card{{background:#fff;border-radius:10px;border:1px solid #e8eaed;overflow:hidden;margin-bottom:18px}}.card-h{{padding:12px 18px;border-bottom:1px solid #e8eaed;font-weight:600;font-size:14px;color:{NAV};display:flex;align-items:center;gap:8px}}.stat{{background:#fff;border-radius:8px;border:1px solid #e8eaed;padding:12px 16px;text-align:center}}table{{width:100%;border-collapse:collapse;font-size:12px}}th{{padding:7px 9px;text-align:left;font-size:11px;font-weight:500;color:#5f6368;border-bottom:1px solid #e8eaed;background:#f8f9fa;white-space:nowrap}}a{{text-decoration:none}}</style></head>
<body><div class="wrap">
<div style="background:{NAV};border-radius:10px;padding:16px 22px;margin-bottom:20px;display:flex;align-items:center;gap:14px;flex-wrap:wrap">
  <div style="width:38px;height:38px;background:{GOLD};border-radius:8px;display:flex;align-items:center;justify-content:center;flex-shrink:0">
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
  </div>
  <div style="flex:1">
    <div style="color:#fff;font-weight:700;font-size:17px">NOXH Monitor – Quét chuyên sâu Hà Nội</div>
    <div style="color:#8fa8c8;font-size:11px">{len(QUERIES)} query | {ts}</div>
  </div>
  <div style="color:#fff;text-align:right">
    <div style="font-size:24px;font-weight:700">{len(all_ps)}</div>
    <div style="font-size:11px;color:#8fa8c8">dự án tổng hợp</div>
  </div>
</div>
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:18px">
  <div class="stat"><div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Tổng dự án</div><div style="font-size:24px;font-weight:700;color:{NAV}">{len(all_ps)}</div></div>
  <div class="stat"><div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Đang/sắp nhận HS</div><div style="font-size:24px;font-weight:700;color:{GRN}">{sum(1 for p in all_ps if 'nhận' in (p.get('trang_thai') or '').lower() or 'sắp' in (p.get('trang_thai') or '').lower())}</div></div>
  <div class="stat"><div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Vừa khởi công</div><div style="font-size:24px;font-weight:700;color:{GOLD}">{sum(1 for p in all_ps if 'khởi' in (p.get('trang_thai') or '').lower())}</div></div>
  <div class="stat"><div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Số query chạy</div><div style="font-size:24px;font-weight:700;color:{NAV}">{len(QUERIES)}</div></div>
</div>
<div class="card">
  <div class="card-h">Chi tiết từng query ({len(scan_log)} query)</div>
  <div style="overflow-x:auto"><table><thead><tr><th>Query ID</th><th>Mô tả</th><th>Tìm được</th><th>Mới</th><th>Lỗi</th></tr></thead>
  <tbody>{scan_rows}</tbody></table></div>
</div>
<div class="card">
  <div class="card-h">Tất cả dự án NOXH Hà Nội ({len(all_ps)} dự án)</div>
  <div style="overflow-x:auto"><table><thead><tr><th>STT</th><th>Tỉnh</th><th>Tên dự án</th><th>Quận/Huyện</th><th>Chủ đầu tư</th><th>Căn</th><th>Giá/m²</th><th>Lịch nhận HS</th><th>Trạng thái</th><th>Nguồn</th></tr></thead>
  <tbody>{rows}</tbody></table></div>
</div>
<div style="text-align:center;color:#9aa0a6;font-size:11px;padding:10px 0">
  NOXH Monitor Deep Scan · {ts} · <a href="https://soxaydung.hanoi.gov.vn" target="_blank" style="color:{GOLD}">SXD HN</a>
</div>
</div></body></html>"""

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    log.info("=" * 60)
    log.info(f"NOXH DEEP SCAN – {ts}")
    log.info(f"Số query: {len(QUERIES)}")
    log.info("=" * 60)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        log.error("Thiếu ANTHROPIC_API_KEY")
        sys.exit(1)

    client  = anthropic.Anthropic(api_key=api_key)
    db      = load_db()
    all_collected = []
    scan_log = []

    # ── Chạy từng query ──────────────────────────────────────────────────────
    for i, q in enumerate(QUERIES):
        log.info(f"\n[{i+1}/{len(QUERIES)}] {q['id']}")
        results = run_query(client, q, delay=65)
        scan_log.append({
            "id": q["id"], "desc": q["desc"],
            "found": len(results), "new": 0,
        })
        all_collected.extend(results)
        log.info(f"  Tích luỹ: {len(all_collected)} bản ghi (trước dedup)")

    log.info("\n" + "=" * 60)
    log.info("Đang gộp và dedup toàn bộ kết quả...")

    # ── Dedup toàn bộ collected trước ────────────────────────────────────────
    seen = set()
    deduped_collected = []
    for p in all_collected:
        k = dedup_key(p)
        if k and k not in seen:
            seen.add(k)
            deduped_collected.append(p)

    log.info(f"Sau dedup collected: {len(deduped_collected)} dự án")

    # ── Merge vào DB ─────────────────────────────────────────────────────────
    existing = db.get("projects", [])
    merged, truly_new, updated_fields = merge(existing, deduped_collected)

    # Cập nhật số "mới" vào scan_log
    total_new = len(truly_new)
    for sl in scan_log:
        sl["new"] = "—"

    db["projects"] = merged
    db.setdefault("scans", []).append({
        "ts": ts,
        "type": "deep_scan",
        "queries": len(QUERIES),
        "found": len(all_collected),
        "deduped": len(deduped_collected),
        "new": total_new,
        "updated_fields": updated_fields,
        "total": len(merged),
    })
    db["scans"] = db["scans"][-100:]
    save_db(db)

    # Geocoding tự động cho các dự án thiếu tọa độ
    if geocode_projects(merged):
        save_db(db)

    sync_webapp(merged)

    log.info(f"\nKẾT QUẢ CUỐI:")
    log.info(f"  Tổng bản ghi thu thập : {len(all_collected)}")
    log.info(f"  Sau dedup             : {len(deduped_collected)}")
    log.info(f"  Thực sự mới vào DB    : {total_new}")
    log.info(f"  Trường bổ sung thêm   : {updated_fields}")
    log.info(f"  Tổng DB hiện tại      : {len(merged)}")

    # ── Tạo báo cáo HTML ─────────────────────────────────────────────────────
    ha_noi_ps = [p for p in merged if p.get("tinh_tp") == "Hà Nội"]
    html = build_report(ha_noi_ps, scan_log, ts)

    (DOCS / "index.html").write_text(html, encoding="utf-8")
    archive = f"deep_scan_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
    (DOCS / archive).write_text(html, encoding="utf-8")
    log.info(f"\nBáo cáo → docs/index.html + docs/{archive}")
    log.info("HOÀN THÀNH DEEP SCAN.")

if __name__ == "__main__":
    main()
