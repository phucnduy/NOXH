#!/usr/bin/env python3
"""
scan.py – NOXH Monitor
Quét toàn quốc (34 tỉnh thành theo địa giới hành chính mới từ 12/6/2025),
gửi email tóm tắt DA mới, lưu báo cáo HTML, đồng bộ sang web-app data.json.

Biến môi trường:
  ANTHROPIC_API_KEY   – bắt buộc
  SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS / EMAIL_TO
  PAGES_URL           – URL trang báo cáo HTML
  WEBAPP_DATA_PATH    – đường dẫn tới web-app/public/data.json (tuỳ chọn)
  FULL_SCAN           – "true" để quét tất cả 6 vùng trong 1 lần chạy (mặc định true)
  FORCE_EMAIL / DRY_RUN
"""
import os, sys, json, re, smtplib, logging, html, time, shutil
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import anthropic

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
    """Escape HTML entities để tránh XSS."""
    return html.escape(str(text)) if text and text != "—" else (text or "—")

def safe_url(url):
    """Chỉ cho phép http/https để tránh javascript: URI injection."""
    u = (url or "").strip()
    return u if u.startswith(("http://", "https://")) else ""

# ── 34 đơn vị hành chính cấp tỉnh mới (Nghị quyết 202/2025/QH15, 12/6/2025) ──
# 6 Thành phố trực thuộc TW + 28 Tỉnh

# Phân vùng địa lý → mỗi vùng là 1 lần gọi Claude API
SCAN_BATCHES = [
    (
        "Đồng bằng Bắc Bộ",
        ["Hà Nội", "Hải Phòng", "Bắc Ninh", "Hưng Yên", "Quảng Ninh"],
        [
            "soxaydung.hanoi.gov.vn", "sotnmt.haiphong.gov.vn",
            "sxd.bacninh.gov.vn", "sxd.hungyen.gov.vn", "sxd.quangninh.gov.vn",
        ],
    ),
    (
        "Trung du & Miền núi phía Bắc",
        ["Phú Thọ", "Thái Nguyên", "Lào Cai", "Sơn La", "Tuyên Quang",
         "Lạng Sơn", "Cao Bằng", "Lai Châu", "Điện Biên"],
        [
            "sxd.phutho.gov.vn", "sxd.thainguyen.gov.vn",
            "sxd.laocai.gov.vn", "sxd.sonla.gov.vn", "sxd.tuyenquang.gov.vn",
        ],
    ),
    (
        "Bắc Trung Bộ",
        ["Ninh Bình", "Thanh Hóa", "Nghệ An", "Hà Tĩnh",
         "Quảng Trị", "Thành phố Huế"],
        [
            "sxd.ninhbinh.gov.vn", "sxd.thanhhoa.gov.vn",
            "sxd.nghean.gov.vn", "sxd.hatinh.gov.vn",
            "sxd.quangtri.gov.vn", "sxd.thuathienhue.gov.vn",
        ],
    ),
    (
        "Nam Trung Bộ & Tây Nguyên",
        ["Đà Nẵng", "Quảng Ngãi", "Gia Lai", "Đắk Lắk",
         "Khánh Hòa", "Lâm Đồng"],
        [
            "soxaydung.danang.gov.vn", "sxd.quangngai.gov.vn",
            "sxd.gialai.gov.vn", "sxd.daklak.gov.vn",
            "sxd.khanhhoa.gov.vn", "sxd.lamdong.gov.vn",
        ],
    ),
    (
        "Đông Nam Bộ",
        ["TP. Hồ Chí Minh", "Đồng Nai", "Tây Ninh"],
        [
            "sxd.hochiminhcity.gov.vn", "sxd.dongnai.gov.vn",
            "sxd.tayninh.gov.vn",
        ],
    ),
    (
        "Đồng bằng sông Cửu Long",
        ["Đồng Tháp", "An Giang", "Vĩnh Long", "Cần Thơ", "Cà Mau"],
        [
            "sxd.dongthap.gov.vn", "sxd.angiang.gov.vn",
            "sxd.vinhlong.gov.vn", "sxd.cantho.gov.vn", "sxd.camau.gov.vn",
        ],
    ),
]

ALL_PROVINCES = [p for _, provinces, _ in SCAN_BATCHES for p in provinces]

# ── Bảng chuẩn hoá tên tỉnh cũ → tên mới ──────────────────────────────────────
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

FORCE_EMAIL  = cfg("FORCE_EMAIL", "false").lower() == "true"
DRY_RUN      = cfg("DRY_RUN",     "false").lower() == "true"
FULL_SCAN    = cfg("FULL_SCAN",   "true").lower()  == "true"
SMTP_HOST    = cfg("SMTP_HOST",   "localhost")
SMTP_PORT    = int(cfg("SMTP_PORT", "25"))
WEBAPP_DATA  = Path(cfg("WEBAPP_DATA_PATH", str(BASE / "../web-app/public/data.json")))

def normalize_province(raw: str) -> str:
    """Chuẩn hoá tên tỉnh/thành → tên mới theo địa giới 2025."""
    s = (raw or "").strip()
    # Thử khớp trực tiếp
    if s in PROVINCE_NORMALIZE:
        return PROVINCE_NORMALIZE[s]
    # Thử khớp không phân biệt hoa thường
    for k, v in PROVINCE_NORMALIZE.items():
        if k.lower() == s.lower():
            return v
    # Thử partial match (ví dụ "TP Hà Nội" → "Hà Nội")
    for k, v in PROVINCE_NORMALIZE.items():
        if k.lower() in s.lower() or s.lower() in k.lower():
            return v
    return s  # Giữ nguyên nếu không tìm thấy

# ─── Database ──────────────────────────────────────────────────────────────────
def load_db() -> dict:
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
            log.error("Không load được DB và backup, khởi tạo mới")
    return {"projects": [], "scans": [], "updated": "", "last_batch_index": 0}

def save_db(db: dict) -> None:
    db["updated"] = datetime.now().isoformat()
    if DB_FILE.exists():
        shutil.copy2(DB_FILE, DB_FILE.with_suffix(".json.bak"))
    DB_FILE.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

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

def dedup(existing: list, new_list: list) -> tuple[list, list]:
    def key(p: dict) -> str:
        return (p.get("ten_du_an", "") + p.get("tinh_tp", "")).lower().strip()
    ex_keys = {key(p) for p in existing}
    truly_new = [p for p in new_list if key(p) not in ex_keys]
    return existing + truly_new, truly_new

# ─── Prompt ────────────────────────────────────────────────────────────────────
def build_prompt(provinces: list[str], sources: list[str]) -> str:
    nm = (datetime.now().replace(day=1) + timedelta(days=32)).strftime("%m/%Y")
    prov_str = ", ".join(provinces)
    src_str  = ", ".join(sources[:6])  # Tối đa 6 nguồn chính thức
    lines = [
        f"Tim kiem thong tin moi nhat ve du an nha o xa hoi (NOXH) tai: {prov_str}",
        f"chuan bi khoi cong hoac mo ban (nhan ho so) thang {nm} hoac sap toi 2026.",
        f"Nguon chinh thuc uu tien: {src_str}",
        "Nguon bao: cafef.vn, vnexpress.net, tienphong.vn, baoxaydung.vn, laodong.vn, dantri.vn",
        "",
        "Luu y quan trong: ten tinh/thanh pho phai dung theo ten moi nhat 2025.",
        "Vi du: Bac Giang → Bac Ninh | Hai Duong → Hai Phong | Thi Binh → Hung Yen",
        "       Ha Nam/Nam Dinh → Ninh Binh | Quang Binh → Quang Tri | Quang Nam → Da Nang",
        "       Binh Dinh → Gia Lai | Phu Yen → Dak Lak | Binh Duong → TP. Ho Chi Minh",
        "",
        "Chi tra JSON array hop le, khong kem text khac:",
        '[{"ten_du_an":"","ten_thuong_mai":"","tinh_tp":"ten tinh moi 2025",',
        '"quan_huyen":"","vi_tri":"","chu_dau_tu":"","so_toa":"","tong_can":"",',
        '"can_ban":"","dien_tich_can":"","gia_ban_m2":"","gia_can_tu":"",',
        '"khoi_cong":"","nhan_ho_so_tu":"","nhan_ho_so_den":"","du_kien_ban_giao":"",',
        '"doi_tuong_uu_tien":"","dia_diem_nop_ho_so":"","website_chu_dau_tu":"",',
        '"trang_thai":"Dang nhan HS|Sap nhan HS|Vua khoi cong|Dang thi cong",',
        '"nguon":"ten bao + ngay","url_nguon":"","ghi_chu":""}]',
        "",
        "Chi du an co thong tin cu the. Neu khong co → tra: []",
    ]
    return "\n".join(lines)

# ─── API call ──────────────────────────────────────────────────────────────────
def run_batch(client: anthropic.Anthropic, batch_name: str,
              provinces: list[str], sources: list[str]) -> list[dict]:
    prompt_text = build_prompt(provinces, sources)
    log.info(f"  Vùng [{batch_name}]: {', '.join(provinces)}")

    last_err = None
    for attempt in range(3):
        if attempt > 0:
            wait = 30 * attempt
            log.info(f"  Retry {attempt}/2, chờ {wait}s...")
            time.sleep(wait)
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt_text}],
            )
            full = "".join(b.text for b in msg.content if b.type == "text")
            log.info(f"  Response: {len(full)} ký tự")
            for pat in [r'\[\s*\{[\s\S]*?\}\s*\]', r'\[\s*\]']:
                m = re.search(pat, full)
                if m:
                    try:
                        data = json.loads(m.group())
                        # Chuẩn hoá tên tỉnh trong kết quả
                        for p in data:
                            if p.get("tinh_tp"):
                                p["tinh_tp"] = normalize_province(p["tinh_tp"])
                        log.info(f"  Parse OK: {len(data)} dự án")
                        return data
                    except Exception:
                        pass
            log.warning(f"  Không parse được JSON từ vùng [{batch_name}]")
            return []
        except Exception as e:
            last_err = e
            log.error(f"  Scan lỗi attempt {attempt + 1}/3: {e}")
    if last_err:
        raise last_err
    return []

def run_scan(db: dict) -> tuple[list[dict], list[str]]:
    """Quét theo vùng, trả về (tất cả dự án mới gộp, danh sách vùng đã quét)."""
    api_key = cfg("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("Thiếu ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)

    if FULL_SCAN:
        batches_to_scan = list(range(len(SCAN_BATCHES)))
        log.info(f"FULL_SCAN=true → quét toàn bộ {len(SCAN_BATCHES)} vùng")
    else:
        # Rotate: quét 1 vùng mỗi ngày, xoay vòng
        idx = db.get("last_batch_index", 0) % len(SCAN_BATCHES)
        batches_to_scan = [idx]
        log.info(f"FULL_SCAN=false → quét vùng {idx}: {SCAN_BATCHES[idx][0]}")

    all_raw: list[dict] = []
    scanned_names: list[str] = []

    for i in batches_to_scan:
        name, provinces, sources = SCAN_BATCHES[i]
        try:
            results = run_batch(client, name, provinces, sources)
            all_raw.extend(results)
            scanned_names.append(name)
        except Exception as e:
            log.error(f"Vùng [{name}] thất bại: {e}")
        # Nghỉ giữa các batch để tránh rate-limit
        if len(batches_to_scan) > 1 and i != batches_to_scan[-1]:
            log.info("  Nghỉ 5s giữa các batch...")
            time.sleep(5)

    # Cập nhật index cho lần sau (dùng khi FULL_SCAN=false)
    last = db.get("last_batch_index", 0)
    db["last_batch_index"] = (last + len(batches_to_scan)) % len(SCAN_BATCHES)

    return all_raw, scanned_names

# ─── HTML Report ───────────────────────────────────────────────────────────────
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
    if "nhận" in t or "mở" in t:  return "green"
    if "sắp" in t or "dự kiến" in t: return "amber"
    if "khởi" in t: return "navy"
    return "gray"

def prov_badge_color(p: str) -> str:
    if p in ("Hà Nội", "TP. Hồ Chí Minh"): return "navy"
    if p in ("Hải Phòng", "Đà Nẵng", "Cần Thơ", "Thành phố Huế"): return "rose"
    if p in ("Bắc Ninh", "Hưng Yên", "Quảng Ninh"): return "green"
    return "amber"

def tbl_row(p, i):
    bg  = "#ffffff" if i % 2 == 0 else "#f8f9fa"
    ten = h(p.get("ten_du_an", "—"))
    tm  = h(p.get("ten_thuong_mai", ""))
    tm_html = (
        f'<div style="font-weight:400;color:#9aa0a6;font-size:10px;'
        f'margin-top:2px;white-space:normal">{tm}</div>'
    ) if tm and tm != "—" else ""

    hs = "—"
    if p.get("nhan_ho_so_tu"):
        hs = h(p["nhan_ho_so_tu"])
        if p.get("nhan_ho_so_den"):
            hs += " →<br>" + h(p["nhan_ho_so_den"])
    elif p.get("khoi_cong"):
        hs = "KC: " + h(p["khoi_cong"])

    src = h(p.get("nguon", "—"))
    url = safe_url(p.get("url_nguon", ""))
    src_html = (
        f'<a href="{url}" target="_blank" rel="noopener noreferrer" '
        f'style="color:{GOLD};text-decoration:none">{src[:40]}</a>'
    ) if url else src[:40]

    web = safe_url(p.get("website_chu_dau_tu", ""))
    web_html = (
        f'<a href="{web}" target="_blank" rel="noopener noreferrer" '
        f'style="color:{GOLD};text-decoration:none;font-size:11px">↗ Link</a>'
    ) if web else "—"

    prov = p.get("tinh_tp", "—")
    return (
        f'<tr style="background:{bg};border-bottom:1px solid #e8eaed">'
        f'<td style="padding:7px 8px;color:#9aa0a6;font-size:11px;text-align:center">{i+1}</td>'
        f'<td style="padding:7px 8px">{badge(h(prov), prov_badge_color(prov))}</td>'
        f'<td style="padding:7px 8px;font-weight:600;color:{NAV};font-size:12px">{ten}{tm_html}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368">{h(p.get("vi_tri","—"))}'
        f'<div style="color:#9aa0a6;font-size:10px">{h(p.get("quan_huyen",""))}</div></td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368">{h(p.get("chu_dau_tu","—"))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;text-align:center">{h(p.get("tong_can","—"))}</td>'
        f'<td style="padding:7px 8px;font-size:12px;font-weight:600;color:{GOLD};text-align:center">{h(p.get("gia_ban_m2","—"))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368;text-align:center">{h(p.get("gia_can_tu","—"))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#3c4043">{hs}</td>'
        f'<td style="padding:7px 8px">{badge(h(p.get("trang_thai","—")), status_color(p.get("trang_thai","")))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#9aa0a6">{src_html}</td>'
        f'<td style="padding:7px 8px;text-align:center">{web_html}</td>'
        f'</tr>'
    )

def build_html(new_ps, all_ps, db, ts, scanned_names):
    # Thống kê theo vùng
    region_stats = []
    for name, provinces, _ in SCAN_BATCHES:
        count = sum(1 for p in all_ps if p.get("tinh_tp") in provinces)
        region_stats.append((name, count))

    # Thống kê 6 tỉnh/thành lớn nhất
    from collections import Counter
    top_provinces = Counter(p.get("tinh_tp", "—") for p in all_ps).most_common(6)

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
        f'<td style="padding:5px 9px;font-size:11px;color:#5f6368">{h(s.get("vung","—"))}</td>'
        f'<td style="padding:5px 9px;font-size:10px;color:#e74c3c">{s.get("error","")[:60]}</td>'
        f'</tr>'
        for i, s in enumerate(reversed(scans))
    )

    new_rows = (
        "".join(tbl_row(p, i) for i, p in enumerate(new_ps))
        if new_ps else
        '<tr><td colspan="12" style="padding:28px;text-align:center;'
        'color:#9aa0a6;font-size:13px">Không có dự án mới trong lần quét này</td></tr>'
    )
    all_rows = "".join(tbl_row(p, i) for i, p in enumerate(all_ps))

    thead = (
        '<tr>'
        '<th style="width:34px">STT</th>'
        '<th style="width:90px">Tỉnh/TP</th>'
        '<th style="width:200px">Tên dự án</th>'
        '<th style="width:140px">Vị trí</th>'
        '<th style="width:140px">Chủ đầu tư</th>'
        '<th style="width:46px">Căn</th>'
        '<th style="width:90px">Giá/m²</th>'
        '<th style="width:90px">Giá từ</th>'
        '<th style="width:130px">Lịch nhận HS</th>'
        '<th style="width:110px">Trạng thái</th>'
        '<th style="width:130px">Nguồn</th>'
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

    scanned_html = " · ".join(
        f'<span class="region-tag">{h(n)}</span>' for n in scanned_names
    ) if scanned_names else "—"

    return f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NOXH Monitor – {ts}</title>
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
      <div style="color:#fff;font-weight:700;font-size:17px">NOXH Monitor – Toàn Quốc</div>
      <div style="color:#8fa8c8;font-size:11px;margin-top:3px">
        34 tỉnh/thành · Địa giới 2025 · {ts}
      </div>
      <div style="margin-top:6px">{scanned_html}</div>
    </div>
    <div style="color:#8fa8c8;font-size:12px;text-align:right">
      {"<span style='background:#1A6B3A;color:#fff;padding:3px 10px;border-radius:12px;font-weight:600'>" + str(len(new_ps)) + " DA mới</span>" if new_ps else "<span>Không có DA mới</span>"}
      <div style="font-size:11px;margin-top:3px">Tổng: {len(all_ps)} dự án</div>
    </div>
  </div>

  <!-- Stats top tỉnh -->
  <div class="g-stat" style="display:grid;grid-template-columns:repeat(6,1fr);
    gap:10px;margin-bottom:18px">
    {stat_cards}
  </div>

  <!-- Dự án mới -->
  <div class="card">
    <div class="card-h" style="background:{'#D4EDDA' if new_ps else '#f8f9fa'}">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
        stroke="{'#1A6B3A' if new_ps else '#9aa0a6'}" stroke-width="2">
        <circle cx="12" cy="12" r="10"/>
        <line x1="12" y1="8" x2="12" y2="16"/>
        <line x1="8" y1="12" x2="16" y2="12"/>
      </svg>
      <span style="color:{'#1A6B3A' if new_ps else '#5f6368'}">
        Dự án mới phát hiện lần này ({len(new_ps)})
      </span>
    </div>
    <div style="overflow-x:auto">
      <table><thead>{thead}</thead><tbody>{new_rows}</tbody></table>
    </div>
  </div>

  <!-- Tất cả dự án -->
  <div class="card">
    <div class="card-h">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
        stroke="{NAV}" stroke-width="2">
        <rect x="3" y="3" width="7" height="7"/>
        <rect x="14" y="3" width="7" height="7"/>
        <rect x="14" y="14" width="7" height="7"/>
        <rect x="3" y="14" width="7" height="7"/>
      </svg>
      Tất cả dự án đang theo dõi ({len(all_ps)}) — 34 tỉnh/thành phố
    </div>
    <div style="overflow-x:auto">
      <table><thead>{thead}</thead><tbody>{all_rows}</tbody></table>
    </div>
  </div>

  <!-- Lịch sử quét -->
  <div class="card">
    <div class="card-h">Lịch sử quét ({len(scans)} lần gần nhất)</div>
    <div style="overflow-x:auto">
      <table>
        <thead>
          <tr>
            <th style="width:140px">Thời gian</th>
            <th style="width:70px;text-align:center">DA mới</th>
            <th style="width:70px;text-align:center">Tổng DB</th>
            <th style="width:200px">Vùng quét</th>
            <th>Lỗi</th>
          </tr>
        </thead>
        <tbody>
          {scan_rows if scan_rows else
           '<tr><td colspan="5" style="padding:18px;text-align:center;'
           'color:#9aa0a6;font-size:12px">Chưa có</td></tr>'}
        </tbody>
      </table>
    </div>
  </div>

  <!-- Footer -->
  <div style="text-align:center;color:#9aa0a6;font-size:11px;padding:10px 0">
    NOXH Monitor · 34 tỉnh/thành · Địa giới hành chính 2025 (NQ 202/2025/QH15) · {ts}
  </div>

</div>
</body>
</html>"""

# ─── Email ─────────────────────────────────────────────────────────────────────
def send_email(new_ps, all_ps, ts, scanned_names):
    su    = cfg("SMTP_USER")
    sp    = cfg("SMTP_PASS")
    eto   = [e.strip() for e in cfg("EMAIL_TO").split(",") if e.strip()]
    pages = cfg("PAGES_URL")

    if not su:
        log.warning("Thiếu SMTP_USER"); return False
    if not eto:
        log.warning("Thiếu EMAIL_TO"); return False

    subj = (
        f"[NOXH] {len(new_ps)} dự án mới – {ts}"
        if new_ps else
        f"[NOXH] Không có dự án mới – {ts}"
    )

    if new_ps:
        cards = ""
        for p in new_ps:
            hs = h(p.get("nhan_ho_so_tu", "") or p.get("khoi_cong", "") or "—")
            if p.get("nhan_ho_so_den"):
                hs += " → " + h(p["nhan_ho_so_den"])
            gia = h(p.get("gia_ban_m2", ""))
            gia_tu = h(p.get("gia_can_tu", ""))
            gia_html = (
                f'<tr><td style="color:#9aa0a6;padding:3px 0;width:110px;font-size:12px">Giá</td>'
                f'<td style="font-weight:600;color:{GOLD};font-size:12px">'
                f'{gia}{"  |  Từ " + gia_tu if gia_tu and gia_tu != "—" else ""}</td></tr>'
            ) if (gia and gia != "—") or (gia_tu and gia_tu != "—") else ""

            trang_thai = h(p.get("trang_thai", ""))
            tt_html = (
                f'<tr><td style="color:#9aa0a6;padding:3px 0;font-size:12px">Trạng thái</td>'
                f'<td><span style="background:#D4EDDA;color:#1A6B3A;font-size:11px;'
                f'padding:2px 8px;border-radius:4px;font-weight:600">{trang_thai}</span></td></tr>'
            ) if trang_thai and trang_thai != "—" else ""

            nguon = h(p.get("nguon", ""))
            ng_html = (
                f'<tr><td style="color:#9aa0a6;padding:3px 0;font-size:12px">Nguồn</td>'
                f'<td style="font-size:11px;color:#9aa0a6">{nguon}</td></tr>'
            ) if nguon and nguon != "—" else ""

            ten_tm = (
                f'<div style="color:#9aa0a6;font-size:12px;margin-bottom:8px">'
                f'{h(p["ten_thuong_mai"])}</div>'
            ) if p.get("ten_thuong_mai") else ""

            prov = h(p.get("tinh_tp", "—"))

            cards += f"""
<div style="border:1px solid #e8eaed;border-radius:8px;padding:14px 16px;
  margin-bottom:14px;background:#fff">
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
    <span style="background:#E8EDF5;color:#1B3A6B;font-size:10px;font-weight:600;
      padding:2px 7px;border-radius:4px">{prov}</span>
    <span style="font-weight:700;color:{NAV};font-size:14px">{h(p.get("ten_du_an","—"))}</span>
  </div>
  {ten_tm}
  <table style="width:100%;border-collapse:collapse">
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;width:110px;font-size:12px">Vị trí</td>
      <td style="color:#3c4043;font-size:12px">
        {h(p.get("vi_tri","—"))}
        {" – " + h(p["quan_huyen"]) if p.get("quan_huyen") else ""}
      </td>
    </tr>
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px">Chủ đầu tư</td>
      <td style="color:#3c4043;font-size:12px">{h(p.get("chu_dau_tu","—"))}</td>
    </tr>
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px">Quy mô</td>
      <td style="color:#3c4043;font-size:12px">
        {h(p.get("tong_can","—"))} căn
        {" | " + h(p["dien_tich_can"]) if p.get("dien_tich_can") else ""}
      </td>
    </tr>
    {gia_html}
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px">Lịch nhận HS</td>
      <td style="font-weight:600;color:#1A6B3A;font-size:12px">{hs}</td>
    </tr>
    {tt_html}
    {ng_html}
  </table>
</div>"""

        body_content = (
            f'<p style="color:#3c4043;font-size:14px;margin-bottom:16px">'
            f'Tìm thấy <strong style="color:{NAV}">{len(new_ps)} dự án mới</strong>'
            f' trong lần quét hôm nay:</p>\n{cards}'
        )
    else:
        body_content = (
            '<p style="color:#5f6368;font-size:14px;text-align:center;padding:30px 0">'
            'Không tìm thấy dự án mới trong lần quét hôm nay.<br>'
            '<span style="font-size:12px;color:#9aa0a6">'
            'Hệ thống sẽ tiếp tục theo dõi và thông báo khi có thông tin mới.'
            '</span></p>'
        )

    btn_html = ""
    if pages:
        btn_html = (
            f'<div style="text-align:center;margin:24px 0 8px">'
            f'<a href="{safe_url(pages)}" style="background:{NAV};color:#fff;'
            f'text-decoration:none;padding:12px 32px;border-radius:7px;'
            f'font-weight:600;font-size:14px;display:inline-block">'
            f'Xem toàn bộ {len(all_ps)} dự án →</a></div>'
        )

    scanned_str = " · ".join(scanned_names) if scanned_names else "—"
    body_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;background:#f1f3f4;margin:0;padding:20px">
  <div style="max-width:600px;margin:0 auto">
    <div style="background:{NAV};border-radius:10px 10px 0 0;padding:16px 22px;
      display:flex;align-items:center;gap:12px">
      <div style="width:36px;height:36px;background:{GOLD};border-radius:7px;
        display:flex;align-items:center;justify-content:center;font-size:18px">🏠</div>
      <div>
        <div style="color:#fff;font-weight:700;font-size:16px">NOXH Monitor – Toàn Quốc</div>
        <div style="color:#8fa8c8;font-size:11px">Báo cáo tự động · {ts}</div>
        <div style="color:#8fa8c8;font-size:10px;margin-top:2px">Vùng quét: {scanned_str}</div>
      </div>
    </div>
    <div style="background:#fff;padding:22px;border-radius:0 0 10px 10px;
      border:1px solid #e8eaed;border-top:none">
      {body_content}
      {btn_html}
      <hr style="border:none;border-top:1px solid #e8eaed;margin:20px 0">
      <p style="color:#9aa0a6;font-size:11px;text-align:center;margin:0">
        NOXH Monitor · 34 tỉnh/thành phố · Địa giới hành chính 2025<br>
        Quét tự động 7:00 SA hàng ngày
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
        log.info(f"Email OK → {', '.join(eto)}")
        return True
    except Exception as e:
        log.error(f"Email lỗi: {e}")
        return False

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    log.info("=" * 60)
    log.info(f"NOXH Monitor – Toàn Quốc – {ts}")
    log.info(f"34 tỉnh/thành · FULL_SCAN={FULL_SCAN} · DRY_RUN={DRY_RUN}")
    log.info("=" * 60)

    db = load_db()

    # 1. Quét
    try:
        raw, scanned_names = run_scan(db)
    except Exception as e:
        log.error(f"Scan lỗi: {e}")
        db.setdefault("scans", []).append({
            "ts": ts, "found": 0, "new": 0,
            "total": len(db.get("projects", [])),
            "vung": "—", "error": str(e),
        })
        save_db(db)
        sys.exit(1)

    # 2. Merge vào DB
    all_ps, new_ps = dedup(db.get("projects", []), raw)
    db["projects"] = all_ps
    db.setdefault("scans", []).append({
        "ts": ts, "found": len(raw),
        "new": len(new_ps), "total": len(all_ps),
        "vung": " · ".join(scanned_names),
    })
    db["scans"] = db["scans"][-100:]
    save_db(db)
    log.info(f"raw={len(raw)} | new={len(new_ps)} | total={len(all_ps)}")

    # 3. Đồng bộ sang web-app
    sync_webapp(all_ps)

    # 4. Tạo HTML report
    report_html = build_html(new_ps, all_ps, db, ts, scanned_names)
    (DOCS / "index.html").write_text(report_html, encoding="utf-8")
    archive = f"report_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
    (DOCS / archive).write_text(report_html, encoding="utf-8")
    for old in sorted(DOCS.glob("report_*.html"), reverse=True)[30:]:
        old.unlink()
    log.info(f"HTML → docs/index.html + docs/{archive}")

    # 5. Gửi email
    if DRY_RUN:
        log.info("DRY_RUN → bỏ qua email")
    elif FORCE_EMAIL or new_ps:
        send_email(new_ps, all_ps, ts, scanned_names)
    else:
        log.info("Không có DA mới → bỏ qua email")

    log.info("Hoàn thành.")

if __name__ == "__main__":
    main()
