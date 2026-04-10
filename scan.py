#!/usr/bin/env python3
"""
scan.py – NOXH Monitor cho GitHub Actions
Quét hàng ngày, gửi email tóm tắt DA mới, lưu báo cáo HTML lên GitHub Pages.
"""
import os, sys, json, re, smtplib, logging
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

REGIONS     = ["Hà Nội", "Bắc Ninh", "Hưng Yên"]
FORCE_EMAIL = cfg("FORCE_EMAIL", "false").lower() == "true"
DRY_RUN     = cfg("DRY_RUN",     "false").lower() == "true"

# ─── Database ──────────────────────────────────────────────────────────────────
def load_db():
    if DB_FILE.exists():
        try:
            return json.loads(DB_FILE.read_text("utf-8"))
        except Exception:
            pass
    return {"projects": [], "scans": [], "updated": ""}

def save_db(db):
    db["updated"] = datetime.now().isoformat()
    DB_FILE.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

def dedup(existing, new_list):
    def key(p):
        return (p.get("ten_du_an", "") + p.get("tinh_tp", "")).lower().strip()
    ex_keys = {key(p) for p in existing}
    truly_new = [p for p in new_list if key(p) not in ex_keys]
    return existing + truly_new, truly_new

# ─── Prompt ────────────────────────────────────────────────────────────────────
def build_prompt():
    nm = (datetime.now().replace(day=1) + timedelta(days=32)).strftime("%m/%Y")
    regions = ", ".join(REGIONS)
    lines = [
        "Tim kiem thong tin moi nhat ve du an nha o xa hoi (NOXH) tai " + regions,
        "chuan bi khoi cong hoac mo ban (nhan ho so) thang " + nm + " hoac sap toi 2026.",
        "Nguon chinh thuc: soxaydung.hanoi.gov.vn, sxd.bacninh.gov.vn, sxd.hungyen.gov.vn",
        "Nguon bao: cafef.vn, vnexpress.net, tienphong.vn, baoxaydung.vn, laodong.vn",
        "",
        "Chi tra JSON array hop le, khong kem text khac:",
        '[{"ten_du_an":"","ten_thuong_mai":"","tinh_tp":"Ha Noi|Bac Ninh|Hung Yen",',
        '"quan_huyen":"","vi_tri":"","chu_dau_tu":"","so_toa":"","tong_can":"",',
        '"can_ban":"","dien_tich_can":"","gia_ban_m2":"","gia_can_tu":"",',
        '"khoi_cong":"","nhan_ho_so_tu":"","nhan_ho_so_den":"","du_kien_ban_giao":"",',
        '"doi_tuong_uu_tien":"","dia_diem_nop_ho_so":"","website_chu_dau_tu":"",',
        '"trang_thai":"Dang nhan HS|Sap nhan HS|Vua khoi cong|Dang thi cong",',
        '"nguon":"ten bao + ngay","url_nguon":"","ghi_chu":""}]',
        "",
        "Chi du an co thong tin cu the va chinh xac. Khong co thi tra: []",
    ]
    return "\n".join(lines)

# ─── API call ──────────────────────────────────────────────────────────────────
def run_scan():
    api_key = cfg("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("Thieu ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    log.info("Goi Claude API + web_search...")
    prompt_text = build_prompt()
    log.info(f"Prompt: {len(prompt_text)} chars")
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4000,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": prompt_text}],
    )
    full = "".join(b.text for b in msg.content if b.type == "text")
    log.info(f"Response: {len(full)} ky tu")
    for pat in [r'\[\s*\{[\s\S]*?\}\s*\]', r'\[\s*\]']:
        m = re.search(pat, full)
        if m:
            try:
                data = json.loads(m.group())
                log.info(f"Parse OK: {len(data)} DA")
                return data
            except Exception:
                pass
    log.warning("Khong parse duoc JSON")
    return []

# ─── HTML Report ───────────────────────────────────────────────────────────────
NAV, GOLD, GRN = "#0B2545", "#C9932A", "#1A6B3A"

def badge(text, color):
    pal = {
        "green": ("#D4EDDA", "#1A6B3A"),
        "amber": ("#FEF3C7", "#D97706"),
        "navy":  ("#E8EDF5", "#1B3A6B"),
        "gray":  ("#F1F3F4", "#5F6368"),
    }
    bg, fg = pal.get(color, pal["gray"])
    return (
        f'<span style="background:{bg};color:{fg};font-size:11px;'
        f'font-weight:600;padding:2px 7px;border-radius:4px;white-space:nowrap">'
        f'{text}</span>'
    )

def status_color(ts):
    t = (ts or "").lower()
    if "nhận" in t or "mở" in t: return "green"
    if "sắp" in t or "dự kiến" in t: return "amber"
    if "khởi" in t: return "navy"
    return "gray"

def prov_color(p):
    return {"Hà Nội": "navy", "Bắc Ninh": "green"}.get(p, "amber")

def tbl_row(p, i):
    bg  = "#ffffff" if i % 2 == 0 else "#f8f9fa"
    ten = p.get("ten_du_an", "—")
    tm  = p.get("ten_thuong_mai", "")
    tm_html = (
        f'<div style="font-weight:400;color:#9aa0a6;font-size:10px;'
        f'margin-top:2px;white-space:normal">{tm}</div>'
    ) if tm else ""

    hs = "—"
    if p.get("nhan_ho_so_tu"):
        hs = p["nhan_ho_so_tu"]
        if p.get("nhan_ho_so_den"):
            hs += " →<br>" + p["nhan_ho_so_den"]
    elif p.get("khoi_cong"):
        hs = "KC: " + p["khoi_cong"]

    src = p.get("nguon", "—")
    url = p.get("url_nguon", "")
    src_html = (
        f'<a href="{url}" target="_blank" '
        f'style="color:{GOLD};text-decoration:none">{src[:40]}</a>'
    ) if url else src[:40]

    web = p.get("website_chu_dau_tu", "")
    web_html = (
        f'<a href="{web}" target="_blank" '
        f'style="color:{GOLD};text-decoration:none;font-size:11px">↗ Link</a>'
    ) if web else "—"

    return (
        f'<tr style="background:{bg};border-bottom:1px solid #e8eaed">'
        f'<td style="padding:7px 8px;color:#9aa0a6;font-size:11px;text-align:center">{i+1}</td>'
        f'<td style="padding:7px 8px">{badge(p.get("tinh_tp","—"), prov_color(p.get("tinh_tp","")))}</td>'
        f'<td style="padding:7px 8px;font-weight:600;color:{NAV};font-size:12px">{ten}{tm_html}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368">{p.get("vi_tri","—")}'
        f'<div style="color:#9aa0a6;font-size:10px">{p.get("quan_huyen","")}</div></td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368">{p.get("chu_dau_tu","—")}</td>'
        f'<td style="padding:7px 8px;font-size:11px;text-align:center">{p.get("tong_can","—")}</td>'
        f'<td style="padding:7px 8px;font-size:12px;font-weight:600;color:{GOLD};text-align:center">{p.get("gia_ban_m2","—")}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#5f6368;text-align:center">{p.get("gia_can_tu","—")}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#3c4043">{hs}</td>'
        f'<td style="padding:7px 8px">{badge(p.get("trang_thai","—"), status_color(p.get("trang_thai","")))}</td>'
        f'<td style="padding:7px 8px;font-size:11px;color:#9aa0a6">{src_html}</td>'
        f'<td style="padding:7px 8px;text-align:center">{web_html}</td>'
        f'</tr>'
    )

def build_html(new_ps, all_ps, db, ts):
    ha  = [p for p in all_ps if p.get("tinh_tp") == "Hà Nội"]
    bn  = [p for p in all_ps if p.get("tinh_tp") == "Bắc Ninh"]
    hy  = [p for p in all_ps if p.get("tinh_tp") == "Hưng Yên"]

    scans = db.get("scans", [])[-20:]
    scan_rows = "".join(
        f'<tr style="background:{"#fff" if i%2==0 else "#f8f9fa"}">'
        f'<td style="padding:5px 9px;font-size:11px;color:#5f6368">{s.get("ts","")[:16]}</td>'
        f'<td style="padding:5px 9px;text-align:center;font-size:12px;font-weight:600;'
        f'color:{"#1A6B3A" if s.get("new",0)>0 else "#9aa0a6"}">{s.get("new",0)}</td>'
        f'<td style="padding:5px 9px;text-align:center;font-size:11px">{s.get("total",0)}</td>'
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

    # Header row với width cố định
    thead = (
        '<tr>'
        '<th style="width:34px">STT</th>'
        '<th style="width:72px">Tỉnh</th>'
        '<th style="width:200px">Tên dự án</th>'
        '<th style="width:140px">Vị trí</th>'
        '<th style="width:150px">Chủ đầu tư</th>'
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
        ".wrap {max-width:1280px;margin:0 auto;padding:20px}"
        ".card {background:#fff;border-radius:10px;border:1px solid #e8eaed;"
        "overflow:hidden;margin-bottom:18px}"
        ".card-h {padding:12px 18px;border-bottom:1px solid #e8eaed;font-weight:600;"
        "font-size:14px;color:" + NAV + ";display:flex;align-items:center;gap:8px}"
        ".stat {background:#fff;border-radius:8px;border:1px solid #e8eaed;"
        "padding:12px 16px;text-align:center}"
        "table {width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed}"
        "th {padding:7px 8px;text-align:left;font-size:11px;font-weight:500;"
        "color:#5f6368;border-bottom:1px solid #e8eaed;background:#f8f9fa;"
        "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
        "td {overflow:hidden;text-overflow:ellipsis;vertical-align:top;word-wrap:break-word}"
        "a {text-decoration:none}"
        "@media(max-width:700px){.g4{grid-template-columns:1fr 1fr!important}}"
    )

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
      <div style="color:#fff;font-weight:700;font-size:17px">NOXH Monitor</div>
      <div style="color:#8fa8c8;font-size:11px">
        Hà Nội · Bắc Ninh · Hưng Yên · {ts}
      </div>
    </div>
    <div style="color:#8fa8c8;font-size:12px;text-align:right">
      {"<span style='background:#1A6B3A;color:#fff;padding:3px 10px;border-radius:12px;font-weight:600'>" + str(len(new_ps)) + " DA mới</span>" if new_ps else "<span>Không có DA mới</span>"}
      <div style="font-size:11px;margin-top:3px">Tổng: {len(all_ps)} dự án</div>
    </div>
  </div>

  <!-- Stats -->
  <div class="g4" style="display:grid;grid-template-columns:repeat(4,1fr);
    gap:12px;margin-bottom:18px">
    <div class="stat">
      <div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Hà Nội</div>
      <div style="font-size:24px;font-weight:700;color:{NAV}">{len(ha)}</div>
    </div>
    <div class="stat">
      <div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Bắc Ninh</div>
      <div style="font-size:24px;font-weight:700;color:{GRN}">{len(bn)}</div>
    </div>
    <div class="stat">
      <div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Hưng Yên</div>
      <div style="font-size:24px;font-weight:700;color:#D97706">{len(hy)}</div>
    </div>
    <div class="stat">
      <div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Tổng</div>
      <div style="font-size:24px;font-weight:700;color:{NAV}">{len(all_ps)}</div>
    </div>
  </div>

  <!-- New projects -->
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

  <!-- All projects -->
  <div class="card">
    <div class="card-h">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none"
        stroke="{NAV}" stroke-width="2">
        <rect x="3" y="3" width="7" height="7"/>
        <rect x="14" y="3" width="7" height="7"/>
        <rect x="14" y="14" width="7" height="7"/>
        <rect x="3" y="14" width="7" height="7"/>
      </svg>
      Tất cả dự án đang theo dõi ({len(all_ps)})
    </div>
    <div style="overflow-x:auto">
      <table><thead>{thead}</thead><tbody>{all_rows}</tbody></table>
    </div>
  </div>

  <!-- Scan history -->
  <div class="card">
    <div class="card-h">Lịch sử quét ({len(scans)} lần gần nhất)</div>
    <div style="overflow-x:auto">
      <table>
        <thead>
          <tr>
            <th style="width:140px">Thời gian</th>
            <th style="width:80px;text-align:center">DA mới</th>
            <th style="width:80px;text-align:center">Tổng DB</th>
            <th>Lỗi</th>
          </tr>
        </thead>
        <tbody>
          {scan_rows if scan_rows else
           '<tr><td colspan="4" style="padding:18px;text-align:center;'
           'color:#9aa0a6;font-size:12px">Chưa có</td></tr>'}
        </tbody>
      </table>
    </div>
  </div>

  <!-- Footer -->
  <div style="text-align:center;color:#9aa0a6;font-size:11px;padding:10px 0">
    NOXH Monitor · GitHub Actions · {ts} ·
    <a href="https://soxaydung.hanoi.gov.vn" target="_blank"
      style="color:{GOLD}">SXD HN</a> ·
    <a href="https://sxd.bacninh.gov.vn" target="_blank"
      style="color:{GOLD}">SXD BN</a> ·
    <a href="https://sxd.hungyen.gov.vn" target="_blank"
      style="color:{GOLD}">SXD HY</a>
  </div>

</div>
</body>
</html>"""

# ─── Email (chỉ tóm tắt DA mới + link web) ────────────────────────────────────
def send_email(new_ps, all_ps, ts):
    su    = cfg("SMTP_USER")
    sp    = cfg("SMTP_PASS")
    eto   = [e.strip() for e in cfg("EMAIL_TO").split(",") if e.strip()]
    pages = cfg("PAGES_URL")

    if not su or not sp:
        log.warning("Thieu SMTP credentials"); return False
    if not eto:
        log.warning("Thieu EMAIL_TO"); return False

    subj = (
        f"[NOXH] {len(new_ps)} du an moi – {ts}"
        if new_ps else
        f"[NOXH] Khong co du an moi – {ts}"
    )

    # ── Card từng dự án mới ──────────────────────────────────────────────────
    if new_ps:
        cards = ""
        for p in new_ps:
            hs = p.get("nhan_ho_so_tu", "") or p.get("khoi_cong", "") or "—"
            if p.get("nhan_ho_so_den"):
                hs += " → " + p["nhan_ho_so_den"]

            gia = p.get("gia_ban_m2", "")
            gia_tu = p.get("gia_can_tu", "")
            gia_html = ""
            if gia or gia_tu:
                gia_html = (
                    f'<tr>'
                    f'<td style="color:#9aa0a6;padding:3px 0;width:110px;'
                    f'font-size:12px;vertical-align:top">Giá</td>'
                    f'<td style="font-weight:600;color:{GOLD};font-size:12px">'
                    f'{gia}{"  |  Từ " + gia_tu if gia_tu else ""}</td>'
                    f'</tr>'
                )

            trang_thai = p.get("trang_thai", "")
            tt_html = ""
            if trang_thai:
                tt_html = (
                    f'<tr>'
                    f'<td style="color:#9aa0a6;padding:3px 0;font-size:12px;'
                    f'vertical-align:top">Trạng thái</td>'
                    f'<td><span style="background:#D4EDDA;color:#1A6B3A;'
                    f'font-size:11px;padding:2px 8px;border-radius:4px;'
                    f'font-weight:600">{trang_thai}</span></td>'
                    f'</tr>'
                )

            nguon = p.get("nguon", "")
            ng_html = ""
            if nguon:
                ng_html = (
                    f'<tr>'
                    f'<td style="color:#9aa0a6;padding:3px 0;font-size:12px;'
                    f'vertical-align:top">Nguồn</td>'
                    f'<td style="font-size:11px;color:#9aa0a6">{nguon}</td>'
                    f'</tr>'
                )

            ten_tm = (
                f'<div style="color:#9aa0a6;font-size:12px;margin-bottom:8px">'
                f'{p["ten_thuong_mai"]}</div>'
            ) if p.get("ten_thuong_mai") else ""

            cards += f"""
<div style="border:1px solid #e8eaed;border-radius:8px;padding:14px 16px;
  margin-bottom:14px;background:#fff">
  <div style="font-weight:700;color:{NAV};font-size:14px;
    margin-bottom:4px">{p.get("ten_du_an","—")}</div>
  {ten_tm}
  <table style="width:100%;border-collapse:collapse">
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;width:110px;
        font-size:12px;vertical-align:top">Vị trí</td>
      <td style="color:#3c4043;font-size:12px">
        {p.get("vi_tri","—")}
        {" – " + p["quan_huyen"] if p.get("quan_huyen") else ""}
      </td>
    </tr>
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px;
        vertical-align:top">Chủ đầu tư</td>
      <td style="color:#3c4043;font-size:12px">{p.get("chu_dau_tu","—")}</td>
    </tr>
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px;
        vertical-align:top">Quy mô</td>
      <td style="color:#3c4043;font-size:12px">
        {p.get("tong_can","—")} căn
        {" | " + p["dien_tich_can"] if p.get("dien_tich_can") else ""}
      </td>
    </tr>
    {gia_html}
    <tr>
      <td style="color:#9aa0a6;padding:3px 0;font-size:12px;
        vertical-align:top">Lịch nhận HS</td>
      <td style="font-weight:600;color:#1A6B3A;font-size:12px">{hs}</td>
    </tr>
    {tt_html}
    {ng_html}
  </table>
</div>"""

        body_content = f"""
<p style="color:#3c4043;font-size:14px;margin-bottom:16px">
  Tìm thấy <strong style="color:{NAV}">{len(new_ps)} dự án mới</strong>
  trong lần quét hôm nay:
</p>
{cards}"""
    else:
        body_content = """
<p style="color:#5f6368;font-size:14px;text-align:center;padding:30px 0">
  Không tìm thấy dự án mới trong lần quét hôm nay.<br>
  <span style="font-size:12px;color:#9aa0a6">
    Hệ thống sẽ tiếp tục theo dõi và thông báo khi có thông tin mới.
  </span>
</p>"""

    # ── Nút xem tất cả ───────────────────────────────────────────────────────
    btn_html = ""
    if pages:
        btn_html = f"""
<div style="text-align:center;margin:24px 0 8px">
  <a href="{pages}"
    style="background:{NAV};color:#fff;text-decoration:none;
    padding:12px 32px;border-radius:7px;font-weight:600;
    font-size:14px;display:inline-block">
    Xem toàn bộ {len(all_ps)} dự án →
  </a>
</div>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:'Segoe UI',Arial,sans-serif;background:#f1f3f4;
  margin:0;padding:20px">
  <div style="max-width:600px;margin:0 auto">

    <!-- Header -->
    <div style="background:{NAV};border-radius:10px 10px 0 0;
      padding:16px 22px;display:flex;align-items:center;gap:12px">
      <div style="width:36px;height:36px;background:{GOLD};border-radius:7px;
        display:flex;align-items:center;justify-content:center;flex-shrink:0;
        font-size:18px">🏠</div>
      <div>
        <div style="color:#fff;font-weight:700;font-size:16px">
          NOXH Monitor
        </div>
        <div style="color:#8fa8c8;font-size:11px">
          Báo cáo tự động · {ts}
        </div>
      </div>
    </div>

    <!-- Body -->
    <div style="background:#fff;padding:22px;
      border-radius:0 0 10px 10px;
      border:1px solid #e8eaed;border-top:none">

      {body_content}
      {btn_html}

      <hr style="border:none;border-top:1px solid #e8eaed;margin:20px 0">
      <p style="color:#9aa0a6;font-size:11px;text-align:center;margin:0">
        NOXH Monitor · Quét tự động lúc 7:00 SA hàng ngày<br>
        Nguồn: SXD Hà Nội · SXD Bắc Ninh · SXD Hưng Yên
      </p>
    </div>

  </div>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["From"]    = f"NOXH Monitor <{su}>"
    msg["To"]      = ", ".join(eto)
    msg["Subject"] = subj
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(su, sp)
            s.sendmail(su, eto, msg.as_string())
        log.info(f"Email OK → {', '.join(eto)}")
        return True
    except Exception as e:
        log.error(f"Email loi: {e}")
        return False

# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    log.info("=" * 55)
    log.info(f"NOXH Monitor – {ts}")
    log.info(f"Khu vuc: {', '.join(REGIONS)}")
    log.info(f"DRY_RUN={DRY_RUN} | FORCE_EMAIL={FORCE_EMAIL}")
    log.info("=" * 55)

    db = load_db()

    # 1. Quét
    try:
        raw = run_scan()
    except Exception as e:
        log.error(f"Scan loi: {e}")
        db.setdefault("scans", []).append({
            "ts": ts, "found": 0, "new": 0,
            "total": len(db.get("projects", [])),
            "error": str(e),
        })
        save_db(db)
        sys.exit(1)

    # 2. Merge vào DB
    all_ps, new_ps = dedup(db.get("projects", []), raw)
    db["projects"] = all_ps
    db.setdefault("scans", []).append({
        "ts": ts, "found": len(raw),
        "new": len(new_ps), "total": len(all_ps),
    })
    db["scans"] = db["scans"][-100:]
    save_db(db)
    log.info(f"raw={len(raw)} | new={len(new_ps)} | total={len(all_ps)}")

    # 3. Tạo HTML report
    html = build_html(new_ps, all_ps, db, ts)
    (DOCS / "index.html").write_text(html, encoding="utf-8")
    archive = f"report_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
    (DOCS / archive).write_text(html, encoding="utf-8")
    for old in sorted(DOCS.glob("report_*.html"), reverse=True)[30:]:
        old.unlink()
    log.info(f"HTML → docs/index.html + docs/{archive}")

    # 4. Gửi email
    if DRY_RUN:
        log.info("DRY_RUN → bo qua email")
    elif FORCE_EMAIL or new_ps:
        send_email(new_ps, all_ps, ts)
    else:
        log.info("Khong co DA moi → bo qua email")

    log.info("Hoan thanh.")

if __name__ == "__main__":
    main()
