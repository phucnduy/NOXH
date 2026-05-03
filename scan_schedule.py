#!/usr/bin/env python3
"""
scan_schedule.py - Quet lai lich nhan ho so va trang thai cho tat ca du an.

- Nhom du an theo tinh/thanh
- Moi tinh: search web + DeepSeek tra ve cap nhat lich + trang thai
- Luu y: phan biet "Du kien" vs "Thuc te", phat hien thong bao lui han
"""
import os, sys, json, re, time, logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).parent
sys.path.insert(0, str(BASE))

import scan  # dung lai deepseek_chat, fetch_url_content, search_web, save_db, sync_webapp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("sched")

TODAY = datetime.today().strftime("%d/%m/%Y")

SYSTEM_PROMPT = """Ban la chuyen gia theo doi thi truong nha o xa hoi Viet Nam.
Nhiem vu: cap nhat LICH NHAN HO SO va TRANG THAI chinh xac cho tung du an.

Quy tac quan trong:
- "loai_lich": "Thuc te" neu da co thong bao chinh thuc, "Du kien" neu chua chinh thuc
- "lui_han": true neu co thong bao lui/gia han so voi lich cu
- "ghi_chu_lich": mo ta ngan neu co cap nhat dang chu y (lui han, mo them dot, v.v.)
- Trang thai chuan: "Sap nhan HS" / "Dang nhan HS" / "Da nhan HS" / "Dang xet duyet" /
  "Dang ki HD" / "Da ban giao" / "Dang thi cong" / "Ke hoach" / "Da ban het"
- Ngay viet dang dd/mm/yyyy hoac "Thang X/YYYY" hoac "QX/YYYY"
- Neu khong tim thay thong tin moi thi de null (KHONG doan)

Tra ve JSON array, moi phan tu la 1 du an duoc cap nhat:
[{"ten_du_an": "...", "trang_thai": "...", "nhan_ho_so_tu": "...", "nhan_ho_so_den": "...",
  "loai_lich": "Du kien|Thuc te", "lui_han": false, "ghi_chu_lich": "..."}]
Chi tra ve du an co thong tin MOI hoac THAY DOI so voi du lieu cu. Bo qua neu khong co gi moi."""


def build_prompt(province: str, projects: list, web_content: str) -> str:
    proj_list = "\n".join(
        "[%d] %s | HS tu: %s | HS den: %s | TT: %s" % (
            i + 1,
            p.get("ten_du_an", "?")[:70],
            p.get("nhan_ho_so_tu") or "?",
            p.get("nhan_ho_so_den") or "?",
            p.get("trang_thai") or "?",
        )
        for i, p in enumerate(projects)
    )

    content_section = (
        "\n\nNOI DUNG TIM KIEM TREN WEB:\n" + web_content
        if web_content
        else ""
    )

    return (
        "Tinh/Thanh pho: %s\n"
        "Ngay hom nay: %s\n\n"
        "DANH SACH DU AN CAN CAP NHAT:\n%s"
        "%s\n\n"
        "Hay kiem tra tung du an va tra ve JSON voi nhung du an co thong tin moi/thay doi."
        % (province, TODAY, proj_list, content_section)
    )


def fetch_province_news(province: str, project_names: list) -> str:
    """Thu thap tin tuc lich nhan ho so cho mot tinh."""
    # Tim kiem chung cho tinh
    queries = [
        "nha o xa hoi %s lich nhan ho so 2026" % province,
        "nha o xa hoi %s mo ban thong bao 2026" % province,
    ]
    # Them query cho du an cu the neu co ten ngan
    short_names = [
        n for n in project_names
        if len(n) < 40 and any(w in n.lower() for w in ["noxh","nha o xa hoi","chung cu"])
    ][:3]
    for name in short_names:
        queries.append("%s lich nhan ho so" % name)

    parts = []
    for q in queries[:4]:
        content = scan.search_web(q, max_chars=3000)
        if content and len(content) > 100:
            parts.append("[Search: %s]\n%s" % (q, content))
        time.sleep(0.5)

    return "\n\n".join(parts)[:12000]


def run_province(province: str, projects: list) -> list[dict]:
    """Quet 1 tinh, tra ve danh sach du an duoc cap nhat."""
    names = [p.get("ten_du_an", "") for p in projects]
    log.info("[%s] %d du an - dang tim kiem..." % (province, len(projects)))

    web_content = fetch_province_news(province, names)
    log.info("  Web content: %d ky tu" % len(web_content))

    prompt = build_prompt(province, projects, web_content)

    for attempt in range(3):
        if attempt > 0:
            log.info("  Retry %d/2..." % attempt)
            time.sleep(20 * attempt)
        try:
            response = scan.deepseek_chat(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=4096,
                temperature=0.1,
            )
            log.info("  Response: %d ky tu" % len(response))
            updates = scan._parse_json_response(response, province)
            if updates is not None:
                log.info("  Cap nhat: %d du an" % len(updates))
                return updates
        except Exception as e:
            log.error("  Loi attempt %d: %s" % (attempt + 1, e))

    return []


STATUS_NORMALIZE = {
    # ASCII -> Unicode
    "Ke hoach": "Kế hoạch", "ke hoach": "Kế hoạch",
    "Dang nhan HS": "Đang nhận HS", "dang nhan hs": "Đang nhận HS",
    "Sap nhan HS": "Sắp nhận HS", "sap nhan hs": "Sắp nhận HS",
    "Da nhan HS": "Đã nhận HS", "da nhan hs": "Đã nhận HS",
    "Da ban het": "Đã bán hết", "da ban het": "Đã bán hết",
    "Da ban": "Đã bán", "da ban": "Đã bán",
    "Dang thi cong": "Đang thi công", "dang thi cong": "Đang thi công",
    "Dang xay dung": "Đang xây dựng", "dang xay dung": "Đang xây dựng",
    "Dang trien khai": "Đang triển khai", "dang trien khai": "Đang triển khai",
    "Vua khoi cong": "Vừa khởi công", "vua khoi cong": "Vừa khởi công",
    "Dang mo ban": "Đang mở bán", "dang mo ban": "Đang mở bán",
    "Chua mo ban": "Chưa mở bán", "chua mo ban": "Chưa mở bán",
    "Da hoan thanh": "Đã hoàn thành", "da hoan thanh": "Đã hoàn thành",
    "Da ban giao": "Đã bàn giao", "da ban giao": "Đã bàn giao",
    "Dang xet duyet": "Đang xét duyệt", "dang xet duyet": "Đang xét duyệt",
    "Dang ki HD": "Đang ký HĐ", "dang ki hd": "Đang ký HĐ",
    # Alias normalization
    "Đã/sắp hoàn thành": "Đã hoàn thành",
    "Chua mo ban": "Chưa mở bán",
    "Chưa mở bán": "Kế hoạch",   # alias -> chuan hoa
}

VALID_STATUSES = {
    "Kế hoạch", "Sắp nhận HS", "Đang nhận HS", "Đã nhận HS",
    "Đang xét duyệt", "Đang ký HĐ", "Đã ký HĐ",
    "Đang mở bán", "Đã bán hết", "Đã bán",
    "Vừa khởi công", "Đang thi công", "Đang xây dựng", "Đang triển khai",
    "Sau KCông – chờ thông báo HS", "Đã hoàn thành", "Đã bàn giao",
    "Thông tin chưa đầy đủ", "Chờ công bố chính thức",
}


def normalize_status(val: str) -> str:
    """Chuyen ASCII -> Unicode, validate theo danh sach chuan."""
    v = str(val or "").strip()
    # Tra trong bang map
    mapped = STATUS_NORMALIZE.get(v) or STATUS_NORMALIZE.get(v.lower())
    if mapped:
        return mapped
    # Neu trong tap hop hop le thi giu nguyen
    if v in VALID_STATUSES:
        return v
    # Neu co tieng Viet hop le nhung khong trong tap -> giu nguyen (DeepSeek biet ro)
    if any(ord(c) > 127 for c in v):
        return v
    # ASCII khong map duoc - canh bao va giu cu
    log.warning("  Trang thai la ma ASCII khong xac dinh: '%s'" % v)
    return v


def apply_updates(projects: list, updates: list) -> tuple[int, list[str]]:
    """Ap dung cap nhat vao danh sach du an, tra ve (so luong thay doi, danh sach log)."""
    changed = 0
    logs = []
    name_map = {p.get("ten_du_an", "").lower().strip(): p for p in projects}

    for upd in updates:
        upd_name = (upd.get("ten_du_an") or "").lower().strip()
        if not upd_name:
            continue

        # Tim du an khop ten (exact hoac substring)
        target = name_map.get(upd_name)
        if not target:
            for name, p in name_map.items():
                if upd_name in name or name in upd_name:
                    target = p
                    break

        if not target:
            log.warning("  Khong khop: '%s'" % upd.get("ten_du_an", "")[:50])
            continue

        row_changed = False
        lines = []

        # trang_thai: normalize truoc khi apply
        new_tt = upd.get("trang_thai")
        if new_tt and str(new_tt).strip() not in ("", "null", "None", "?"):
            new_tt = normalize_status(new_tt)
            old_tt = target.get("trang_thai", "")
            if new_tt != old_tt:
                lines.append("    trang_thai: '%s' -> '%s'" % (old_tt, new_tt))
                target["trang_thai"] = new_tt
                row_changed = True

        # Schedule fields
        for field in ["nhan_ho_so_tu", "nhan_ho_so_den"]:
            new_val = upd.get(field)
            if new_val and str(new_val).strip() not in ("", "null", "None", "?", "Không rõ", "Chưa xác định"):
                old_val = target.get(field, "")
                new_val = str(new_val).strip()
                if new_val != str(old_val).strip():
                    lines.append("    %s: '%s' -> '%s'" % (field, old_val, new_val))
                    target[field] = new_val
                    row_changed = True

        # Extra fields
        for field in ["loai_lich", "ghi_chu_lich"]:
            val = upd.get(field)
            if val and str(val).strip() not in ("", "null", "None"):
                target[field] = str(val).strip()

        if upd.get("lui_han") is True:
            target["lui_han"] = True
            lines.append("    *** LUI HAN ***")

        if row_changed:
            changed += 1
            logs.append("  + %s" % target.get("ten_du_an", "")[:55])
            logs.extend(lines)

    return changed, logs


def main():
    api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        log.error("Thieu DEEPSEEK_API_KEY")
        sys.exit(1)

    data = json.load(open(scan.DB_FILE))
    projects = data["projects"]
    log.info("Bat dau scan lich nhan ho so: %d du an | %s" % (len(projects), TODAY))

    # Nhom theo tinh
    by_province = defaultdict(list)
    for p in projects:
        prov = p.get("tinh_tp") or "Khac"
        by_province[prov].append(p)

    log.info("Nhom: %d tinh/thanh" % len(by_province))

    total_changed = 0
    provinces = sorted(by_province.keys())

    for i, province in enumerate(provinces):
        group = by_province[province]
        updates = run_province(province, group)

        if updates:
            n, logs = apply_updates(projects, updates)
            total_changed += n
            if n > 0:
                log.info("  [%s] Thay doi %d du an:" % (province, n))
                for line in logs:
                    log.info(line)
            else:
                log.info("  [%s] Khong co thay doi" % province)
        else:
            log.info("  [%s] Khong co thong tin moi" % province)

        # Luu intermediate de tranh mat data
        if (i + 1) % 5 == 0 or i == len(provinces) - 1:
            data["projects"] = projects
            scan.save_db(data)
            log.info("  --- Checkpoint saved (%d/%d tinh) ---" % (i + 1, len(provinces)))

        # Rate limit
        if i < len(provinces) - 1:
            time.sleep(8)

    # Final save + sync
    data["projects"] = projects
    scan.save_db(data)
    scan.sync_webapp(projects)
    log.info("Hoan thanh. Tong thay doi: %d du an" % total_changed)

    # Rebuild report
    from datetime import datetime as dt
    ts = dt.now().strftime("%Y%m%d_%H%M")
    html_content = scan.build_html([], projects, data, ts, [])
    (scan.DOCS / "index.html").write_text(html_content, encoding="utf-8")
    (scan.DOCS / ("report_%s.html" % ts)).write_text(html_content, encoding="utf-8")
    log.info("Report rebuilt: docs/report_%s.html" % ts)


if __name__ == "__main__":
    main()
