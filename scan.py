#!/usr/bin/env python3
"""
scan.py – NOXH Monitor cho GitHub Actions
Đọc config từ biến môi trường (Secrets), lưu kết quả vào repo.
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

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("noxh")

def cfg(k, default=""):
    return os.environ.get(k, default).strip()

REGIONS     = ["Hà Nội", "Bắc Ninh", "Hưng Yên"]
FORCE_EMAIL = cfg("FORCE_EMAIL","false").lower()=="true"
DRY_RUN     = cfg("DRY_RUN","false").lower()=="true"

def load_db():
    if DB_FILE.exists():
        try: return json.loads(DB_FILE.read_text("utf-8"))
        except: pass
    return {"projects":[],"scans":[],"updated":""}

def save_db(db):
    db["updated"] = datetime.now().isoformat()
    DB_FILE.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

def dedup(existing, new_list):
    def key(p): return (p.get("ten_du_an","")+p.get("tinh_tp","")).lower().strip()
    ex = {key(p) for p in existing}
    truly_new = [p for p in new_list if key(p) not in ex]
    return existing+truly_new, truly_new

def build_prompt():
    nm = (datetime.now().replace(day=1)+timedelta(days=32)).strftime("%m/%Y")
    return f"""Tìm thông tin mới nhất về dự án nhà ở xã hội (NOXH) tại {', '.join(REGIONS)} chuẩn bị khởi công hoặc mở bán tháng {nm} hoặc sắp tới 2026.
Nguồn: soxaydung.hanoi.gov.vn, sxd.bacninh.gov.vn, sxd.hungyen.gov.vn, cafef.vn, vnexpress.net, tienphong.vn, baoxaydung.vn, laodong.vn.
Chỉ trả JSON array hợp lệ (không text khác):
[{{"ten_du_an":"","ten_thuong_mai":"","tinh_tp":"Hà Nội|Bắc Ninh|Hưng Yên","quan_huyen":"","vi_tri":"","chu_dau_tu":"","so_toa":"","tong_can":"","can_ban":"","dien_tich_can":"","gia_ban_m2":"","gia_can_tu":"","khoi_cong":"","nhan_ho_so_tu":"","nhan_ho_so_den":"","du_kien_ban_giao":"","doi_tuong_uu_tien":"","dia_diem_nop_ho_so":"","website_chu_dau_tu":"","trang_thai":"Đang nhận HS|Sắp nhận HS|Vừa khởi công|Đang thi công","nguon":"tên báo + ngày","url_nguon":"","ghi_chu":""}}]
Chỉ dự án có thông tin cụ thể/chính xác. Không có thì trả: []"""

def run_scan():
    api_key = cfg("ANTHROPIC_API_KEY")
    if not api_key: raise ValueError("Thiếu ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    log.info("Gọi Claude API + web_search...")
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        tools=[{"type":"web_search_20250305","name":"web_search"}],
        messages=[{"role":"user","content":build_prompt()}])
    full = "".join(b.text for b in msg.content if b.type=="text")
    log.info(f"Response: {len(full)} ký tự")
    for pat in [r'\[\s*\{[\s\S]*?\}\s*\]', r'\[\s*\]']:
        m = re.search(pat, full)
        if m:
            try:
                data = json.loads(m.group())
                log.info(f"Parse OK: {len(data)} DA")
                return data
            except: pass
    log.warning("Không parse được JSON"); return []

NAV,GOLD,GRN="#0B2545","#C9932A","#1A6B3A"

def badge(text,color):
    p={"green":("#D4EDDA","#1A6B3A"),"amber":("#FEF3C7","#D97706"),"navy":("#E8EDF5","#1B3A6B"),"gray":("#F1F3F4","#5F6368"),"red":("#FDDEDE","#C0392B")}
    bg,fg=p.get(color,p["gray"]); return f'<span style="background:{bg};color:{fg};font-size:11px;font-weight:600;padding:2px 8px;border-radius:4px">{text}</span>'

def sc(ts):
    t=(ts or "").lower()
    if "nhận" in t or "mở" in t: return "green"
    if "sắp" in t or "dự kiến" in t: return "amber"
    if "khởi" in t: return "navy"
    return "gray"

def pc(p): return {"Hà Nội":"navy","Bắc Ninh":"green"}.get(p,"amber")

def row(p,i):
    bg="#fff" if i%2==0 else "#f8f9fa"
    hs=f"{p.get('nhan_ho_so_tu','')} → {p.get('nhan_ho_so_den','')}" if p.get('nhan_ho_so_tu') and p.get('nhan_ho_so_den') else p.get('nhan_ho_so_tu') or p.get('khoi_cong') or "—"
    sl=f'<a href="{p["url_nguon"]}" target="_blank" style="color:{GOLD}">{p.get("nguon","")[:45]}</a>' if p.get("url_nguon") else p.get("nguon","—")[:45]
    wl=f'<a href="{p["website_chu_dau_tu"]}" target="_blank" style="color:{GOLD}">Website</a>' if p.get("website_chu_dau_tu") else "—"
    return f"""<tr style="background:{bg};border-bottom:1px solid #e8eaed">
      <td style="padding:7px 9px;color:#9aa0a6;font-size:11px;text-align:center">{i+1}</td>
      <td style="padding:7px 9px">{badge(p.get('tinh_tp','—'),pc(p.get('tinh_tp','')))}</td>
      <td style="padding:7px 9px;font-weight:600;color:{NAV};font-size:12px;max-width:200px;line-height:1.35">{p.get('ten_du_an','—')}{'<br><small style="font-weight:400;color:#9aa0a6">'+p['ten_thuong_mai']+'</small>' if p.get('ten_thuong_mai') else ''}</td>
      <td style="padding:7px 9px;font-size:11px;color:#5f6368;max-width:140px">{p.get('vi_tri','—')}<br><small style="color:#9aa0a6">{p.get('quan_huyen','')}</small></td>
      <td style="padding:7px 9px;font-size:11px;color:#5f6368;max-width:140px">{p.get('chu_dau_tu','—')}</td>
      <td style="padding:7px 9px;font-size:11px;text-align:center">{p.get('tong_can','—')}</td>
      <td style="padding:7px 9px;font-size:11px;font-weight:600;color:{GOLD};white-space:nowrap">{p.get('gia_ban_m2','—')}</td>
      <td style="padding:7px 9px;font-size:11px;color:#5f6368;white-space:nowrap">{p.get('gia_can_tu','—')}</td>
      <td style="padding:7px 9px;font-size:11px;color:#5f6368;white-space:nowrap">{hs}</td>
      <td style="padding:7px 9px">{badge(p.get('trang_thai','—'),sc(p.get('trang_thai','')))}</td>
      <td style="padding:7px 9px;font-size:10px;color:#9aa0a6">{sl}</td>
      <td style="padding:7px 9px;font-size:11px;text-align:center">{wl}</td>
    </tr>"""

def build_html(new_ps,all_ps,db,ts):
    ha=[p for p in all_ps if p.get("tinh_tp")=="Hà Nội"]
    bn=[p for p in all_ps if p.get("tinh_tp")=="Bắc Ninh"]
    hy=[p for p in all_ps if p.get("tinh_tp")=="Hưng Yên"]
    scans=db.get("scans",[])[-20:]
    sr="".join(f'<tr style="background:{"#fff" if i%2==0 else "#f8f9fa"}"><td style="padding:5px 9px;font-size:11px;color:#5f6368">{s.get("ts","")[:16]}</td><td style="padding:5px 9px;text-align:center;font-size:12px;font-weight:600;color:{"#1A6B3A" if s.get("new",0)>0 else "#9aa0a6"}">{s.get("new",0)}</td><td style="padding:5px 9px;text-align:center;font-size:11px">{s.get("total",0)}</td><td style="padding:5px 9px;font-size:10px;color:#e74c3c">{s.get("error","")[:60]}</td></tr>' for i,s in enumerate(reversed(scans)))
    nr="".join(row(p,i) for i,p in enumerate(new_ps)) or '<tr><td colspan="12" style="padding:28px;text-align:center;color:#9aa0a6">Không có dự án mới</td></tr>'
    ar="".join(row(p,i) for i,p in enumerate(all_ps))
    th='<tr><th>STT</th><th>Tỉnh</th><th>Tên dự án</th><th>Vị trí</th><th>Chủ đầu tư</th><th>Căn</th><th>Giá/m²</th><th>Giá từ</th><th>Lịch nhận HS</th><th>Trạng thái</th><th>Nguồn</th><th>Web</th></tr>'
    return f"""<!DOCTYPE html><html lang="vi"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NOXH Monitor – {ts}</title>
<style>*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:'Segoe UI',Arial,sans-serif;background:#f1f3f4;color:#3c4043}}.wrap{{max-width:1280px;margin:0 auto;padding:20px}}.card{{background:#fff;border-radius:10px;border:1px solid #e8eaed;overflow:hidden;margin-bottom:18px}}.card-h{{padding:12px 18px;border-bottom:1px solid #e8eaed;font-weight:600;font-size:14px;color:{NAV};display:flex;align-items:center;gap:8px}}.stat{{background:#fff;border-radius:8px;border:1px solid #e8eaed;padding:12px 16px;text-align:center}}table{{width:100%;border-collapse:collapse;font-size:12px}}th{{padding:7px 9px;text-align:left;font-size:11px;font-weight:500;color:#5f6368;border-bottom:1px solid #e8eaed;background:#f8f9fa;white-space:nowrap}}a{{text-decoration:none}}@media(max-width:700px){{.g4{{grid-template-columns:1fr 1fr!important}}}}</style></head>
<body><div class="wrap">
<div style="background:{NAV};border-radius:10px;padding:16px 22px;margin-bottom:20px;display:flex;align-items:center;gap:14px;flex-wrap:wrap">
  <div style="width:38px;height:38px;background:{GOLD};border-radius:8px;display:flex;align-items:center;justify-content:center;flex-shrink:0"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg></div>
  <div style="flex:1"><div style="color:#fff;font-weight:700;font-size:17px">NOXH Monitor</div><div style="color:#8fa8c8;font-size:11px">Hà Nội · Bắc Ninh · Hưng Yên · {ts}</div></div>
  <div style="color:#8fa8c8;font-size:12px;text-align:right">{'<span style="background:#1A6B3A;color:#fff;padding:3px 10px;border-radius:12px;font-weight:600">'+str(len(new_ps))+' DA mới</span>' if new_ps else '<span>Không có DA mới</span>'}<div style="font-size:11px;margin-top:3px">Tổng: {len(all_ps)} dự án</div></div>
</div>
<div class="g4" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:18px">
  <div class="stat"><div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Hà Nội</div><div style="font-size:24px;font-weight:700;color:{NAV}">{len(ha)}</div></div>
  <div class="stat"><div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Bắc Ninh</div><div style="font-size:24px;font-weight:700;color:{GRN}">{len(bn)}</div></div>
  <div class="stat"><div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Hưng Yên</div><div style="font-size:24px;font-weight:700;color:#D97706">{len(hy)}</div></div>
  <div class="stat"><div style="font-size:10px;color:#9aa0a6;margin-bottom:3px">Tổng</div><div style="font-size:24px;font-weight:700;color:{NAV}">{len(all_ps)}</div></div>
</div>
<div class="card"><div class="card-h" style="background:{'#D4EDDA' if new_ps else '#f8f9fa'}"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="{'#1A6B3A' if new_ps else '#9aa0a6'}" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="16"/><line x1="8" y1="12" x2="16" y2="12"/></svg><span style="color:{'#1A6B3A' if new_ps else '#5f6368'}">Dự án mới ({len(new_ps)})</span></div><div style="overflow-x:auto"><table><thead>{th}</thead><tbody>{nr}</tbody></table></div></div>
<div class="card"><div class="card-h"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="{NAV}" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>Tất cả dự án ({len(all_ps)})</div><div style="overflow-x:auto"><table><thead>{th}</thead><tbody>{ar}</tbody></table></div></div>
<div class="card"><div class="card-h">Lịch sử quét</div><div style="overflow-x:auto"><table><thead><tr><th>Thời gian</th><th>DA mới</th><th>Tổng</th><th>Lỗi</th></tr></thead><tbody>{sr or '<tr><td colspan=4 style="padding:18px;text-align:center;color:#9aa0a6;font-size:12px">Chưa có</td></tr>'}</tbody></table></div></div>
<div style="text-align:center;color:#9aa0a6;font-size:11px;padding:10px 0">NOXH Monitor · GitHub Actions · {ts} · <a href="https://soxaydung.hanoi.gov.vn" target="_blank" style="color:{GOLD}">SXD HN</a> · <a href="https://sxd.bacninh.gov.vn" target="_blank" style="color:{GOLD}">SXD BN</a></div>
</div></body></html>"""

def send_email(new_ps,all_ps,html_body,ts):
    su=cfg("SMTP_USER"); sp=cfg("SMTP_PASS")
    eto=[e.strip() for e in cfg("EMAIL_TO").split(",") if e.strip()]
    if not su or not sp: log.warning("Thiếu SMTP credentials"); return False
    if not eto: log.warning("Thiếu EMAIL_TO"); return False
    subj=f"[NOXH] {len(new_ps)} dự án mới – {ts}" if new_ps else f"[NOXH] Báo cáo định kỳ – {ts}"
    msg=MIMEMultipart("alternative")
    msg["From"]=f"NOXH Monitor <{su}>"; msg["To"]=", ".join(eto); msg["Subject"]=subj
    msg.attach(MIMEText(html_body,"html","utf-8"))
    try:
        with smtplib.SMTP("smtp.gmail.com",587) as s:
            s.starttls(); s.login(su,sp); s.sendmail(su,eto,msg.as_string())
        log.info(f"Email OK → {', '.join(eto)}"); return True
    except Exception as e:
        log.error(f"Email lỗi: {e}"); return False

def main():
    ts=datetime.now().strftime("%d/%m/%Y %H:%M")
    log.info("="*55); log.info(f"NOXH Monitor – {ts}"); log.info("="*55)
    db=load_db()
    try: raw=run_scan()
    except Exception as e:
        log.error(f"Scan lỗi: {e}")
        db.setdefault("scans",[]).append({"ts":ts,"found":0,"new":0,"total":len(db.get("projects",[])),"error":str(e)})
        save_db(db); sys.exit(1)
    all_ps,new_ps=dedup(db.get("projects",[]),raw)
    db["projects"]=all_ps
    db.setdefault("scans",[]).append({"ts":ts,"found":len(raw),"new":len(new_ps),"total":len(all_ps)})
    db["scans"]=db["scans"][-100:]; save_db(db)
    log.info(f"raw={len(raw)} | new={len(new_ps)} | total={len(all_ps)}")
    html=build_html(new_ps,all_ps,db,ts)
    (DOCS/"index.html").write_text(html,encoding="utf-8")
    archive=f"report_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
    (DOCS/archive).write_text(html,encoding="utf-8")
    for old in sorted(DOCS.glob("report_*.html"),reverse=True)[30:]:
        old.unlink()
    log.info(f"HTML → docs/index.html + docs/{archive}")
    if DRY_RUN: log.info("DRY_RUN → bỏ qua email")
    elif FORCE_EMAIL or new_ps: send_email(new_ps,all_ps,html,ts)
    else: log.info("Không có DA mới → bỏ qua email")
    log.info("Hoàn thành.")

if __name__=="__main__":
    main()
