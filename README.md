# NOXH Monitor – GitHub Actions

Tự động quét nhà ở xã hội (NOXH) hàng ngày lúc 7:00 SA (Hà Nội).

## Cấu trúc
```
├── .github/workflows/noxh-scan.yml  ← Workflow tự động
├── scan.py                           ← Script quét chính
├── requirements.txt
├── data/projects.json                ← Database (tự tạo sau lần đầu)
└── docs/index.html                   ← Báo cáo HTML (GitHub Pages)
```

## Thiết lập (5 phút)

### Bước 1 – Fork/tạo repo
Fork repo này hoặc tạo repo mới, push toàn bộ code lên.

### Bước 2 – Thêm Secrets
`Settings → Secrets and variables → Actions → New repository secret`

| Secret | Giá trị |
|---|---|
| `ANTHROPIC_API_KEY` | `sk-ant-...` từ console.anthropic.com |
| `SMTP_USER` | Gmail của bạn: `abc@gmail.com` |
| `SMTP_PASS` | **App Password** Gmail (không phải mật khẩu thường) |
| `EMAIL_TO` | Email nhận: `abc@gmail.com` hoặc `a@x.com,b@y.com` |

**Lấy App Password Gmail:**
1. Bật 2FA tại myaccount.google.com
2. Security → App passwords → Create → Copy

### Bước 3 – Bật GitHub Pages
`Settings → Pages → Source: Deploy from branch → Branch: main → Folder: /docs → Save`

### Bước 4 – Chạy lần đầu
`Actions → NOXH Monitor → Run workflow → Run workflow`

### Bước 5 – Xem kết quả
- Báo cáo web: `https://YOUR_USERNAME.github.io/YOUR_REPO/`
- Email: nhận tự động khi có dự án mới
- Cron: tự chạy 07:00 sáng hàng ngày (UTC+7)

## Chạy thủ công
`Actions → NOXH Monitor – Quét hàng ngày → Run workflow`

Options:
- `Gửi email kể cả không có DA mới`: check để luôn nhận email
- `Dry run`: chạy thử không gửi email

## Chi phí
- GitHub Actions: **miễn phí** (private repo: 2000 phút/tháng, đủ dùng ~3 phút/lần)
- Claude API: **~$0.01–0.05/lần** quét
- Tổng: khoảng **$0.3–1.5/tháng**
