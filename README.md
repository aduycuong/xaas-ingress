# xaas-ingress

Worker cửa vào của XaaS Workflows: mọi cách khởi động một run từ bên ngoài đều đi qua đây. Ingress tạo dòng `runs` trong database rồi trigger Upstash Workflow để runner chạy. Kiến trúc: `xaas-workflows/docs/kien-truc-upstash-cloudflare-hono.md`.

| Route | Ai gọi | Xác thực |
| --- | --- | --- |
| `POST /wh/<token>` | Hệ thống bên ngoài, cho trigger loại `webhook` | Token trong URL |
| `POST /internal/triggers/<id>/fire` | QStash Schedule, cho trigger loại `cron` | Chữ ký Upstash (signing keys) |
| `POST /runs` | App `xaas-workflows`, khi người dùng bấm Run | Bearer `INGRESS_API_TOKEN` |

Body của `POST /runs` có thể kèm `test` (`{}` hoặc `{ nodeId, outputs }`) cùng `workflowVersionId` của bản **nháp**: đó là run thử từ editor (`env` luôn là `staging`, workflow không cần đã publish, không vào Runs, không phát sự kiện).
| `GET /health` | Uptime monitor | Không |

Toàn bộ code nằm trong `src/index.ts`.

## Deploy lên Cloudflare

Thứ tự giữa các Worker: **node → runner → ingress**. Ingress cần URL của runner, nên deploy runner trước (xem `../runner/README.md`).

### Lần đầu

1. Cài dependency và đăng nhập Cloudflare (một lần cho mỗi máy):

```bash
npm install
```

```bash
npx wrangler login
```

2. Khai báo URL của runner. Thêm vào `wrangler.jsonc`, không có dấu `/` ở cuối:

```jsonc
"vars": { "RUNNER_URL": "https://xaas-runner.<subdomain>.workers.dev" }
```

3. Đặt secret. Mỗi lệnh sẽ hỏi giá trị; nếu Worker chưa tồn tại, wrangler hỏi có tạo không, chọn có.

```bash
npx wrangler secret put DATABASE_URL
```

```bash
npx wrangler secret put QSTASH_TOKEN
```

```bash
npx wrangler secret put QSTASH_CURRENT_SIGNING_KEY
```

```bash
npx wrangler secret put QSTASH_NEXT_SIGNING_KEY
```

```bash
npx wrangler secret put INGRESS_API_TOKEN
```

| Secret | Lấy ở đâu |
| --- | --- |
| `DATABASE_URL` | Connection string của PlanetScale Postgres, cùng database với app và runner; giữ `?sslmode=verify-full`, bỏ `sslrootcert` |
| `QSTASH_TOKEN`, `QSTASH_CURRENT_SIGNING_KEY`, `QSTASH_NEXT_SIGNING_KEY` | Upstash console, mục QStash; cùng tài khoản QStash với runner |
| `INGRESS_API_TOKEN` | Tự sinh một chuỗi ngẫu nhiên dài; đặt cùng giá trị vào `.env` của app |
| `CALLBACK_SIGNING_KEY` | Cùng giá trị với runner: kiểm `callback_url` của node Async (`POST /callbacks/:nodeRunId/:attempt`) |
| `QSTASH_URL` (tùy chọn) | Chỉ khi QStash của bạn không ở endpoint mặc định (theo vùng) |

4. Deploy:

```bash
npm run deploy
```

Wrangler in ra URL của Worker, dạng `https://xaas-ingress.<subdomain>.workers.dev`. Đây là giá trị của `INGRESS_URL`.

5. Nối với app. Trong `.env` của `xaas-workflows`:

- `INGRESS_URL`: URL vừa có. App dùng nó làm đích cho QStash Schedule của các trigger `cron`, và để gọi `POST /runs`.
- `INGRESS_API_TOKEN`: cùng giá trị với secret ở bước 3.

Đổi `INGRESS_URL` sau này thì các schedule đã tạo vẫn trỏ về URL cũ; phải đồng bộ lại từng trigger `cron` (`syncTriggerSchedule` trong app).

### Kiểm tra sau khi deploy

```bash
curl https://xaas-ingress.<subdomain>.workers.dev/health
```

Kết quả mong đợi: `{"ok":true}`.

Chạy thử một workflow qua webhook (token lấy từ cột `webhook_token` của bảng `triggers`, hoặc từ output của script seed):

```bash
curl -X POST https://xaas-ingress.<subdomain>.workers.dev/wh/<token> -H "Content-Type: application/json" -d "{\"text\":\"hello\",\"again\":true}"
```

Kết quả mong đợi: `202` kèm `runId`. Sau vài giây, dòng tương ứng trong bảng `runs` chuyển sang `succeeded`.

| Trả lời | Nghĩa là |
| --- | --- |
| `404 not_found` | Không có trigger `webhook` nào dùng token đó |
| `409 skipped` | Trigger đang tắt, hoặc workflow chưa `active` / chưa có version đã publish |
| `500 could_not_start_run` | Không ghi được database hoặc không trigger được Upstash; xem log. Thường do thiếu secret hoặc sai `RUNNER_URL` |
| `503 api_token_not_configured` (ở `/runs`) | Chưa đặt secret `INGRESS_API_TOKEN` |

Nếu run được tạo nhưng đứng mãi ở `queued`, Upstash không gọi được runner: kiểm tra `RUNNER_URL` và signing keys của runner.

### Các lần sau

```bash
npm run typecheck
```

```bash
npm run deploy
```

- Deploy không gây gián đoạn và không ảnh hưởng tới run đang chạy: sau khi trigger, run chỉ còn đi giữa Upstash và runner.
- Secret giữ nguyên qua các lần deploy; chỉ đặt lại khi đổi giá trị.
- Khi runner có route mới (ví dụ `/run/v5`), đổi hằng `RUNNER_ROUTE` trong `src/runs.ts` rồi deploy lại; run mới sẽ đi vào route mới.

### Quay lại bản trước và xem log

```bash
npx wrangler rollback
```

```bash
npx wrangler tail
```

Log cũng xem được trên dashboard Cloudflare (Workers → xaas-ingress → Observability).

### Tên miền riêng (tùy chọn)

URL webhook là thứ người dùng dán vào hệ thống khác, nên nên dùng tên miền ổn định. Thêm vào `wrangler.jsonc` rồi deploy lại, sau đó cập nhật `INGRESS_URL` trong app:

```jsonc
"routes": [{ "pattern": "hooks.example.com", "custom_domain": true }]
```

## Giới hạn hiện tại

- Webhook chỉ xác thực bằng token trong URL; chưa kiểm tra chữ ký của bên gửi, chưa có rate limit.
- Body phải là JSON object, tối đa 256 KB, và được dùng nguyên làm input của run.
- Chưa có chính sách concurrency của trigger (Skip / Queue / Parallel).

## Chạy ở local

Xem `../README.md`. Tóm tắt: chép `.dev.vars.example` thành `.dev.vars`, điền giá trị, đặt `RUNNER_URL` bằng URL công khai của runner (URL của tunnel khi runner chạy ở local), rồi:

```bash
npm run dev
```

Ingress nghe ở cổng 8788.

## Triggers

| Ruta | Dùng cho | Xác thực |
| --- | --- | --- |
| `POST /wh/:token` | Trigger `webhook` và `api` | Token trong URL, cộng cấu hình của trigger: `hmac` (header `X-Xaas-Signature: sha256=<hex>` của body thô, hoặc của `<timestamp>.<body thô>` khi có `X-Xaas-Timestamp`, lệch tối đa 5 phút) hoặc `bearer` |
| `GET /wh/:token/runs/:runId` | Trạng thái và kết quả một run của trigger `api` (`?wait=0..30` giữ câu trả lời tới khi run xong) | Như trên; `hmac` ký `<timestamp>.GET.<path>`, `X-Xaas-Timestamp` bắt buộc |
| `POST /internal/triggers/:id/fire` | Schedule QStash của trigger `cron` | Chữ ký Upstash |
| `POST /internal/events/run-finished` | Runner báo run kết thúc: gửi result callback, chạy run đang chờ slot và bắn trigger `event`. App gửi `{ runId, callbackOnly: true }` khi tự kết thúc run (hủy, sweep run treo) | Chữ ký Upstash |
| `POST /internal/callbacks/deliver` | Một lần POST kết quả của run tới callback URL; trả 502 để QStash thử lại | Chữ ký Upstash |
| `POST /triggers/:id/fire` | "Fire now" từ app | Bearer `INGRESS_API_TOKEN` |
| `POST /dispatch-queued` | Sweep của app: khởi động run đang xếp hàng khi trigger còn slot | Bearer `INGRESS_API_TOKEN` |
| `POST /runs` | Chạy workflow từ app | Bearer `INGRESS_API_TOKEN` |
| `POST /hooks/telegram/:botId` | Update của bot có connection bật sự kiện (app gọi `setWebhook`) | Header `X-Telegram-Bot-Api-Secret-Token`, so SHA-256 với `connection_subscriptions.secret_hash` |
| `GET /hooks/facebook` | Facebook xác minh webhook của app | `hub.verify_token` = secret `FACEBOOK_WEBHOOK_VERIFY_TOKEN` |
| `POST /hooks/facebook` | Webhook Page: comment, Messenger, Lead Ads | `X-Hub-Signature-256` (HMAC body bằng `FACEBOOK_APP_SECRET`) |

### Sự kiện của connection (`src/connection-events.ts`)

Mỗi delivery được chuẩn hóa thành sự kiện (`telegram.message`, `telegram.channel_post`, `telegram.callback_query`, `facebook.comment`, `facebook.message`, `facebook.postback`, `facebook.lead`), ghi `inbound_event_receipts` để delivery gửi lại không chạy trùng, rồi bắn trigger `event` của mọi connection của tài khoản đó đang phát loại sự kiện ấy (`connections.events`; sự kiện do chính tài khoản gửi, `sender.role = self`, chỉ tới connection bật `fromSelf`). Lỗi khi bắn: xóa receipt và trả 500 để dịch vụ gửi lại. Cần thêm secret (cùng giá trị với app):

```bash
npx wrangler secret put FACEBOOK_APP_ID
```

```bash
npx wrangler secret put FACEBOOK_APP_SECRET
```

```bash
npx wrangler secret put FACEBOOK_WEBHOOK_VERIFY_TOKEN
```

Sau khi deploy, trỏ webhook Page của app Facebook về đây một lần, từ thư mục `xaas-workflows`: `npx tsx scripts/setup-facebook-webhook.ts --yes`.

### Kết quả của trigger `api` (`src/results.ts`, `src/callback-target.ts`)

- Câu trả lời 202 có `statusUrl` (`/wh/:token/runs/:runId`). `GET` trả `{ runId, no, version, status, env, createdAt, startedAt, finishedAt, output, error, nodes, statusUrl }`; `nodes[]` là các node run đã tới (`{ id, label, type, status, startedAt, finishedAt, error }`, `id` là id của node trong graph của `version`), không có output của node; chỉ run thật của chính trigger đó (run test, run của trigger khác: 404).
- **Result callback** (`triggers.callback_*`): URL mặc định, hoặc header `X-Xaas-Callback-Url` khi origin nằm trong `callback_allowed_origins` (`https://*.host` khớp subdomain). Callback tắt mà gọi có header: `422 callbacks_off`; origin không được phép: `422 callback_url_not_allowed`. URL được chốt lúc nhận lời gọi: dòng `run_callbacks` tạo cùng transaction với run.
- Khi run kết thúc, `run-finished` chuyển `run_callbacks` từ `waiting` sang `sending` (một lần) và publish QStash tới `/internal/callbacks/deliver`, 3 lần thử lại. Mỗi lần thử ký lại: `X-Xaas-Timestamp` + `X-Xaas-Signature: sha256=<hex HMAC(secret của trigger, "<timestamp>.<body>")>`, cùng `X-Xaas-Event`, `X-Xaas-Delivery-Id`, `X-Xaas-Run-Id`; không theo redirect, timeout 15 s. Output trên 64 KB bị bỏ (`outputOmitted: true`).

Mỗi lượt gọi trigger qua các bước: bật, workflow `active`, rate limit, filter, input mapping, rồi chính sách concurrency; mọi kết quả ghi vào `trigger_events`. Secret HMAC / bearer lưu mã hóa; Worker cần secret `TRIGGERS_ENCRYPTION_KEY` (cùng giá trị với app). `src/graph.ts` giống từng byte với bản ở runner và app.
