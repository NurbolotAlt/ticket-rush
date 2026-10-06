// "Открытие продаж" целиком: VUS человек приходят в первые ARRIVAL_SECONDS секунд
// и проходят весь путь как живые люди:
//
//   страница (статика + каталог + схема) -> WebSocket + очередь -> впуск ->
//   2-5 с выбирают места по ЖИВОЙ схеме -> бронь 1-4 соседних мест (заняли — ищут
//   другие) -> 1-3 с думают -> оплата (10% кликают дважды) -> PDF по уведомлению
//
// Мест меньше, чем людей: часть законно получает "всё распродано". Ошибкой
// считается только неожиданное — 5xx, обрывы, неизвестные коды.
//
//   sh loadtest/reset.sh
//   docker compose -f docker-compose.yml -f loadtest/docker-compose.loadtest.yml up -d --scale api=2 --scale worker=2
//   docker run --rm -i --network ticket-rush_default grafana/k6 run - < loadtest/journey.js

import http from "k6/http";
import ws from "k6/ws";
import exec from "k6/execution";
import { sleep } from "k6";
import { Counter, Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "https://nginx";
const WS_BASE = BASE.replace(/^http/, "ws");
const EVENT_ID = Number(__ENV.EVENT_ID || 1);
const ARRIVAL_SECONDS = Number(__ENV.ARRIVAL_SECONDS || 10);

const bought = new Counter("journey_bought");
const soldOut = new Counter("journey_sold_out");
const unexpected = new Counter("journey_unexpected");
const holdConflicts = new Counter("hold_conflicts");
const pageLoad = new Trend("step_page_load_ms");
const waitAdmit = new Trend("step_queue_wait_s");
const holdMs = new Trend("step_hold_ms");
const payMs = new Trend("step_pay_ms");
const pdfMs = new Trend("step_pay_to_pdf_ms");
const timeToTicket = new Trend("journey_time_to_ticket_s");

export const options = {
  insecureSkipTLSVerify: true,
  scenarios: {
    rush: {
      executor: "per-vu-iterations",
      vus: Number(__ENV.VUS || 3000),
      iterations: 1,
      maxDuration: "8m",
    },
  },
  thresholds: {
    journey_unexpected: ["count==0"],
    step_page_load_ms: ["p(95)<2000"],
    step_hold_ms: ["p(95)<1000"],
    step_pay_ms: ["p(95)<2000"],
  },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "max"],
};

// Раскладка зала один раз: ряды как списки id мест — чтобы искать соседние.
export function setup() {
  const seatmap = http.get(`${BASE}/api/events/${EVENT_ID}/seats`).json();
  return { rows: seatmap.sections.flatMap((s) => s.rows.map((r) => r.seats.map((x) => x[0]))) };
}

function clientIp() {
  const id = exec.vu.idInTest;
  return `10.${(id >> 16) & 255}.${(id >> 8) & 255}.${id & 255}`;
}

function uuid() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}

const rand = (min, max) => min + Math.random() * (max - min);

// N соседних свободных мест; ближние ряды популярнее — начинаем со случайного
// ряда в первой трети зала и идём назад.
function findSeats(rows, taken, count) {
  const start = Math.floor(Math.random() * Math.max(1, rows.length / 3));
  for (let i = 0; i < rows.length; i++) {
    const row = rows[(start + i) % rows.length];
    const offset = Math.floor(Math.random() * row.length);
    for (let j = 0; j < row.length; j++) {
      const from = (offset + j) % row.length;
      const pick = row.slice(from, from + count);
      if (pick.length === count && pick.every((id) => !taken.has(id))) return pick;
    }
  }
  return null;
}

function expected(res, ...codes) {
  if (!codes.includes(res.status)) {
    unexpected.add(1, { status: String(res.status), url: res.request.url.replace(/\d+/g, "N") });
    return false;
  }
  return true;
}

export default function (data) {
  sleep(rand(0, ARRIVAL_SECONDS)); // люди приходят не в одну миллисекунду
  const arrivedAt = Date.now();
  const headers = { "X-Forwarded-For": clientIp() };

  // --- Страница: статика и каталог, как браузер — параллельно -----------------
  const started = Date.now();
  const page = http.batch([
    ["GET", `${BASE}/`, null, { headers }],
    ["GET", `${BASE}/app.js`, null, { headers }],
    ["GET", `${BASE}/style.css`, null, { headers }],
    ["GET", `${BASE}/api/events/${EVENT_ID}`, null, { headers }],
    ["GET", `${BASE}/api/events/${EVENT_ID}/seats`, null, { headers }],
  ]);
  pageLoad.add(Date.now() - started);
  if (!page.every((r) => expected(r, 200))) return;

  const join = http.post(`${BASE}/api/events/${EVENT_ID}/queue`, null, { headers });
  if (join.status === 409 && join.json("reason") === "sold_out") { soldOut.add(1); return; }
  if (!expected(join, 200)) return;
  const { seq } = join.json();
  const withCookie = { ...headers, Cookie: `sid=${join.cookies.sid[0].value}` };
  const json = { ...withCookie, "Content-Type": "application/json" };

  const taken = new Set(); // занятые места по живой схеме зала
  let admitted = false;
  let orderId = null;
  let paidAt = 0;

  ws.connect(`${WS_BASE}/ws/events/${EVENT_ID}`, { headers: withCookie }, (socket) => {
    const finish = () => socket.close();

    const pay = (seats) => {
      const key = uuid();
      const request = {
        method: "POST",
        url: `${BASE}/api/events/${EVENT_ID}/orders`,
        body: JSON.stringify({ seat_ids: seats }),
        params: { headers: { ...json, "Idempotency-Key": key } },
      };
      const t = Date.now();
      // 10% — нетерпеливый двойной клик.
      const responses = Math.random() < 0.1 ? http.batch([request, request]) : [http.request(
        request.method, request.url, request.body, request.params)];
      payMs.add(Date.now() - t);
      const ok = responses.find((r) => r.status === 201 || r.status === 200);
      for (const r of responses) {
        if (r.status === 409 && r.json("reason") === "order_in_progress") continue;
        if (!expected(r, 200, 201)) return finish();
      }
      if (!ok) return finish();
      orderId = ok.json("order_id");
      paidAt = Date.now();
      // PDF придёт уведомлением; страховка — не ждать вечно.
      socket.setTimeout(finish, 30000);
    };

    const choose = (attempt) => {
      const seats = findSeats(data.rows, taken, 1 + Math.floor(Math.random() * 4));
      if (!seats) { soldOut.add(1); return finish(); }
      const t = Date.now();
      const res = http.post(`${BASE}/api/events/${EVENT_ID}/holds`, JSON.stringify({ seat_ids: seats }),
        { headers: json });
      holdMs.add(Date.now() - t);
      if (res.status === 409 && res.json("reason") === "taken") {
        holdConflicts.add(1);
        seats.forEach((id) => taken.add(id));
        if (attempt < 5) return choose(attempt + 1);
        soldOut.add(1);
        return finish();
      }
      if (!expected(res, 200)) return finish();
      socket.setTimeout(() => pay(seats), rand(1000, 3000)); // думает перед оплатой
    };

    socket.on("message", (raw) => {
      const message = JSON.parse(raw);
      if (message.type === "seats_snapshot") {
        message.held.concat(message.sold).forEach((id) => taken.add(id));
      } else if (message.type === "seats") {
        for (const [id, status] of message.changes) {
          if (status === "free") taken.delete(id); else taken.add(id);
        }
      } else if (message.type === "queue" && !admitted && message.sold_out) {
        soldOut.add(1); // распродано, пока ждали в очереди
        finish();
      } else if (message.type === "queue" && !admitted && message.admitted_upto >= seq) {
        admitted = true;
        waitAdmit.add((Date.now() - arrivedAt) / 1000);
        socket.setTimeout(() => choose(1), rand(2000, 5000)); // выбирает места
      } else if (message.type === "order" && message.order_id === orderId) {
        if (message.pdf_status !== "ready") { unexpected.add(1); return finish(); }
        pdfMs.add(Date.now() - paidAt);
        const pdf = http.get(`${BASE}/api/orders/${orderId}/tickets.pdf`, { headers: withCookie });
        if (expected(pdf, 200)) {
          bought.add(1);
          timeToTicket.add((Date.now() - arrivedAt) / 1000);
        }
        finish();
      }
    });
    socket.setInterval(() => socket.send("ping"), 15000);
    socket.setTimeout(finish, 400000);
  });
}
