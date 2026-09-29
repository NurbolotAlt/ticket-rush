// Гонка за местами: VUS человек проходят очередь и пытаются забронировать по 2
// места в "горячей зоне" (первые ряды сектора A) — все хотят одни и те же места.
//
// Цель — не "все успели", а инвариант: ни одно место не забронировано дважды.
// После теста сверяем с Redis: занято мест == успешных броней × 2.
//
//   docker compose -f docker-compose.yml -f loadtest/docker-compose.loadtest.yml up -d --scale api=2
//   docker run --rm -i --network ticket-rush_default grafana/k6 run - < loadtest/holds.js

import http from "k6/http";
import ws from "k6/ws";
import exec from "k6/execution";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "https://nginx";
const WS_BASE = BASE.replace(/^http/, "ws");
const EVENT_ID = __ENV.EVENT_ID || 1;
const HOT_ROWS = Number(__ENV.HOT_ROWS || 3);

const holdOk = new Counter("hold_ok");
const holdTaken = new Counter("hold_taken");
const holdOther = new Counter("hold_other");
const seatChangesSeen = new Counter("seat_changes_seen");
const holdDuration = new Trend("hold_duration_ms");

export const options = {
  insecureSkipTLSVerify: true,
  scenarios: {
    rush: { executor: "per-vu-iterations", vus: Number(__ENV.VUS || 1500), iterations: 1, maxDuration: "4m" },
  },
  thresholds: {
    hold_other: ["count==0"],               // ни одной неожиданной ошибки
    "http_req_failed{name:hold}": ["rate<1"], // 409 — ожидаемый исход, не сбой сети
  },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "max"],
};

// Раскладка зала один раз на весь тест: "горячая зона" — первые ряды сектора A.
export function setup() {
  const seatmap = http.get(`${BASE}/api/events/${EVENT_ID}/seats`).json();
  const rows = seatmap.sections[0].rows.slice(0, HOT_ROWS);
  return { rows: rows.map((row) => row.seats.map((seat) => seat[0])) };
}

function clientIp() {
  const id = exec.vu.idInTest;
  return `10.${(id >> 16) & 255}.${(id >> 8) & 255}.${id & 255}`;
}

// Два соседних места в случайном горячем ряду — как выбирают пары.
function pickPair(rows) {
  const row = rows[Math.floor(Math.random() * rows.length)];
  const start = Math.floor(Math.random() * (row.length - 1));
  return [row[start], row[start + 1]];
}

export default function (data) {
  const headers = { "X-Forwarded-For": clientIp() };
  const join = http.post(`${BASE}/api/events/${EVENT_ID}/queue`, null, { headers, tags: { name: "join" } });
  if (!check(join, { "joined queue": (r) => r.status === 200 })) return;
  const { seq } = join.json();
  const sid = join.cookies.sid[0].value;
  const cookieHeaders = { ...headers, Cookie: `sid=${sid}` };

  let admitted = false;
  ws.connect(`${WS_BASE}/ws/events/${EVENT_ID}`, { headers: cookieHeaders }, (socket) => {
    socket.on("message", (raw) => {
      const message = JSON.parse(raw);
      if (message.type === "seats") seatChangesSeen.add(1);
      if (message.type === "queue" && !admitted && message.admitted_upto >= seq) {
        admitted = true;
        const started = Date.now();
        const res = http.post(
          `${BASE}/api/events/${EVENT_ID}/holds`,
          JSON.stringify({ seat_ids: pickPair(data.rows) }),
          { headers: { ...cookieHeaders, "Content-Type": "application/json" }, tags: { name: "hold" } },
        );
        holdDuration.add(Date.now() - started);
        if (res.status === 200) holdOk.add(1);
        else if (res.status === 409) holdTaken.add(1);
        else holdOther.add(1);
        // Ещё немного послушаем изменения схемы, как живой зритель.
        socket.setTimeout(() => socket.close(), 3000);
      }
    });
    socket.setInterval(() => socket.send("ping"), 15000);
    socket.setTimeout(() => socket.close(), 200000);
  });
  check(null, { "admitted before timeout": () => admitted });
}
