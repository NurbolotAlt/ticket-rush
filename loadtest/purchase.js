// Покупка под нагрузкой с "двойными кликами".
//
// VUS человек: очередь -> бронь 2 случайных соседних мест -> "Оплатить" ТРИ раза
// одновременно с одним ключом идемпотентности (нетерпеливый пользователь).
// Цель — инварианты: один заказ и одно списание на ключ, ни одного места с двумя
// билетами. Их проверяет скрипт после теста — по базе и Redis, а не по ответам.
//
//   docker compose -f docker-compose.yml -f loadtest/docker-compose.loadtest.yml up -d --scale api=2
//   docker run --rm -i --network ticket-rush_default grafana/k6 run - < loadtest/purchase.js

import http from "k6/http";
import ws from "k6/ws";
import exec from "k6/execution";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "https://nginx";
const WS_BASE = BASE.replace(/^http/, "ws");
const EVENT_ID = __ENV.EVENT_ID || 1;

const held = new Counter("hold_ok");
const holdTaken = new Counter("hold_taken");
const created = new Counter("purchase_created");      // 201 — заказ создан
const replayed = new Counter("purchase_replayed");    // 200 — повтор того же ключа
const inProgress = new Counter("purchase_in_progress"); // 409 — первый ещё идёт
const unexpected = new Counter("unexpected_errors");
const purchaseMs = new Trend("purchase_duration_ms");

export const options = {
  insecureSkipTLSVerify: true,
  scenarios: {
    rush: { executor: "per-vu-iterations", vus: Number(__ENV.VUS || 1500), iterations: 1, maxDuration: "5m" },
  },
  thresholds: {
    unexpected_errors: ["count==0"],
    purchase_duration_ms: ["p(95)<2000"],
  },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "max"],
};

export function setup() {
  const seatmap = http.get(`${BASE}/api/events/${EVENT_ID}/seats`).json();
  return { rows: seatmap.sections.flatMap((s) => s.rows.map((r) => r.seats.map((seat) => seat[0]))) };
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

function buy(headers, seats) {
  const key = uuid();
  const request = {
    method: "POST",
    url: `${BASE}/api/events/${EVENT_ID}/orders`,
    body: JSON.stringify({ seat_ids: seats }),
    params: {
      headers: { ...headers, "Content-Type": "application/json", "Idempotency-Key": key },
      tags: { name: "purchase" },
    },
  };
  const started = Date.now();
  // Три одинаковых запроса разом — как тройной клик по "Оплатить".
  const responses = http.batch([request, request, request]);
  purchaseMs.add(Date.now() - started);
  for (const r of responses) {
    if (r.status === 201) created.add(1);
    else if (r.status === 200) replayed.add(1);
    else if (r.status === 409 && r.json("reason") === "order_in_progress") inProgress.add(1);
    else unexpected.add(1);
  }
}

export default function (data) {
  const headers = { "X-Forwarded-For": clientIp() };
  const join = http.post(`${BASE}/api/events/${EVENT_ID}/queue`, null, { headers });
  if (!check(join, { "joined queue": (r) => r.status === 200 })) return;
  const { seq } = join.json();
  const withCookie = { ...headers, Cookie: `sid=${join.cookies.sid[0].value}` };

  let admitted = false;
  ws.connect(`${WS_BASE}/ws/events/${EVENT_ID}`, { headers: withCookie }, (socket) => {
    socket.on("message", (raw) => {
      const message = JSON.parse(raw);
      if (message.type !== "queue" || admitted || message.admitted_upto < seq) return;
      admitted = true;
      const row = data.rows[Math.floor(Math.random() * data.rows.length)];
      const start = Math.floor(Math.random() * (row.length - 1));
      const seats = [row[start], row[start + 1]];
      const hold = http.post(`${BASE}/api/events/${EVENT_ID}/holds`, JSON.stringify({ seat_ids: seats }), {
        headers: { ...withCookie, "Content-Type": "application/json" },
      });
      if (hold.status === 200) {
        held.add(1);
        buy(withCookie, seats);
      } else if (hold.status === 409) {
        holdTaken.add(1);
      } else {
        unexpected.add(1);
      }
      socket.close();
    });
    socket.setInterval(() => socket.send("ping"), 15000);
    socket.setTimeout(() => socket.close(), 250000);
  });
  check(null, { "admitted before timeout": () => admitted });
}
