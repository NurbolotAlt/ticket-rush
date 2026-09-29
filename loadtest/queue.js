// Открытие продаж: VUS человек одновременно встают в очередь и ждут впуска.
//
// Каждый виртуальный пользователь: POST в очередь (получает номер и обещанное
// время ожидания) -> WebSocket с heartbeat -> ждёт, пока "впущены до N" дойдёт
// до его номера. Меряем, сколько реально ждал и насколько ошибся ETA.
//
// Нужен режим nginx с доверием к X-Forwarded-For (у каждого VU свой IP):
//   docker compose -f docker-compose.yml -f loadtest/docker-compose.loadtest.yml up -d --scale api=2
//   docker run --rm -i --network ticket-rush_default grafana/k6 run - < loadtest/queue.js

import http from "k6/http";
import ws from "k6/ws";
import exec from "k6/execution";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "https://nginx";
const WS_BASE = BASE.replace(/^http/, "ws");
const EVENT_ID = __ENV.EVENT_ID || 1;

const waitSeconds = new Trend("queue_wait_seconds");
const etaErrorSeconds = new Trend("queue_eta_error_seconds");
const admitted = new Counter("queue_admitted");

export const options = {
  insecureSkipTLSVerify: true,
  scenarios: {
    rush: {
      // Каждый VU делает ровно одну попытку — как живой человек, а не бот.
      executor: "per-vu-iterations",
      vus: Number(__ENV.VUS || 1500),
      iterations: 1,
      maxDuration: "4m",
    },
  },
  thresholds: {
    "http_req_failed{name:join}": ["rate<0.01"],
    "http_req_duration{name:join}": ["p(95)<1000"],
    checks: ["rate>0.99"],
  },
  summaryTrendStats: ["avg", "min", "p(50)", "p(95)", "max"],
};

function clientIp() {
  const id = exec.vu.idInTest;
  return `10.${(id >> 16) & 255}.${(id >> 8) & 255}.${id & 255}`;
}

export default function () {
  const headers = { "X-Forwarded-For": clientIp() };
  const joinedAt = Date.now();
  const res = http.post(`${BASE}/api/events/${EVENT_ID}/queue`, null, {
    headers,
    tags: { name: "join" },
  });
  if (!check(res, { "joined queue": (r) => r.status === 200 })) return;

  const { seq, eta_seconds: promisedEta } = res.json();
  const sid = res.cookies.sid[0].value;
  let done = false;

  ws.connect(
    `${WS_BASE}/ws/events/${EVENT_ID}`,
    // Cookie сессии браузер отправляет при открытии WebSocket сам; k6 — вручную.
    { headers: { ...headers, Cookie: `sid=${sid}` } },
    (socket) => {
      socket.on("message", (data) => {
        const state = JSON.parse(data);
        // По одному соединению приходят и очередь, и изменения мест.
        if (state.type !== "queue") return;
        if (!done && state.admitted_upto >= seq) {
          done = true;
          const waited = (Date.now() - joinedAt) / 1000;
          waitSeconds.add(waited);
          if (promisedEta !== null) etaErrorSeconds.add(waited - promisedEta);
          admitted.add(1);
          socket.close();
        }
      });
      socket.setInterval(() => socket.send("ping"), 15000);
      socket.setTimeout(() => socket.close(), 200000);
    },
  );
  check(null, { "admitted before timeout": () => done });
}
