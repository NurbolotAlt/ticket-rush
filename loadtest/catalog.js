// Нагрузка на каталог: все пользователи смотрят карточку концерта и схему зала.
// Так выглядят первые секунды открытия продаж — тысячи людей жмут F5.
//
// Через nginx (с кэшем):
//   docker run --rm -i --network ticket-rush_default grafana/k6 run - < loadtest/catalog.js
// Напрямую в API, мимо nginx и его кэша:
//   docker run --rm -i --network ticket-rush_default -e BASE_URL=http://api:8000 \
//     grafana/k6 run - < loadtest/catalog.js

import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "https://nginx";

export const options = {
  // Сертификат self-signed — проверку отключаем, это тест внутри своей сети.
  insecureSkipTLSVerify: true,
  scenarios: {
    browse: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 300),
      duration: __ENV.DURATION || "30s",
    },
  },
  // Пороги: тест "провален", если их нарушить. Так нагрузочный тест становится
  // проверкой, а не просто набором цифр.
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
};

export default function () {
  // Без пауз между запросами — это стресс, а не имитация живого человека:
  // хотим узнать потолок пропускной способности.
  const [event, seatmap] = http.batch([
    ["GET", `${BASE}/api/events/1`],
    ["GET", `${BASE}/api/events/1/seats`],
  ]);
  check(event, { "event 200": (r) => r.status === 200 });
  check(seatmap, { "seatmap 200": (r) => r.status === 200 });
}
