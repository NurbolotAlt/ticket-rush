"use strict";

// Простой роутинг по hash: "#/events/1" — концерт, всё остальное — список.
// Страница одна (index.html), поэтому nginx отдаёт её для любого пути из кэша
// браузера, а данные приходят из API (которое nginx тоже кэширует).

const $ = (id) => document.getElementById(id);

const STATUS_TEXT = { free: "свободно", held: "забронировано", sold: "продано" };

// Все данные вставляются через textContent, не innerHTML: названия концертов
// приходят из базы, и разметкой они стать не должны (XSS).
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function formatDate(iso) {
  return new Date(iso).toLocaleString("ru-RU", { dateStyle: "long", timeStyle: "short" });
}

function money(cents) {
  return (cents / 100).toLocaleString("ru-RU", { style: "currency", currency: "USD" });
}

async function getJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(body?.detail || `Ошибка ${response.status}`);
  }
  return { data: await response.json(), cache: response.headers.get("X-Cache-Status") };
}

function showError(message) {
  $("error").hidden = !message;
  $("error").textContent = message || "";
}

async function showEvents() {
  $("events-view").hidden = false;
  $("event-view").hidden = true;
  const { data: events } = await getJson("/api/events");
  const list = $("events");
  list.replaceChildren(...events.map((event) => {
    const link = el("a");
    link.href = `#/events/${event.id}`;
    link.append(
      el("div", "title", event.title),
      el("div", "muted", `${event.venue} · ${formatDate(event.starts_at)}`),
      el("div", "muted", `Продажи с ${formatDate(event.sale_opens_at)}`),
    );
    const item = el("li");
    item.append(link);
    return item;
  }));
}

function renderSeatmap(seatmap) {
  const hall = $("hall");
  const stage = hall.querySelector(".stage");
  const sections = seatmap.sections.map((section) => {
    const block = el("div", "section");
    block.append(el("div", "section-title", `Сектор ${section.name} · ${money(section.price_cents)}`));
    for (const row of section.rows) {
      const line = el("div", "row");
      line.append(el("span", "row-label", String(row.row)));
      for (const [id, number, status] of row.seats) {
        const seat = el("span", `seat ${status}`);
        seat.dataset.seatId = id;
        seat.title = `Сектор ${section.name}, ряд ${row.row}, место ${number} — ${STATUS_TEXT[status]}`;
        line.append(seat);
      }
      block.append(line);
    }
    return block;
  });
  hall.replaceChildren(stage, ...sections);
}

async function showEvent(eventId) {
  $("events-view").hidden = true;
  $("event-view").hidden = false;
  // Карточка и схема — параллельно: это два независимых кэшируемых запроса.
  const [{ data: event }, { data: seatmap, cache }] = await Promise.all([
    getJson(`/api/events/${eventId}`),
    getJson(`/api/events/${eventId}/seats`),
  ]);
  $("event-title").textContent = event.title;
  $("event-meta").textContent =
    `${event.venue} · ${formatDate(event.starts_at)} · мест: ${event.total_seats}`;
  $("legend").replaceChildren(...Object.entries(STATUS_TEXT).map(([status, text]) => {
    const item = el("span");
    item.append(el("span", `seat ${status}`), text);
    return item;
  }));
  $("source").textContent = `схема зала: версия ${seatmap.version}, кэш nginx: ${cache ?? "—"}`;
  renderSeatmap(seatmap);
  queue.open(event);
}

// ---------------------------------------------------------------------------
// Очередь ожидания
//
// Сервер раз в секунду рассылает ВСЕМ одно общее состояние: "впущены номера до
// N, скорость R в секунду". Свою позицию и время ожидания клиент считает сам из
// своего номера — поэтому 10 000 ждущих стоят серверу одного сообщения в секунду.
// ---------------------------------------------------------------------------

const HEARTBEAT_MS = 15000;

const queue = {
  event: null,
  seq: null,
  ws: null,
  timers: [],
  reconnectDelay: 1000,

  async open(event) {
    this.close();
    this.event = event;
    const opensAt = new Date(event.sale_opens_at);
    if (opensAt > new Date()) return this.countdownToOpening(opensAt);
    // Уже стоим в очереди (перезагрузили страницу)? Сессия — в cookie.
    const status = await this.fetchStatus();
    if (status.state === "not_in_queue") return this.offerJoin();
    this.apply(status);
    this.connect();
  },

  close() {
    this.timers.forEach(clearInterval);
    this.timers = [];
    if (this.ws) { this.ws.onclose = null; this.ws.close(); this.ws = null; }
    this.seq = null;
  },

  view(text, meta = "", { button = null, progress = null, admitted = false } = {}) {
    $("queue").classList.toggle("admitted", admitted);
    $("queue-text").textContent = text;
    $("queue-meta").textContent = meta;
    $("queue-btn").hidden = !button;
    if (button) $("queue-btn").textContent = button;
    $("queue-progress").hidden = progress === null;
    if (progress !== null) $("queue-bar").style.width = `${Math.round(progress * 100)}%`;
  },

  countdownToOpening(opensAt) {
    const tick = () => {
      const left = Math.ceil((opensAt - new Date()) / 1000);
      if (left <= 0) { this.close(); this.offerJoin(); return; }
      const m = Math.floor(left / 60), s = String(left % 60).padStart(2, "0");
      this.view(`Продажи откроются через ${m}:${s}`, "Встать в очередь можно с момента открытия.");
    };
    tick();
    this.timers.push(setInterval(tick, 1000));
  },

  offerJoin() {
    this.view("Продажи открыты", "Места раздаются по очереди — встаньте в неё.",
      { button: "Встать в очередь" });
  },

  async join() {
    $("queue-btn").disabled = true;
    try {
      const response = await fetch(`/api/events/${this.event.id}/queue`, { method: "POST" });
      const body = await response.json().catch(() => null);
      if (!response.ok) {
        const wait = response.headers.get("Retry-After");
        this.view(body?.detail || `Ошибка ${response.status}`,
          wait ? `Попробуйте через ${wait} с.` : "", { button: "Попробовать снова" });
        return;
      }
      this.apply(body);
      this.connect();
    } finally {
      $("queue-btn").disabled = false;
    }
  },

  async fetchStatus() {
    const response = await fetch(`/api/events/${this.event.id}/queue/me`);
    return response.json();
  },

  // Состояние из ответа сервера про НАШУ сессию.
  apply(status) {
    this.seq = status.seq ?? null;
    if (status.state === "admitted") return this.showAdmitted(status.pass_expires_at);
    if (status.state === "passed") {
      return this.view("Ваш пропуск истёк",
        "Вы отошли или не успели купить. Можно встать в очередь заново — в конец.",
        { button: "Встать заново" });
    }
    if (status.state === "waiting") {
      this.showWaiting({ admitted_upto: status.admitted_upto, rate_per_second: status.rate_per_second });
    }
  },

  // Общее состояние очереди из рассылки — позицию считаем сами.
  async onBroadcast(state) {
    if (this.seq === null) return;
    if (state.admitted_upto >= this.seq) {
      // Наш номер прошёл: впустили или пропустили — уточняем у сервера.
      this.apply(await this.fetchStatus());
      return;
    }
    this.showWaiting(state);
  },

  showWaiting(state) {
    const position = this.seq - state.admitted_upto;
    const rate = state.rate_per_second;
    const eta = rate > 0 ? Math.ceil(position / rate) : null;
    const etaText = eta === null ? "оцениваем время…"
      : eta < 60 ? `≈ ${eta} с` : `≈ ${Math.ceil(eta / 60)} мин`;
    this.view(`Перед вами ${position.toLocaleString("ru-RU")} чел.`,
      `Ваш номер ${this.seq}. Ожидание ${etaText}. Не закрывайте вкладку — ушедших пропускают.`,
      { progress: this.seq > 0 ? state.admitted_upto / this.seq : 0 });
  },

  showAdmitted(expiresAt) {
    this.timers.forEach(clearInterval);
    this.timers = [];
    const tick = () => {
      const left = Math.max(0, expiresAt - Math.floor(Date.now() / 1000));
      const m = Math.floor(left / 60), s = String(left % 60).padStart(2, "0");
      this.view("Ваша очередь! Выбирайте места", `Пропуск действует ещё ${m}:${s}.`,
        { admitted: true });
      if (left === 0) this.apply({ state: "passed" });
    };
    tick();
    this.timers.push(setInterval(tick, 1000));
  },

  connect() {
    if (this.ws) return;
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws/events/${this.event.id}/queue`);
    this.ws = ws;
    ws.onopen = () => {
      this.reconnectDelay = 1000;
      // Heartbeat: сервер пропускает тех, от кого давно нет вестей (закрыли вкладку).
      const beat = setInterval(() => ws.readyState === 1 && ws.send("ping"), HEARTBEAT_MS);
      this.timers.push(beat);
    };
    ws.onmessage = (message) => {
      try { this.onBroadcast(JSON.parse(message.data)); } catch { /* битое сообщение */ }
    };
    ws.onclose = () => {
      // Реплика перезапустилась или сеть моргнула — переподключаемся с растущей
      // паузой: сотни вкладок не должны долбить лежащий сервер каждую секунду.
      this.ws = null;
      setTimeout(() => this.event && this.connect(), this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    };
  },
};

$("queue-btn").addEventListener("click", () => queue.join());

async function route() {
  showError(null);
  $("source").textContent = "";
  queue.close();
  queue.event = null;
  const match = location.hash.match(/^#\/events\/(\d+)$/);
  try {
    if (match) await showEvent(Number(match[1]));
    else await showEvents();
  } catch (err) {
    showError(err.message);
  }
}

$("back").addEventListener("click", (e) => { e.preventDefault(); location.hash = ""; });
window.addEventListener("hashchange", route);
route();
