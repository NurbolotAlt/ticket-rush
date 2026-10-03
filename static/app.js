"use strict";

// Страница одна (index.html), маршрут — в hash: "#/events/1" — концерт, иначе список.
// Три части: live (одно WebSocket-соединение на страницу), queue (очередь
// ожидания), hall (схема зала, выбор и бронь мест).

const $ = (id) => document.getElementById(id);

const STATUS_TEXT = { free: "свободно", held: "забронировано", sold: "продано", mine: "ваша бронь" };
const HEARTBEAT_MS = 15000;

// Данные вставляются только через textContent, не innerHTML: названия приходят из
// базы, и разметкой они стать не должны (XSS).
function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

const formatDate = (iso) =>
  new Date(iso).toLocaleString("ru-RU", { dateStyle: "long", timeStyle: "short" });
const money = (cents) =>
  (cents / 100).toLocaleString("ru-RU", { style: "currency", currency: "USD" });
const mmss = (seconds) =>
  `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, "0")}`;

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

// ---------------------------------------------------------------------------
// live: одно WebSocket-соединение на страницу концерта.
// По нему приходят состояние очереди, точный снимок статусов мест и их изменения.
// ---------------------------------------------------------------------------

const live = {
  eventId: null,
  ws: null,
  beat: null,
  reconnectDelay: 1000,

  open(eventId) {
    this.close();
    this.eventId = eventId;
    this.connect();
  },

  close() {
    this.eventId = null;
    clearInterval(this.beat);
    if (this.ws) { this.ws.onclose = null; this.ws.close(); this.ws = null; }
  },

  // Переподключиться сразу: например, при пропуске версии — новое соединение
  // начнётся со свежего точного снимка.
  restart() {
    if (!this.ws) return;
    this.ws.onclose = null;
    this.ws.close();
    this.ws = null;
    this.connect();
  },

  connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws/events/${this.eventId}`);
    this.ws = ws;
    hall.resetLive();
    ws.onopen = () => {
      this.reconnectDelay = 1000;
      clearInterval(this.beat);
      // Heartbeat: сервер пропускает в очереди тех, от кого давно нет вестей.
      this.beat = setInterval(() => ws.readyState === 1 && ws.send("ping"), HEARTBEAT_MS);
    };
    ws.onmessage = (event) => {
      let message;
      try { message = JSON.parse(event.data); } catch { return; }
      if (message.type === "queue") queue.onBroadcast(message);
      else if (message.type === "seats_snapshot") hall.applySnapshot(message);
      else if (message.type === "seats") hall.applyChanges(message);
    };
    ws.onclose = () => {
      // Реплика перезапустилась или сеть моргнула — переподключаемся с растущей
      // паузой: сотни вкладок не должны долбить лежащий сервер каждую секунду.
      this.ws = null;
      clearInterval(this.beat);
      setTimeout(() => this.eventId && !this.ws && this.connect(), this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    };
  },
};

// ---------------------------------------------------------------------------
// queue: очередь ожидания. Сервер раз в секунду рассылает ВСЕМ одно состояние
// ("впущены номера до N, скорость R/с"); позицию и ETA клиент считает сам.
// ---------------------------------------------------------------------------

const queue = {
  event: null,
  seq: null,
  admitted: false,
  timer: null,

  async open(event) {
    this.close();
    this.event = event;
    const opensAt = new Date(event.sale_opens_at);
    if (opensAt > new Date()) return this.countdownToOpening(opensAt);
    // Уже в очереди (перезагрузили страницу)? Сессия — в cookie.
    const status = await this.fetchStatus();
    if (status.state === "not_in_queue") return this.offerJoin();
    this.apply(status);
  },

  close() {
    clearInterval(this.timer);
    this.timer = null;
    this.seq = null;
    this.admitted = false;
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
      this.view(`Продажи откроются через ${mmss(left)}`, "Встать в очередь можно с момента открытия.");
    };
    tick();
    this.timer = setInterval(tick, 1000);
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
    } finally {
      $("queue-btn").disabled = false;
    }
  },

  async fetchStatus() {
    return (await fetch(`/api/events/${this.event.id}/queue/me`)).json();
  },

  apply(status) {
    this.seq = status.seq ?? null;
    if (status.state === "admitted") return this.showAdmitted(status.pass_expires_at);
    this.admitted = false;
    hall.setSelectable(false);
    if (status.state === "passed") {
      return this.view("Ваш пропуск истёк",
        "Вы отошли или не успели купить. Можно встать в очередь заново — в конец.",
        { button: "Встать заново" });
    }
    if (status.state === "waiting") this.showWaiting(status);
  },

  async onBroadcast(state) {
    if (this.seq === null || this.admitted) return;
    if (state.admitted_upto >= this.seq) {
      // Наш номер прошёл — впустили или пропустили: уточняем у сервера.
      this.apply(await this.fetchStatus());
      return;
    }
    this.showWaiting(state);
  },

  showWaiting(state) {
    const position = this.seq - state.admitted_upto;
    const rate = state.rate_per_second;
    const eta = rate > 0 ? Math.ceil(position / rate) : null;
    const etaText = eta === null ? "оцениваем время…" : eta < 60 ? `≈ ${eta} с` : `≈ ${Math.ceil(eta / 60)} мин`;
    this.view(`Перед вами ${position.toLocaleString("ru-RU")} чел.`,
      `Ваш номер ${this.seq}. Ожидание ${etaText}. Не закрывайте вкладку — ушедших пропускают.`,
      { progress: this.seq > 0 ? state.admitted_upto / this.seq : 0 });
  },

  showAdmitted(expiresAt) {
    this.admitted = true;
    clearInterval(this.timer);
    hall.setSelectable(true);
    const tick = () => {
      const left = Math.max(0, expiresAt - Math.floor(Date.now() / 1000));
      this.view("Ваша очередь! Выберите до 4 мест на схеме",
        `Пропуск действует ещё ${mmss(left)}.`, { admitted: true });
      if (left === 0) { clearInterval(this.timer); this.apply({ state: "passed" }); }
    };
    tick();
    this.timer = setInterval(tick, 1000);
  },
};

// ---------------------------------------------------------------------------
// hall: схема зала. Раскладка мест — из кэшируемого API, статусы — сначала
// оттуда же, затем ТОЧНЫЙ снимок по WebSocket и изменения поверх него.
//
// Версии: снимок имеет версию V; изменение с версией <= V уже в нём учтено
// (отбрасываем), V+1 — следующее по порядку (применяем), > V+1 — что-то
// пропустили, и тогда переподключаемся за свежим снимком.
// ---------------------------------------------------------------------------

const MAX_SEATS = 4;

const hall = {
  eventId: null,
  seats: new Map(),     // id -> { node, status, label, price }
  version: null,        // версия точного снимка; null — ещё не получен
  pending: [],          // изменения, пришедшие раньше снимка
  mine: new Set(),
  selected: new Set(),
  selectable: false,
  holdExpiresAt: null,
  timer: null,

  render(seatmap) {
    this.eventId = seatmap.event_id;
    this.seats.clear();
    this.mine.clear();
    this.selected.clear();
    const hallNode = $("hall");
    const stage = hallNode.querySelector(".stage");
    const blocks = seatmap.sections.map((section) => {
      const block = el("div", "section");
      block.append(el("div", "section-title", `Сектор ${section.name} · ${money(section.price_cents)}`));
      for (const row of section.rows) {
        const line = el("div", "row");
        line.append(el("span", "row-label", String(row.row)));
        for (const [id, number, status] of row.seats) {
          const node = el("button", "seat");
          node.type = "button";
          node.addEventListener("click", () => this.onSeatClick(id));
          this.seats.set(id, {
            node, status, price: section.price_cents,
            label: `Сектор ${section.name}, ряд ${row.row}, место ${number}`,
          });
          line.append(node);
        }
        block.append(line);
      }
      return block;
    });
    hallNode.replaceChildren(stage, ...blocks);
    this.seats.forEach((_, id) => this.paint(id));
    this.renderCart();
  },

  resetLive() {
    this.version = null;
    this.pending = [];
  },

  applySnapshot(snapshot) {
    const held = new Set(snapshot.held), sold = new Set(snapshot.sold);
    this.seats.forEach((seat, id) => {
      seat.status = sold.has(id) ? "sold" : held.has(id) ? "held" : "free";
      this.paint(id);
    });
    this.version = snapshot.version;
    const buffered = this.pending;
    this.pending = [];
    buffered.forEach((message) => this.applyChanges(message));
  },

  applyChanges(message) {
    if (this.version === null) { this.pending.push(message); return; }
    if (message.version <= this.version) return;          // уже в снимке
    if (message.version > this.version + 1) {             // пропуск — нужен свежий снимок
      live.restart();
      return;
    }
    this.version = message.version;
    for (const [id, status] of message.changes) {
      const seat = this.seats.get(id);
      if (!seat) continue;
      seat.status = status;
      if (status === "free" && this.mine.has(id)) this.mine.delete(id);  // наша бронь истекла
      this.selected.delete(id);
      this.paint(id);
    }
    this.renderCart();
  },

  paint(id) {
    const seat = this.seats.get(id);
    const shown = this.mine.has(id) ? "mine" : seat.status;
    seat.node.className = `seat ${shown}${this.selected.has(id) ? " selected" : ""}`;
    seat.node.title = `${seat.label} — ${STATUS_TEXT[shown]}, ${money(seat.price)}`;
    seat.node.disabled = !this.selectable || (shown !== "free" && shown !== "mine");
  },

  setSelectable(on) {
    this.selectable = on;
    if (!on) this.selected.clear();
    this.seats.forEach((_, id) => this.paint(id));
    this.renderCart();
    if (on) this.loadMine();
  },

  async loadMine() {
    const { data } = await getJson(`/api/events/${this.eventId}/holds/me`);
    this.setMine(data.seat_ids, data.expires_at);
  },

  setMine(seatIds, expiresAt) {
    const previous = this.mine;
    this.mine = new Set(seatIds);
    [...previous, ...this.mine].forEach((id) => this.seats.has(id) && this.paint(id));
    this.holdExpiresAt = expiresAt;
    this.renderCart();
  },

  async onSeatClick(id) {
    if (this.mine.has(id)) {
      // Своё место — снять бронь.
      const response = await fetch(`/api/events/${this.eventId}/holds/${id}`, { method: "DELETE" });
      if (response.ok) this.setMine([...this.mine].filter((s) => s !== id), this.holdExpiresAt);
      return;
    }
    if (this.selected.has(id)) this.selected.delete(id);
    else if (this.selected.size + this.mine.size < MAX_SEATS) this.selected.add(id);
    this.paint(id);
    this.renderCart();
  },

  async holdSelected() {
    const seatIds = [...this.selected];
    $("cart-btn").disabled = true;
    try {
      const response = await fetch(`/api/events/${this.eventId}/holds`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ seat_ids: seatIds }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok) {
        // Места, которые кто-то успел занять, снимаем из выбора.
        (body?.seat_ids || []).forEach((id) => { this.selected.delete(id); this.seats.has(id) && this.paint(id); });
        $("cart-note").textContent = body?.detail || `Ошибка ${response.status}`;
        this.renderCart(false);
        return;
      }
      this.selected.clear();
      this.setMine(body.seat_ids, body.expires_at);
    } finally {
      $("cart-btn").disabled = false;
    }
  },

  // Ключ идемпотентности живёт, пока длится ОДНА попытка оплаты. Повтор после
  // обрыва сети или 503 идёт с тем же ключом — если первая попытка на самом деле
  // прошла, сервер вернёт её результат, а не спишет деньги второй раз. Новый ключ
  // — только для новой попытки (банк отказал, бронь истекла).
  checkoutKey: null,

  async pay() {
    const seatIds = [...this.mine].sort((a, b) => a - b);
    this.checkoutKey ??= crypto.randomUUID();
    $("pay-btn").disabled = true;
    $("cart-note").textContent = "Оплачиваем…";
    try {
      for (let attempt = 0; attempt < 5; attempt++) {
        let response, body;
        try {
          response = await fetch(`/api/events/${this.eventId}/orders`, {
            method: "POST",
            headers: { "Content-Type": "application/json", "Idempotency-Key": this.checkoutKey },
            body: JSON.stringify({ seat_ids: seatIds }),
          });
          body = await response.json().catch(() => null);
        } catch {
          // Сеть оборвалась: мы не знаем, прошла ли оплата. Повторяем С ТЕМ ЖЕ ключом.
          await new Promise((r) => setTimeout(r, 1000));
          continue;
        }
        if (response.ok) {
          this.checkoutKey = null;
          return this.showTickets(body);
        }
        const inProgress = response.status === 409 && body?.reason === "order_in_progress";
        if (inProgress || response.status === 503) {
          const wait = Number(response.headers.get("Retry-After") || 1);
          await new Promise((r) => setTimeout(r, wait * 1000));
          continue;
        }
        // Окончательный отказ: следующая попытка будет новой — с новым ключом.
        this.checkoutKey = null;
        $("cart-note").textContent = body?.detail || `Ошибка ${response.status}`;
        if (body?.reason === "holds_mismatch") this.loadMine();
        return;
      }
      $("cart-note").textContent = "Не удалось связаться с сервером — попробуйте ещё раз.";
    } finally {
      $("pay-btn").disabled = false;
    }
  },

  showTickets(order) {
    this.setMine([], null);
    queue.close();
    this.setSelectable(false);
    $("queue").hidden = true;
    $("tickets").hidden = false;
    $("tickets-meta").textContent = `Заказ №${order.order_id} · ${money(order.amount_cents)}`;
    $("tickets-list").replaceChildren(...order.tickets.map((ticket) => {
      const item = el("li");
      item.append(`${this.seats.get(ticket.seat_id)?.label ?? `Место ${ticket.seat_id}`} — код `,
        el("code", null, ticket.code));
      return item;
    }));
  },

  renderCart(clearNote = true) {
    const cart = $("cart");
    cart.hidden = !this.selectable;
    $("pay-btn").hidden = !this.selectable || this.mine.size === 0;
    if (!this.selectable) return;
    if (clearNote) $("cart-note").textContent = "";
    const total = [...this.mine].reduce((sum, id) => sum + (this.seats.get(id)?.price || 0), 0);
    $("cart-mine").textContent = this.mine.size
      ? `Забронировано: ${this.mine.size} (${money(total)})` : "Пока ничего не забронировано";
    $("cart-btn").hidden = this.selected.size === 0;
    $("cart-btn").textContent = `Забронировать ${this.selected.size}`;
    clearInterval(this.timer);
    if (this.mine.size && this.holdExpiresAt) {
      const tick = () => {
        const left = Math.max(0, this.holdExpiresAt - Math.floor(Date.now() / 1000));
        $("cart-timer").textContent = `Бронь держится ещё ${mmss(left)}. Нажмите на своё место, чтобы снять.`;
      };
      tick();
      this.timer = setInterval(tick, 1000);
    } else {
      $("cart-timer").textContent = "";
    }
  },
};

// ---------------------------------------------------------------------------
// Страницы
// ---------------------------------------------------------------------------

async function showEvents() {
  $("events-view").hidden = false;
  $("event-view").hidden = true;
  const { data: events } = await getJson("/api/events");
  $("events").replaceChildren(...events.map((event) => {
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

async function showEvent(eventId) {
  $("events-view").hidden = true;
  $("event-view").hidden = false;
  $("queue").hidden = false;
  $("tickets").hidden = true;
  // Карточка и схема — параллельно: два независимых кэшируемых запроса.
  const [{ data: event }, { data: seatmap, cache }] = await Promise.all([
    getJson(`/api/events/${eventId}`),
    getJson(`/api/events/${eventId}/seats`),
  ]);
  $("event-title").textContent = event.title;
  $("event-meta").textContent = `${event.venue} · ${formatDate(event.starts_at)} · мест: ${event.total_seats}`;
  $("legend").replaceChildren(...Object.entries(STATUS_TEXT).map(([status, text]) => {
    const item = el("span");
    item.append(el("span", `seat ${status}`), text);
    return item;
  }));
  $("source").textContent = `схема из кэша nginx: ${cache ?? "—"}, дальше — живые изменения`;
  hall.render(seatmap);
  live.open(eventId);
  await queue.open(event);
}

async function route() {
  showError(null);
  $("source").textContent = "";
  live.close();
  queue.close();
  hall.setSelectable(false);
  const match = location.hash.match(/^#\/events\/(\d+)$/);
  try {
    if (match) await showEvent(Number(match[1]));
    else await showEvents();
  } catch (err) {
    showError(err.message);
  }
}

$("queue-btn").addEventListener("click", () => queue.join());
$("cart-btn").addEventListener("click", () => hall.holdSelected());
$("pay-btn").addEventListener("click", () => hall.pay());
$("back").addEventListener("click", (e) => { e.preventDefault(); location.hash = ""; });
window.addEventListener("hashchange", route);
route();
