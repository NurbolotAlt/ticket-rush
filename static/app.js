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
}

async function route() {
  showError(null);
  $("source").textContent = "";
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
