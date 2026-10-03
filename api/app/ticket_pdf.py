"""Рендеринг PDF-билетов. Выполняется в ОТДЕЛЬНОМ процессе (ProcessPoolExecutor).

Поэтому функция синхронная и чистая: получает готовые данные (словари), отдаёт
байты. Никакой базы, Redis и логгера — у процесса пула их нет, а данные туда и
обратно передаются через pickle. Тот же приём, что fibonacci в compute-service.
"""

import io
from pathlib import Path

from reportlab.graphics import renderPDF
from reportlab.graphics.barcode.qr import QrCodeWidget
from reportlab.graphics.shapes import Drawing
from reportlab.lib.pagesizes import A6, landscape
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

FONT = "TicketFont"
PAGE = landscape(A6)
# Запасные пути к шрифту с кириллицей — для запуска тестов вне контейнера.
_FALLBACK_FONTS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "/Library/Fonts/Arial Unicode.ttf",
)


def _register_font(font_path: str) -> None:
    # Регистрация — один раз на процесс пула: процессы живут долго и рендерят
    # тысячи билетов, а разбор TTF-файла заметно дороже самого рендеринга.
    if FONT in pdfmetrics.getRegisteredFontNames():
        return
    for candidate in (font_path, *_FALLBACK_FONTS):
        if Path(candidate).exists():
            pdfmetrics.registerFont(TTFont(FONT, candidate))
            return
    raise FileNotFoundError(f"Нет шрифта с кириллицей: {font_path}")


def render_tickets_pdf(order: dict, tickets: list[dict], font_path: str) -> bytes:
    """Один PDF на заказ, страница на билет.

    order: order_id, title, venue, starts_at (строка); tickets: code, section, row,
    seat, price (строка).
    """
    _register_font(font_path)
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=PAGE)
    pdf.setTitle(f"Билеты, заказ №{order['order_id']}")
    width, height = PAGE

    for ticket in tickets:
        pdf.setFont(FONT, 16)
        pdf.drawString(20, height - 36, order["title"])
        pdf.setFont(FONT, 10)
        pdf.drawString(20, height - 54, f"{order['venue']} · {order['starts_at']}")

        pdf.setFont(FONT, 13)
        pdf.drawString(20, height - 100, f"Сектор {ticket['section']}")
        pdf.drawString(20, height - 120, f"Ряд {ticket['row']}, место {ticket['seat']}")
        pdf.setFont(FONT, 10)
        pdf.drawString(20, height - 142, f"Цена: {ticket['price']}")
        pdf.drawString(20, 28, f"Заказ №{order['order_id']}")
        pdf.drawString(20, 14, f"Код: {ticket['code']}")

        # QR кодирует только случайный код билета: на входе контролёр сверяет его с
        # базой. Ни данных покупателя, ни id — их нельзя подделать или перебрать.
        size = 120
        drawing = Drawing(size, size)
        drawing.add(QrCodeWidget(f"ticket-rush:{ticket['code']}", barWidth=size, barHeight=size))
        renderPDF.draw(drawing, pdf, width - size - 16, (height - size) / 2)
        pdf.showPage()

    pdf.save()
    return buffer.getvalue()
