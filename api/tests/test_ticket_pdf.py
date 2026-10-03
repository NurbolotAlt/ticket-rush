from app.config import WorkerSettings
from app.ticket_pdf import render_tickets_pdf


def test_pdf_has_page_per_ticket():
    order = {"order_id": 7, "title": "Rush Live", "venue": "Arena", "starts_at": "01.11.2026"}
    tickets = [
        {"code": f"code{i}", "section": "A", "row": 1, "seat": i, "price": "$120.00"}
        for i in range(1, 4)
    ]

    pdf = render_tickets_pdf(order, tickets, WorkerSettings().pdf_font_path)

    assert pdf.startswith(b"%PDF-")
    pages = pdf.count(b"/Type /Page") - pdf.count(b"/Type /Pages")
    assert pages == 3
