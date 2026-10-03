from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Все параметры — из переменных окружения: один образ и локально, и в CI."""

    # --- PostgreSQL ---
    database_url: str = "postgresql://ticket:ticket@localhost:5432/ticket"
    # Размер пула соединений к БД на ОДИН gunicorn-воркер. Соединение с Postgres
    # дорогое (отдельный процесс на сервере, ~10 МБ памяти), а max_connections
    # у сервера конечен (100). Считать нужно по всей системе:
    # реплики × воркеры × db_pool_max + служебные соединения < max_connections.
    db_pool_min: int = 2
    db_pool_max: int = 10
    # Пул asyncpg при нехватке соединений ЖДЁТ свободное — но по умолчанию вечно.
    # Ограничиваем: не дождались за 5 с — 503, а не зависший запрос.
    db_acquire_timeout_seconds: float = 5.0
    # Потолок на один запрос к БД. Наши запросы — миллисекунды; секунды — авария.
    db_command_timeout_seconds: float = 5.0
    db_connect_timeout_seconds: float = 3.0

    # --- Redis ---
    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 50
    redis_pool_timeout_seconds: float = 5.0
    redis_socket_timeout_seconds: float = 5.0
    redis_connect_timeout_seconds: float = 2.0

    log_level: str = "INFO"

    # --- Сессии ---
    # Ключ подписи cookie. БЕЗ значения по умолчанию: без него приложение не
    # стартует. Дефолтный ключ в коде = ключ, известный всем, кто видел репозиторий,
    # и любой смог бы подделать чужую сессию.
    session_secret: str = Field(min_length=32)
    # Secure-флаг cookie: браузер отправит её только по HTTPS. Выключается только
    # в тестах (ASGI-клиент ходит по http://test).
    session_cookie_secure: bool = True

    # --- Очередь ожидания ---
    # Сколько человек впускать в секунду (не больше) и сколько может покупать
    # одновременно. Для зала на 2000 мест 200 одновременных покупателей — это
    # десятки секунд на распродажу при бронировании по 4 места.
    admit_rate_per_second: int = 50
    admit_capacity: int = 200
    # Сколько живёт пропуск к покупке. Истёк — место уступается следующему.
    pass_ttl_seconds: int = 600
    # Кто не подавал признаков жизни столько секунд (закрыл вкладку), того при
    # подходе очереди пропускают, а не выдают ему пропуск впустую.
    presence_timeout_seconds: int = 60
    admitter_tick_seconds: float = 1.0
    # Фоновый впускатель в каждом воркере. Тесты выключают его и вызывают тики
    # вручную — так результат не зависит от того, успел ли тик пройти.
    admitter_enabled: bool = True
    # Лидерство впускателя: лок живёт столько, продлевается каждый тик. Умер
    # лидер — через leader_ttl другой воркер подхватит впуск.
    leader_ttl_seconds: float = 5.0
    # --- Бронь мест ---
    # Сколько держится бронь: достаточно, чтобы оплатить, и не настолько долго,
    # чтобы передумавший держал места, пока остальные ждут.
    hold_ttl_seconds: int = 300
    # Больше 4 мест в одни руки — меньше простора для перекупщиков.
    max_seats_per_session: int = 4

    # --- Покупка ---
    # На время оплаты бронь продлевается хотя бы на столько: иначе она могла бы
    # истечь посреди платежа, и место ушло бы другому, пока деньги списываются.
    payment_window_seconds: int = 60
    # Заказ, застрявший в pending дольше этого (процесс упал посреди оплаты),
    # фоновая уборка помечает неудачным и возвращает по нему деньги.
    pending_order_timeout_seconds: int = 120
    # Имитация платёжного провайдера: задержка ответа и доля отказов (0..1).
    fake_payment_latency_ms: int = 100
    fake_payment_failure_rate: float = 0.0

    # Сколько раз в минуту одна сессия и один IP могут вставать в очередь.
    join_rate_limit_per_minute: int = 5
    join_rate_limit_per_ip_per_minute: int = 60


class SeedSettings(BaseSettings):
    """Настройки наполнения зала (python -m app.seed в контейнере init).

    Отдельно от Settings: процессу init нужны только адрес базы и параметры зала.
    Секретов приложения (ключ сессий) он не получает — у каждого процесса ровно
    те секреты, которые ему нужны.
    """

    database_url: str = "postgresql://ticket:ticket@localhost:5432/ticket"
    seed_event_slug: str = "rush-live-2026"
    seed_sections: str = "A,B,C,D"
    seed_rows: int = 20
    seed_seats_per_row: int = 25
    # Через сколько минут после наполнения откроются продажи. Отрицательное
    # значение — продажи уже открыты (удобно для локальной разработки).
    seed_sale_opens_in_minutes: int = 10


@lru_cache
def get_settings() -> Settings:
    return Settings()
