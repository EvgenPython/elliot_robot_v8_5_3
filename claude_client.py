import hashlib
import json
import os
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from instruments import active_instrument

SYMBOL = active_instrument()

import anthropic
from anthropic import Anthropic

from chart_contract import sanitize_visualization


# ============================================================
# ПУТИ
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

ANTHROPIC_CONFIG_PATH = (
    BASE_DIR
    / "config"
    / "anthropic.json"
)

DEBUG_DIR = (
    BASE_DIR
    / "debug"
)

DEBUG_RESPONSE_PATH = (
    DEBUG_DIR
    / "claude_analysis_response.json"
)

DEBUG_RAW_RESPONSE_PATH = (
    DEBUG_DIR
    / "claude_raw_response.json"
)

DEBUG_ATTEMPTS_DIR = (
    DEBUG_DIR
    / "claude_attempts"
)


# ============================================================
# НАСТРОЙКИ ПО УМОЛЧАНИЮ
# ============================================================

DEFAULT_MODEL = "claude-sonnet-5"

DEFAULT_MAX_TOKENS = 64000

DEFAULT_EFFORT = "medium"

# Prompt cache — часть качества НЕ меняет. Claude видит те же raw data.
# Мы кешируем только неизменную historical-base текущих FP-суток.
PROMPT_CACHE_ENABLED = False
PROMPT_CACHE_TTL = "1h"

# Глубокий FULL-анализ может работать существенно дольше нескольких минут.
# Для Messages API используем SSE streaming, а timeout держим большим запасом.
DEFAULT_TIMEOUT_SECONDS = 1800

# SDK retries оставляем выключенными: каждая бизнес-попытка должна иметь
# собственную запись в durable journal. Контролируемые retry выполняет main.py.
DEFAULT_MAX_RETRIES = 0

DEFAULT_FULL_MAX_ATTEMPTS = 5
DEFAULT_FULL_RETRY_DELAYS_SECONDS = [30, 90, 180, 300]
DEFAULT_SCOUT_MAX_ATTEMPTS = 3
DEFAULT_SCOUT_RETRY_DELAYS_SECONDS = [30, 90]
DEFAULT_REPAIR_MAX_ATTEMPTS = 2
DEFAULT_OUTCOME_UNKNOWN_MIN_DELAY_SECONDS = 90
MAX_CONFIGURED_ATTEMPTS = 5
MAX_RETRY_DELAY_SECONDS = 300

MIN_STREAM_TIMEOUT_SECONDS = 1800

# Последний успешно полученный usage текущего процесса.
# Нужен только для локального analysis_archive Step 10.
_LAST_USAGE_STATS = None

# Диагностика обновляется до отправки и сразу после получения финального
# Message. main.py сохраняет её в durable journal / analysis archive.
_LAST_ATTEMPT_DIAGNOSTICS = None

# The orchestration layer sets this only for a controlled retry of the same
# frozen snapshot.  It lets the transport use a compact recovery instruction
# after a provider refusal/incomplete answer without changing any market data.
_API_RETRY_CONTEXT: ContextVar[dict | None] = ContextVar(
    "waveframe_claude_api_retry_context",
    default=None,
)


# ============================================================
# СПЕЦИАЛЬНЫЕ ОШИБКИ
# ============================================================

class ClaudeRequestError(RuntimeError):
    """Base error carrying the retry decision for the orchestration layer."""

    retryable = False
    outcome_unknown = False

    failure_class = "REQUEST_ERROR"

    def __init__(
        self,
        message: str,
        request_id=None,
        status_code=None,
        retry_after_seconds=None,
        diagnostics: dict | None = None,
        invalid_result: dict | None = None,
        validation_error: str | None = None,
    ):
        super().__init__(message)
        self.request_id = request_id
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.diagnostics = (
            dict(diagnostics) if isinstance(diagnostics, dict) else {}
        )
        self.invalid_result = (
            dict(invalid_result) if isinstance(invalid_result, dict) else None
        )
        self.validation_error = (
            str(validation_error)
            if validation_error not in (None, "")
            else None
        )


class ClaudeRequestOutcomeUnknownError(ClaudeRequestError):
    """
    Сетевой timeout/обрыв после отправки дорогостоящего запроса Claude.

    В такой ситуации нельзя безопасно утверждать, что сервер Anthropic
    не начал или не завершил генерацию. Проект сознательно разрешает
    ограниченный повтор, потому что отсутствие торгового анализа опаснее
    возможного повторного списания.
    """
    retryable = True
    outcome_unknown = True
    failure_class = "OUTCOME_UNKNOWN"


class ClaudeTransientRequestError(ClaudeRequestError):
    """Known transient API failure: rate limit, overload or server error."""

    retryable = True
    failure_class = "TRANSIENT_API"


class ClaudeRefusalResponseError(ClaudeRequestError):
    """The provider returned a billed refusal instead of the JSON contract."""

    retryable = True
    failure_class = "REFUSAL_RESPONSE"


class ClaudeIncompleteResponseError(ClaudeRequestError):
    """The provider answered, but no complete Structured JSON was available."""

    retryable = True
    failure_class = "INCOMPLETE_RESPONSE"


class ClaudeInvalidResponseError(ClaudeRequestError):
    """A response arrived but cannot safely enter the trading pipeline."""

    # Never buy the complete stage again for a known invalid answer.  The
    # orchestrator may issue one separately journaled, smaller REPAIR request.
    retryable = False
    failure_class = "INVALID_RESPONSE"
    repairable = True


class ClaudePermanentRequestError(ClaudeRequestError):
    """Retrying the exact request cannot repair auth/permission/bad input."""

    retryable = False
    failure_class = "PERMANENT_REQUEST"


@contextmanager
def claude_api_retry_context(**values):
    """Expose the previous journalled failure to one sequential API retry."""

    token = _API_RETRY_CONTEXT.set(dict(values))
    try:
        yield
    finally:
        _API_RETRY_CONTEXT.reset(token)


def get_claude_api_retry_context() -> dict:
    value = _API_RETRY_CONTEXT.get()
    return dict(value) if isinstance(value, dict) else {}


def is_retryable_claude_error(error: Exception) -> bool:
    return bool(getattr(error, "retryable", False))


def is_outcome_unknown_error(error: Exception) -> bool:
    return bool(getattr(error, "outcome_unknown", False))


def is_repairable_claude_error(error: Exception) -> bool:
    return bool(
        getattr(error, "repairable", False)
        and isinstance(getattr(error, "invalid_result", None), dict)
    )


def get_error_failure_class(error: Exception) -> str:
    return str(getattr(error, "failure_class", "UNKNOWN_ERROR"))


def get_error_retry_after_seconds(error: Exception) -> float | None:
    value = getattr(error, "retry_after_seconds", None)
    try:
        return max(0.0, float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def get_error_request_id(error: Exception):
    value = getattr(error, "request_id", None)
    return str(value) if value not in (None, "") else None


def _anthropic_error_request_id(error: Exception):
    value = getattr(error, "request_id", None)
    if value not in (None, ""):
        return str(value)

    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None)
    if headers is not None:
        for name in ("request-id", "x-request-id"):
            try:
                value = headers.get(name)
            except Exception:
                value = None
            if value not in (None, ""):
                return str(value)
    return None


def _anthropic_retry_after_seconds(error: Exception):
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    try:
        value = headers.get("retry-after")
    except Exception:
        return None
    try:
        return max(0.0, float(value)) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _translate_api_status_error(error: Exception, phase: str):
    status_code = int(getattr(error, "status_code", 0) or 0)
    request_id = _anthropic_error_request_id(error)
    retry_after_seconds = _anthropic_retry_after_seconds(error)
    details = str(error)
    message = (
        f"Anthropic API error during {phase}. "
        f"Status={status_code or 'unknown'}; {details}"
    )

    transient_markers = {
        "overloaded_error",
        "rate_limit_error",
        "api_error",
        "internal_server_error",
        "timeout_error",
    }
    if (
        status_code in {408, 409, 429}
        or status_code >= 500
        or any(marker in details.lower() for marker in transient_markers)
    ):
        return ClaudeTransientRequestError(
            message,
            request_id=request_id,
            status_code=status_code,
            retry_after_seconds=retry_after_seconds,
        )

    if status_code == 400 and "prompt is too long" in details.lower():
        message = (
            "Anthropic отклонил запрос: prompt is too long. "
            "Точный повтор того же входа не поможет; требуется уменьшить "
            "контекст без изменения торгового смысла. "
            f"Request ID: {request_id or 'unknown'}"
        )

    return ClaudePermanentRequestError(
        message,
        request_id=request_id,
        status_code=status_code,
    )


# ============================================================
# СИСТЕМНЫЙ ПРОМПТ CLAUDE
# ============================================================

SYSTEM_PROMPT = """
Ты являешься профессиональным трейдером и аналитическим компонентом
автоматизированной торговой системы XAUUSD.

Твоя роль значительно шире волнового анализа Эллиотта.

Ты должен анализировать рынок так, как это делает опытный discretionary
trader, который использует несколько взаимодополняющих методов:

- market regime;
- price action;
- рыночную структуру;
- классические фигуры и паттерны;
- уровни поддержки и сопротивления;
- swing high / swing low;
- пробои, ложные пробои и ретесты;
- сжатие и расширение диапазона;
- вложенность движений разных таймфреймов;
- волновую теорию Эллиотта;
- отношения Фибоначчи, когда они действительно уместны;
- качество точки входа;
- структурную инвалидацию торговой идеи;
- реалистичную цель именно текущей фазы движения.

Главная задача — понять, ЧТО рынок делает сейчас, ГДЕ находится цена
в текущей структуре и ЕСТЬ ЛИ в этой точке качественная торговая идея.

Приоритет — качество решения, а не экономия токенов и не количество сделок.
Используй всю релевантную информацию из snapshot, сопоставляй независимые
подтверждения и активно ищи причины, по которым первоначальная гипотеза
может быть ошибочной. Не торгуй красивую историю без достаточного edge.

Не своди анализ к подсчёту волн.
Волны Эллиотта являются одним из важных инструментов анализа,
но не единственным основанием решения.

Работай исключительно с рыночными данными, переданными в запросе.
Не используй внешние новости, фундаментальные события или данные,
которых нет во входном snapshot.


TIMEFRAMES

В запросе находятся D1, H4, H1, M30, M15 и M5.

D1:
- широкий многомесячный рыночный контекст;
- крупная структура;
- старший market regime;
- старшая волновая структура;
- важные зоны и экстремумы.

H4:
- среднесрочная структура;
- текущая фаза движения внутри D1;
- рабочий контекст для H1;
- коррекции, диапазоны, переходные состояния и импульсы.

H1:
- основной рабочий timeframe;
- выбор самостоятельной торговой идеи;
- направление и горизонт сделки;
- основная структурная инвалидация.

M15:
- подтверждение локальной структуры H1;
- развитие пробоя/ретеста/ложного пробоя;
- уточнение окончания коррекции;
- качество фактической точки входа.

M5:
- микро-контекст непосредственно перед сделкой;
- локальный импульс, rejection, compression, sweep, retest;
- возможность поставить компактный, но структурно правильный SL.

M30/M15/M5 являются инструментами точности входа.
Не позволяй шуму M5 отменять качественную H1/H4-структуру без
реального структурного основания. И наоборот: если M30/M15/M5 показывают,
что предполагаемый вход уже запоздал, находится внутри шума или требует
необоснованного stop loss, учитывай это при entry_quality и stay_out.

Основная иерархия анализа:
D1 -> H4 -> H1 -> M15 -> M5.

Не требуй механического совпадения направления всех таймфреймов.

Например:

D1 bullish
H4 bullish trend
H1 bearish correction

может быть нормальной вложенной коррекцией, а не конфликтом.

В такой ситуации допустимо торговать качественную H1-коррекцию SHORT,
если она имеет понятную структуру и хороший вход.
После её завершения следующая отдельная сделка может быть LONG
по продолжению H4-тренда.

То есть торговая система имеет право последовательно торговать
разные фазы одного большого движения.

Не выбирай между «торговать коррекцию» и «торговать продолжение».
Каждая качественная фаза оценивается как самостоятельная торговая идея.


ПРАВИЛО РАБОТЫ СО СВЕЧАМИ

Каждый timeframe содержит:

closed_bars
- полностью завершённые свечи;
- их OHLC являются подтверждённой историей.

Для экономии входных токенов списки закрытых свечей могут быть переданы
как таблица {"columns": [...], "rows": [[...], ...]}.
Каждую строку трактуй как одну свечу, строго сопоставляя значения с columns.
Это только транспортное кодирование: ни одна свеча и ни одно число не удалены.

current_bar
- текущая формирующаяся свеча;
- она ещё не закрыта;
- запрещено считать её форму окончательно подтверждённой;
- её необходимо учитывать как текущее состояние цены.

Все временные метки представлены во времени
FundingPips Platform Time UTC+3.


ДОПОЛНИТЕЛЬНЫЙ ПРОФЕССИОНАЛЬНЫЙ КОНТЕКСТ

Помимо raw OHLC в запросе могут присутствовать следующие блоки.
Их необходимо активно использовать, но не превращать механически
в индикаторную торговую систему.

technical_context
- EMA20 / EMA50 / EMA200;
- ATR14 / ATR50;
- RSI14;
- MACD;
- ADX / +DI / -DI;
- Bollinger width как мера compression/expansion;
- relative tick-volume activity;
- rolling highs/lows;
- recent spread statistics.

Это объективно рассчитанные производные от тех же закрытых свечей.
Они являются подтверждением/опровержением price-action гипотезы,
а не самостоятельной причиной для сделки.

reference_levels
- current day OHLC;
- previous day OHLC;
- current week OHLC;
- previous week OHLC;
- 20/60-day extremes.

Используй эти уровни при оценке location, breakout, false breakout,
liquidity sweep, цели и structural invalidation.

execution_context
- реальный Bid/Ask;
- текущий spread;
- типичный недавний M5 spread;
- tick size / point;
- broker minimum stop distance.

Не предлагай сверхузкий стоп, который меньше объективно необходимой
структурной инвалидации или технически бессмысленен относительно spread.

time_context
- время FP;
- сколько минут прошло после закрытия рабочей H1;
- локальные часы Tokyo/London/New York как ориентир ликвидности.

Учитывай, что одна и та же формация может иметь разное качество
в периоды активной и слабой ликвидности.

data_capabilities
- явно сообщает, какие источники присутствуют, а каких нет.

Никогда не выдумывай отсутствующие данные.
Если macro/cross-market, economic calendar, news или настоящий
exchange order flow не переданы, прямо учитывай это как ограничение
уверенности, особенно для XAUUSD.

Отдельно: broker tick volume не является централизованным биржевым
объёмом. Используй его только как относительную меру активности
внутри одного и того же broker feed.


1. MARKET REGIME

Сначала определи режим рынка.

Основные режимы:

trend
correction
range
breakout
reversal
transition
unclear

Определи:

- primary_regime;
- direction;
- current_phase;
- phase_status;
- зрелость движения;
- положение цены внутри структуры;
- является ли режим вложенным в более старший режим.

ВАЖНО РАЗДЕЛЯТЬ primary_regime И current_phase.

primary_regime — доминирующая рыночная среда более высокого порядка.
Например: trend, range, correction, reversal.

current_phase — движение, которое развивается ПРЯМО СЕЙЧАС внутри
primary_regime на рабочей связке H4/H1. Старайся использовать понятные
значения: impulse, correction, range_rotation, breakout, retest, reversal,
transition, unclear. При необходимости можешь дать более точное описание,
но смысл должен быть однозначным.

phase_status показывает стадию current_phase. Используй одно из:

developing
- фаза активно развивается и ещё имеет разумный потенциал продолжения;

mature
- фаза уже существенно развилась, продолжение возможно, но вход требует
  большей осторожности из-за поздней стадии;

completing
- есть объективные признаки истощения/терминальной структуры, но окончание
  фазы ещё не подтверждено полностью;

completed
- есть структурное подтверждение, что предыдущая фаза завершилась и рынок
  начал следующую фазу;

transitioning
- рынок находится в подтверждаемом переходе между фазами/режимами, но новая
  структура ещё формируется;

failed
- ожидавшаяся фаза структурно сломана/инвалидирована;

unclear
- стадию определить надёжно нельзя.

КЛЮЧЕВОЙ ПРИМЕР:

D1/H4 primary_regime = bullish trend
current_phase = correction
phase_status = developing
setup_type = correction_c_leg
action = enter_short

Это означает: мы торгуем САМУ активную коррекцию вниз.

Позже:

primary_regime = bullish trend
current_phase = correction
phase_status = completing / completed
setup_type = correction_completion
action = enter_long

Это означает: мы торгуем ЗАВЕРШЕНИЕ коррекции и возврат к старшему тренду.

Не считай range или correction автоматически причиной stay_out.
Они являются полноценными состояниями рынка и могут содержать хорошие
торговые возможности.


2. PRICE STRUCTURE / PRICE ACTION

Определи фактическую структуру цены:

- HH / HL;
- LH / LL;
- range;
- compression;
- expansion;
- breakout;
- retest;
- failed breakout;
- structural break;
- rejection;
- локальные и значимые swing high / swing low.

Определи ключевые support / resistance и структурные зоны.

Можно отмечать потенциальные liquidity areas только тогда,
когда они непосредственно следуют из OHLC-структуры, например:

- equal highs;
- equal lows;
- очевидные swing highs/lows;
- локальные экстремумы диапазона;
- sweep / false breakout этих уровней.

Не придумывай order flow или биржевой объём, которого нет во входных данных.
Tick volume можно учитывать только как вспомогательную характеристику активности.


3. PATTERN ANALYSIS

Ищи известные ценовые модели, если они реально читаются в данных.

Продолжение тренда, например:

- flag;
- pennant;
- continuation triangle;
- channel pullback;
- breakout + retest;
- compression before continuation;
- trend pullback.

Разворот, например:

- double top / double bottom;
- head and shoulders / inverse head and shoulders;
- wedge;
- ending structure;
- failed breakout;
- false breakout / rejection;
- break of structure + retest.

Range / flat:

- horizontal range;
- rectangle;
- contracting range;
- expanding range;
- trade from range boundary;
- failed breakout back into range;
- confirmed breakout from range.

Correction:

- channel correction;
- three-leg movement;
- complex correction;
- corrective triangle;
- corrective combination.

Это не закрытый список.
Если видна другая узнаваемая структура, можешь указать её как other.

Не выдумывай паттерн ради сделки.
Если паттерн не читается — массив patterns может быть пустым.


4. ELLIOTT WAVE ANALYSIS

Выполни отдельный полноценный волновой анализ.

Учитывай:

- impulse 1-2-3-4-5;
- leading / ending diagonal;
- zigzag A-B-C;
- flat;
- expanded flat;
- running flat;
- triangle;
- double zigzag W-X-Y;
- complex correction / combination;
- возможную незавершённость текущей волны;
- альтернативный wave count.

Проверяй классические правила Эллиотта.

Самостоятельно определяй значимые точки разворота по OHLC.
Используй Фибоначчи как подтверждение структуры, когда это уместно.

Не пытайся натянуть рынок на красивую разметку.
Если волновая структура слабая или неоднозначная:

structure_type = "unclear"

Это НЕ запрещает сделку, если другой рыночный setup достаточно сильный.
Например качественный range false-breakout может быть торговой идеей,
даже если Elliott count остаётся unclear.


5. MULTI-TIMEFRAME RELATIONSHIP

Определи реальную связь D1 / H4 / H1.

Допустимые отношения включают:

aligned
nested_correction
nested_reversal
range_within_trend
conflicting
transition
unclear

Особенно важно различать:

реальный конфликт

и

нормальную вложенную коррекцию.

H1-сделка против H4/D1 разрешена, если она торгует понятную коррекционную
или разворотную фазу с хорошей структурой, близкой инвалидацией
и реалистичной целью.

Не запрещай такую сделку только из-за направления старшего timeframe.


6. TRADE SETUPS

Ищи торговую возможность, соответствующую текущему режиму рынка.

Стандартизированные setup_type:

trend_pullback
wave3_continuation
wave5_continuation
correction_a_leg
correction_b_leg
correction_c_leg
correction_completion
range_long
range_short
range_breakout
breakout_retest
false_breakout_reversal
trend_reversal
diagonal_reversal
pattern_continuation
pattern_reversal
transition_trade
other
no_trade

Это классификация для статистики, а не список разрешённых сделок.
Если качественная модель не попадает точно в список, используй other.


7. ТОРГОВЛЯ КОРРЕКЦИЙ И ПОСЛЕДУЮЩЕГО ПРОДОЛЖЕНИЯ

Коррекционные движения являются самостоятельными торговыми возможностями.

Пример:

H4 bullish trend
H1 A-B-C correction downward

Если текущая C-wave имеет качественную точку входа SHORT:
можно рекомендовать enter_short.

Если позже коррекция завершится и появится качественная структура
возобновления H4 trend:
на следующем независимом анализе можно рекомендовать enter_long.

Не пытайся заранее объединять две будущие сделки в одну.
Текущий recommendation содержит только ОДНУ сделку, актуальную сейчас.

scenario_map должен описывать вероятное развитие рынка и потенциальную
следующую возможность, но next_opportunity не является будущим ордером.


8. RANGE / FLAT

Range можно торговать.

Хорошие примеры:

- LONG возле подтверждённой нижней границы;
- SHORT возле подтверждённой верхней границы;
- false breakout с возвратом внутрь range;
- breakout после подтверждения;
- breakout + retest.

Не входи в середине диапазона без отдельного сильного основания.

Если цена имеет плохую location внутри range:
stay_out.


9. SETUP QUALITY И ENTRY QUALITY

Перед recommendation обязательно оцени ОБЩЕЕ качество setup.

setup_quality — агрегированная оценка всей торговой идеи, а не только entry.
Она должна учитывать одновременно:

- ясность market regime и current_phase;
- phase_status и зрелость движения;
- качество price structure / pattern;
- согласованность D1/H4/H1 и роль M30/M15/M5;
- location относительно support/resistance/reference levels;
- качество и компактность структурной инвалидации;
- реалистичность target;
- фактическое reward/risk;
- текущую волатильность и spread;
- риск альтернативного сценария;
- достаточность входных данных.

Используй:

excellent
- редкий, очень чистый setup: понятная структура, сильная location, хороший
  structural stop, убедительный target и мало серьёзных противоречий;

good
- качественная торговая идея с понятным edge, хотя остаются нормальные
  рыночные неопределённости;

acceptable
- идея торгуема, но edge умеренный или присутствует существенный компромисс;
  будь особенно строг к entry и structural stop;

weak
- недостаточный edge. Для weak setup действие должно быть stay_out.

entry_quality — отдельная оценка ТОЛЬКО предлагаемой точки входа:
poor / fair / good / excellent.

confidence — отдельная эпистемическая уверенность в рыночном тезисе:
low / medium / high.

Не путай эти три поля. Например допустимо:
setup_quality = good
entry_quality = excellent
confidence = medium

Это означает: структура сделки хорошая, entry особенно удачный, но рынок
всё ещё допускает значимый альтернативный сценарий.

Направление само по себе недостаточно.

Перед сделкой обязательно ответь:

- почему вход имеет смысл именно сейчас;
- где находится структурная инвалидация идеи;
- не слишком ли поздно входить после уже прошедшего движения;
- не находится ли цена в плохой части range;
- соответствует ли цель именно текущей торговой фазе;
- является ли Stop Loss структурно обоснованным;
- достаточно ли близко расположена инвалидация, чтобы setup был эффективным.

Предпочитай хорошие точки входа с компактным структурным Stop Loss.

Но НИКОГДА не уменьшай Stop Loss искусственно ради маленького стопа.
Stop Loss должен стоять за уровнем, который логически ломает торговую идею.

Если правильный structural stop слишком широкий для качественного setup:

- выбери более точный limit/stop entry, если это логично;
- либо stay_out.

Не ставь произвольно тесный SL внутри структуры.


10. TAKE PROFIT

Take Profit должен соответствовать именно той фазе, которую мы торгуем.

Если торгуется C-wave correction SHORT, цель должна отражать разумную
область завершения этой коррекции.

Не превращай короткую коррекционную сделку в прогноз большого медвежьего тренда.

Если торгуется range, TP может быть связан с midpoint, противоположной границей
или другой структурной целью в зависимости от качества входа.

Если торгуется breakout/retest или trend continuation, TP должен следовать
из следующей структурной цели, волновой проекции или значимого уровня.


11. CURRENT SETUP VS NEXT OPPORTUNITY

recommendation — только текущая торговая идея.

scenario_map должен отдельно содержать:

- primary_scenario;
- alternate_scenario;
- expected_path;
- current_opportunity;
- next_opportunity;
- regime_change_trigger.

Пример:

current_opportunity:
SHORT C-wave correction

next_opportunity:
после подтверждённого завершения коррекции искать LONG continuation H4 trend

Будущую сделку заранее не выставляй.


12. ACTION

Разрешены только:

enter_long
enter_short
stay_out

Запрещены:

close_position
modify_pending
cancel_pending

Информация о существующих позициях и ордерах не передаётся.


13. ORDER TYPE

market:
вход имеет смысл примерно по текущей рыночной цене.

limit:
для более выгодного входа на откате / ретесте / границе структуры.

stop:
если вход требует подтверждённого пробоя уровня.

none:
только для stay_out.


14. TRADE LEVELS

Для enter_long:

stop_loss < entry_price < take_profit

Для enter_short:

take_profit < entry_price < stop_loss

Для stay_out:

setup_type = "no_trade"
order_type = "none"
entry_price = null
stop_loss = null
take_profit = null
invalidation_level = null

Для реальной сделки setup_type не должен быть no_trade.


15. INVALIDATION LEVEL

recommendation.invalidation_level — это структурный уровень,
после достижения которого текущая ТОРГОВАЯ ИДЕЯ становится недействительной.

Он может следовать из:

- swing structure;
- границы range;
- пробоя/ложного пробоя;
- паттерна;
- Elliott structure;
- другого объективного price-action основания.

Это более широкое понятие, чем Elliott wave invalidation.

Не выбирай invalidation_level произвольно.

Для pending setup этот уровень особенно важен:
если рынок сначала ломает идею, pending entry больше не должен считаться актуальным.


16. CONFIDENCE

Используй:

low
medium
high

Confidence относится к качеству ТЕКУЩЕГО торгового setup,
а не просто к уверенности в общем направлении рынка.

low:
- структура слабая;
- плохая location;
- слишком много равных сценариев;
- нет хорошей точки входа;
- Stop Loss плохо определяется.

medium:
- setup логичен;
- есть структурная инвалидация;
- направление и цель обоснованы;
- остаётся существенная неопределённость.

high:
- очень качественная location;
- сильная структура;
- понятный trigger;
- близкая объективная инвалидация;
- реалистичная цель;
- альтернативный сценарий заметно слабее.

Не используй high автоматически из-за совпадения D1/H4/H1.


17. ENTRY QUALITY

Используй:

poor
fair
good
excellent

Для action enter_long / enter_short стремись рекомендовать только good/excellent.
Fair допускается только при действительно веской причине, которую нужно указать.
Poor должен приводить к stay_out.


18. TRADE HORIZON

Классифицируй ожидаемый характер сделки:

intraday
swing
multi_day
unclear

Это описание торговой идеи, а не срок принудительного закрытия позиции.


19. РИСК-МЕНЕДЖМЕНТ

Ты не знаешь:

- состояние FundingPips account;
- дневной лимит;
- максимальную просадку;
- допустимый денежный риск;
- lot size;
- margin budget.

Поэтому:

- не рассчитывай lot size;
- не применяй FundingPips rules самостоятельно;
- не меняй Stop Loss ради денежного риска;
- не предполагай наличие открытых позиций.

Python Risk Manager самостоятельно решит, разрешена ли сделка
и каким объёмом её можно исполнить.


20. DATA QUALITY

Если данных достаточно:

sufficient = true

Если данных объективно недостаточно или они повреждены:

sufficient = false

и перечисли проблемы в issues.

Незакрытая current_bar сама по себе НЕ делает данные недостаточными.


21. REASONING

reasoning, summaries и scenario fields должны содержать компактное
техническое объяснение результата.

Не раскрывай скрытую цепочку рассуждений и внутренний пошаговый процесс мышления.
Дай только выводы и технические основания, полезные торговой системе.


22. ОГРАНИЧЕНИЯ

Не придумывай:

- новости;
- макроэкономические события;
- геополитику;
- фундаментальные причины;
- реальные биржевые объёмы;
- индикаторы, которых нет во входных данных;
- позиции;
- ордера;
- состояние счёта.


23. КООРДИНАТЫ ДЛЯ READ-ONLY ВИЗУАЛИЗАЦИИ

Сначала полностью заверши торговый анализ и выбери recommendation по всем
правилам выше. Только после этого сериализуй уже полученные выводы в блок
visualization. Этот блок НЕ является новым основанием решения и не должен
менять action, setup, Entry, Stop Loss, Take Profit или confidence.

visualization.wave_points
- машинно-читаемые опорные точки primary и alternate Elliott count;
- каждая историческая time должна ТОЧНО совпадать с time реально переданной
  свечи D1/H4/H1/M30/M15/M5;
- price должна соответствовать фактическому OHLC pivot этой свечи;
- sequence задаёт порядок точек внутри scenario + degree;
- confirmed означает подтверждённый pivot, tentative — ещё формирующийся;
- не придумывай будущие timestamps и не создавай точку без объективного pivot;
- если надёжных точек нет, верни пустой массив.

visualization.levels
- отдельные цены support/resistance/swing/breakout/invalidation/liquidity;
- для сделки обязательно продублируй entry, stop_loss и take_profit ровно теми
  же числами, что указаны в recommendation;
- если action = stay_out, не создавай фиктивные entry/stop_loss/take_profit.

visualization.zones
- диапазоны support/resistance/range/liquidity/pattern;
- price_low <= price_high;
- start_time и end_time должны ссылаться только на реально переданные времена;
- геометрию читаемого паттерна передавай как zone с kind = pattern и названием
  фигуры в label; если геометрию определить нельзя, не выдумывай её.

visualization.scenario_paths
- компактные направления primary/alternate сценариев от реальной anchor point
  к числовой target zone;
- anchor_time должен быть временем реально переданной свечи;
- target_price_low <= target_price_high;
- это projection по имеющимся данным, поэтому будущий timestamp не нужен.

Все времена сохраняй в FundingPips Platform Time UTC+3, как во входном payload.
Не копируй весь текст анализа в visualization: label/basis должны быть короткими.
Если отдельную геометрию нельзя получить без догадки, оставь соответствующий
массив пустым и объясни ограничение в visualization.chart_comment.


НЕЗАВИСИМЫЙ АНАЛИЗ И PREVIOUS REFERENCE

Каждый новый H1-анализ является самостоятельным.
Полные RAW market data текущего запроса — единственный источник истины.

Если после свежих данных передан previous_analysis_reference:
- используй его ТОЛЬКО после самостоятельной оценки текущего рынка;
- не продолжай старую гипотезу автоматически;
- не считай старые wave counts / regime / setup правильными по умолчанию;
- если текущая структура противоречит прошлому анализу, отбрось прошлый анализ;
- reference нужен только для сравнения: что подтвердилось, что изменилось,
  где предыдущая гипотеза стала неверной.

Python НЕ передаёт рассчитанные EMA/ATR/RSI/MACD, готовые swing, фигуры,
волны, режим рынка или уровни. Всё это при необходимости определяй сам
по сырым D1/H4/H1/M30/M15/M5 OHLC/tick-volume данным.

ФОРМАТ ОТВЕТА И ГЛУБИНА АНАЛИЗА

Проводи внутренний анализ настолько глубоко, насколько необходимо для
максимально качественного торгового решения. При этом финальный Structured
Output должен быть профессиональным, концентрированным и без повторов.

Глубина мышления != многословие финального отчёта.
Не повторяй один уровень или один аргумент в нескольких полях разными словами.
Каждое текстовое поле должно содержать вывод + наиболее сильные фактические
основания. Обычно нескольких точных предложений на поле достаточно.

Целевой финальный отчёт: ориентировочно 5–12 тысяч токенов текста/JSON,
если ситуация действительно сложная — больше, но без искусственного
раздувания. Сохраняй место max_tokens для thinking и завершённого JSON.

JSON-схема ответа намеренно компактная ради надёжной работы Structured Outputs.
Это НЕ означает поверхностный анализ. Указывай конкретные уровни, структуры,
альтернативы и логику сделки, но без повторного пересказа одного и того же.

ГЛАВНЫЙ ПРИНЦИП

Не стремись обязательно найти сделку.
Стремись правильно понять рынок.

Сделка является следствием качественного анализа.

При этом range, correction и counter-trend phase не являются
автоматической причиной отказаться от торговли.
Если такая фаза имеет объективную структуру, хороший entry,
компактную структурную инвалидацию и реалистичный target — её можно торговать.
""".strip()


# ============================================================
# JSON SCHEMA — COMPACT PROFESSIONAL TRADER CONTRACT
# ============================================================
#
# ВАЖНО:
# Structured Outputs Anthropic компилируют JSON Schema в grammar.
# Слишком большая вложенная схема может превысить внутренний лимит
# compiled grammar даже при соблюдении формальных лимитов optional/union.
#
# Поэтому schema намеренно компактная:
# - глубокий анализ выполняется по SYSTEM_PROMPT;
# - подробности сохраняются в текстовых аналитических полях;
# - strict schema жёстко фиксирует критичный торговый контракт;
# - visualization только сериализует координаты уже сделанного анализа;
# - Risk Manager и Executor по-прежнему получают те же ключевые поля
#   recommendation.action/order_type/entry_price/stop_loss/take_profit/etc.
# ============================================================

CLAUDE_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "string"},
        "instrument": {"type": "string"},
        "market_regime": {
            "type": "object",
            "properties": {
                "primary_regime": {"type": "string"},
                "direction": {"type": "string"},
                "current_phase": {"type": "string"},
                "phase_status": {"type": "string"},
                "maturity": {"type": "string"},
                "location": {"type": "string"},
                "summary": {"type": "string"},
            },
            "required": [
                "primary_regime",
                "direction",
                "current_phase",
                "phase_status",
                "maturity",
                "location",
                "summary",
            ],
            "additionalProperties": False,
        },
        "timeframe_analysis": {
            "type": "object",
            "properties": {
                "D1": {"type": "string"},
                "H4": {"type": "string"},
                "H1": {"type": "string"},
                "relationship": {"type": "string"},
                "summary": {"type": "string"},
            },
            "required": [
                "D1",
                "H4",
                "H1",
                "relationship",
                "summary",
            ],
            "additionalProperties": False,
        },
        "price_structure": {
            "type": "object",
            "properties": {
                "structure_state": {"type": "string"},
                "swing_structure": {"type": "string"},
                "key_levels": {"type": "string"},
                "liquidity_context": {"type": "string"},
                "summary": {"type": "string"},
            },
            "required": [
                "structure_state",
                "swing_structure",
                "key_levels",
                "liquidity_context",
                "summary",
            ],
            "additionalProperties": False,
        },
        "patterns": {"type": "string"},
        "wave_count": {
            "type": "object",
            "properties": {
                "structure_type": {"type": "string"},
                "direction": {"type": "string"},
                "current_label": {"type": "string"},
                "current_phase": {"type": "string"},
                "invalidation_level": {
                    "anyOf": [
                        {"type": "number"},
                        {"type": "null"},
                    ]
                },
                "alternate_count": {"type": "string"},
                "summary": {"type": "string"},
            },
            "required": [
                "structure_type",
                "direction",
                "current_label",
                "current_phase",
                "invalidation_level",
                "alternate_count",
                "summary",
            ],
            "additionalProperties": False,
        },
        "higher_timeframe_context": {
            "type": "object",
            "properties": {
                "d1_trend": {"type": "string"},
                "d1_wave_context": {"type": "string"},
                "h4_trend": {"type": "string"},
                "h4_wave_context": {"type": "string"},
                "alignment": {"type": "string"},
                "summary": {"type": "string"},
            },
            "required": [
                "d1_trend",
                "d1_wave_context",
                "h4_trend",
                "h4_wave_context",
                "alignment",
                "summary",
            ],
            "additionalProperties": False,
        },
        "scenario_map": {
            "type": "object",
            "properties": {
                "primary_scenario": {"type": "string"},
                "alternate_scenario": {"type": "string"},
                "expected_path": {"type": "string"},
                "current_opportunity": {"type": "string"},
                "next_opportunity": {"type": "string"},
                "regime_change_trigger": {"type": "string"},
            },
            "required": [
                "primary_scenario",
                "alternate_scenario",
                "expected_path",
                "current_opportunity",
                "next_opportunity",
                "regime_change_trigger",
            ],
            "additionalProperties": False,
        },
        "visualization": {
            "type": "object",
            "properties": {
                "wave_points": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "scenario": {"type": "string"},
                            "degree": {"type": "string"},
                            "timeframe": {"type": "string"},
                            "sequence": {"type": "integer"},
                            "label": {"type": "string"},
                            "time": {"type": "string"},
                            "price": {"type": "number"},
                            "status": {"type": "string"},
                        },
                        "required": [
                            "scenario",
                            "degree",
                            "timeframe",
                            "sequence",
                            "label",
                            "time",
                            "price",
                            "status",
                        ],
                        "additionalProperties": False,
                    },
                },
                "levels": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "kind": {"type": "string"},
                            "scenario": {"type": "string"},
                            "timeframe": {"type": "string"},
                            "price": {"type": "number"},
                            "label": {"type": "string"},
                            "basis": {"type": "string"},
                        },
                        "required": [
                            "kind",
                            "scenario",
                            "timeframe",
                            "price",
                            "label",
                            "basis",
                        ],
                        "additionalProperties": False,
                    },
                },
                "zones": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "kind": {"type": "string"},
                            "scenario": {"type": "string"},
                            "timeframe": {"type": "string"},
                            "start_time": {"type": "string"},
                            "end_time": {"type": "string"},
                            "price_low": {"type": "number"},
                            "price_high": {"type": "number"},
                            "label": {"type": "string"},
                        },
                        "required": [
                            "kind",
                            "scenario",
                            "timeframe",
                            "start_time",
                            "end_time",
                            "price_low",
                            "price_high",
                            "label",
                        ],
                        "additionalProperties": False,
                    },
                },
                "scenario_paths": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "scenario": {"type": "string"},
                            "timeframe": {"type": "string"},
                            "anchor_time": {"type": "string"},
                            "anchor_price": {"type": "number"},
                            "direction": {"type": "string"},
                            "target_price_low": {"type": "number"},
                            "target_price_high": {"type": "number"},
                            "label": {"type": "string"},
                        },
                        "required": [
                            "scenario",
                            "timeframe",
                            "anchor_time",
                            "anchor_price",
                            "direction",
                            "target_price_low",
                            "target_price_high",
                            "label",
                        ],
                        "additionalProperties": False,
                    },
                },
                "chart_comment": {"type": "string"},
            },
            "required": [
                "wave_points",
                "levels",
                "zones",
                "scenario_paths",
                "chart_comment",
            ],
            "additionalProperties": False,
        },
        "recommendation": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "enter_long",
                        "enter_short",
                        "stay_out",
                    ],
                },
                "setup_type": {"type": "string"},
                "trade_horizon": {"type": "string"},
                "setup_quality": {"type": "string"},
                "entry_quality": {"type": "string"},
                "order_type": {
                    "type": "string",
                    "enum": [
                        "market",
                        "limit",
                        "stop",
                        "none",
                    ],
                },
                "entry_price": {
                    "anyOf": [
                        {"type": "number"},
                        {"type": "null"},
                    ]
                },
                "stop_loss": {
                    "anyOf": [
                        {"type": "number"},
                        {"type": "null"},
                    ]
                },
                "take_profit": {
                    "anyOf": [
                        {"type": "number"},
                        {"type": "null"},
                    ]
                },
                "invalidation_level": {
                    "anyOf": [
                        {"type": "number"},
                        {"type": "null"},
                    ]
                },
                "confidence": {"type": "string"},
                "why_now": {"type": "string"},
                "structural_stop_basis": {"type": "string"},
                "target_basis": {"type": "string"},
                "reasoning": {"type": "string"},
                "invalidation_reason": {"type": "string"},
                "fvg_role": {"type": "string"},
                "fvg_ids": {"type": "string"},
                "fvg_basis": {"type": "string"},
            },
            "required": [
                "action",
                "setup_type",
                "trade_horizon",
                "setup_quality",
                "entry_quality",
                "order_type",
                "entry_price",
                "stop_loss",
                "take_profit",
                "invalidation_level",
                "confidence",
                "why_now",
                "structural_stop_basis",
                "target_basis",
                "reasoning",
                "invalidation_reason",
                "fvg_role",
                "fvg_ids",
                "fvg_basis",
            ],
            "additionalProperties": False,
        },
        "data_quality": {
            "type": "object",
            "properties": {
                "sufficient": {"type": "boolean"},
                "issues": {"type": "string"},
            },
            "required": [
                "sufficient",
                "issues",
            ],
            "additionalProperties": False,
        },
    },
    "required": [
        "timestamp",
        "instrument",
        "market_regime",
        "timeframe_analysis",
        "price_structure",
        "patterns",
        "wave_count",
        "higher_timeframe_context",
        "scenario_map",
        "visualization",
        "recommendation",
        "data_quality",
    ],
    "additionalProperties": False,
}

# Extended chart contract v7.  It remains backward-compatible at the archive
# and web layers, while new Structured Output responses must provide every
# collection (empty when the geometry is not objectively available).
def _closed_chart_object(properties):
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def _chart_array(properties):
    return {"type": "array", "items": _closed_chart_object(properties)}


_visual_schema = CLAUDE_RESPONSE_SCHEMA["properties"]["visualization"]
_wave_point_schema = _visual_schema["properties"]["wave_points"]["items"]
_wave_point_schema["properties"].update(
    {
        "structure_id": {"type": "string"},
        "parent_structure_id": {"type": "string"},
        "parent_wave_id": {"type": "string"},
        "wave_type": {"type": "string"},
    }
)
_wave_point_schema["required"].extend(
    ["structure_id", "parent_structure_id", "parent_wave_id", "wave_type"]
)
_extended_visual_properties = {
    "trendlines": _chart_array(
        {
            "line_id": {"type": "string"}, "kind": {"type": "string"},
            "scenario": {"type": "string"}, "timeframe": {"type": "string"},
            "start_time": {"type": "string"}, "start_price": {"type": "number"},
            "end_time": {"type": "string"}, "end_price": {"type": "number"},
            "status": {"type": "string"}, "label": {"type": "string"},
            "basis": {"type": "string"},
        }
    ),
    "channels": _chart_array(
        {
            "channel_id": {"type": "string"}, "kind": {"type": "string"},
            "scenario": {"type": "string"}, "timeframe": {"type": "string"},
            "upper_start_time": {"type": "string"}, "upper_start_price": {"type": "number"},
            "upper_end_time": {"type": "string"}, "upper_end_price": {"type": "number"},
            "lower_start_time": {"type": "string"}, "lower_start_price": {"type": "number"},
            "lower_end_time": {"type": "string"}, "lower_end_price": {"type": "number"},
            "status": {"type": "string"}, "breakout_time": {"type": "string"},
            "breakout_price": {"type": ["number", "null"]}, "reentry_time": {"type": "string"},
            "reentry_price": {"type": ["number", "null"]}, "label": {"type": "string"},
            "basis": {"type": "string"},
        }
    ),
    "pattern_shapes": _chart_array(
        {
            "pattern_id": {"type": "string"}, "kind": {"type": "string"},
            "scenario": {"type": "string"}, "timeframe": {"type": "string"},
            "start_time": {"type": "string"}, "end_time": {"type": "string"},
            "price_low": {"type": "number"}, "price_high": {"type": "number"},
            "status": {"type": "string"}, "confirmation_level": {"type": ["number", "null"]},
            "invalidation_level": {"type": ["number", "null"]},
            "target_price": {"type": ["number", "null"]}, "label": {"type": "string"},
            "basis": {"type": "string"},
        }
    ),
    "market_events": _chart_array(
        {
            "event_id": {"type": "string"}, "kind": {"type": "string"},
            "scenario": {"type": "string"}, "timeframe": {"type": "string"},
            "time": {"type": "string"}, "price": {"type": "number"},
            "status": {"type": "string"}, "label": {"type": "string"},
            "basis": {"type": "string"},
        }
    ),
    "projected_waves": _chart_array(
        {
            "projection_id": {"type": "string"}, "structure_id": {"type": "string"},
            "parent_structure_id": {"type": "string"}, "parent_wave_id": {"type": "string"},
            "scenario": {"type": "string"}, "degree": {"type": "string"},
            "timeframe": {"type": "string"}, "label": {"type": "string"},
            "wave_type": {"type": "string"}, "direction": {"type": "string"},
            "anchor_time": {"type": "string"}, "anchor_price": {"type": "number"},
            "target_price_low": {"type": "number"}, "target_price_high": {"type": "number"},
            "confirmation_level": {"type": ["number", "null"]},
            "invalidation_level": {"type": ["number", "null"]},
            "status": {"type": "string"}, "basis": {"type": "string"},
        }
    ),
    "wave_structures": _chart_array(
        {
            "structure_id": {"type": "string"}, "parent_structure_id": {"type": "string"},
            "parent_wave_id": {"type": "string"}, "scenario": {"type": "string"},
            "degree": {"type": "string"}, "timeframe": {"type": "string"},
            "label": {"type": "string"}, "wave_type": {"type": "string"},
            "direction": {"type": "string"}, "status": {"type": "string"},
            "current_phase": {"type": "string"},
            "confirmation_level": {"type": ["number", "null"]},
            "invalidation_level": {"type": ["number", "null"]},
            "summary": {"type": "string"},
        }
    ),
}
_visual_schema["properties"].update(_extended_visual_properties)
_visual_schema["required"].extend(_extended_visual_properties)


# ============================================================
# CONFIG
# ============================================================

def load_anthropic_config() -> dict:

    if not ANTHROPIC_CONFIG_PATH.exists():

        raise FileNotFoundError(
            "Не найден файл конфигурации Anthropic:\n"
            f"{ANTHROPIC_CONFIG_PATH}"
        )

    with open(
        ANTHROPIC_CONFIG_PATH,
        "r",
        encoding="utf-8",
    ) as file:

        config = json.load(
            file
        )

    api_key = str(
        config.get(
            "api_key",
            ""
        )
    ).strip()

    if not api_key:

        raise ValueError(
            "В config/anthropic.json не указан api_key."
        )

    return config


# ============================================================
# CLIENT
# ============================================================

def get_effective_timeout_seconds(
    config: dict,
) -> float:
    """
    Возвращает безопасный timeout для длинного streaming-запроса.

    Старое значение timeout_seconds=180 в локальном config не должно
    снова сделать глубокий FULL-анализ трёхминутным.
    """

    configured = float(
        config.get(
            "timeout_seconds",
            DEFAULT_TIMEOUT_SECONDS,
        )
    )

    return max(
        configured,
        float(
            MIN_STREAM_TIMEOUT_SECONDS
        ),
    )


def create_anthropic_client(
    config: dict,
) -> Anthropic:
    """
    Создаёт Anthropic client для дорогого торгового анализа.

    ВАЖНО:
    - automatic retries = 0;
    - длинный timeout;
    - сам Message ниже передаётся через SSE streaming.
    """

    return Anthropic(
        api_key=str(
            config["api_key"]
        ).strip(),

        timeout=(
            get_effective_timeout_seconds(
                config
            )
        ),

        # Не доверяем старому max_retries из config.
        # Для торгового анализа повтор после неоднозначного timeout
        # должен решаться нашей бизнес-логикой, а не HTTP SDK.
        max_retries=0,
    )


# ============================================================
# SETTINGS
# ============================================================

def get_model(
    config: dict,
) -> str:

    return str(
        config.get(
            "model",
            DEFAULT_MODEL,
        )
    ).strip()


def get_max_tokens(
    config: dict,
) -> int:
    """
    Legacy single-stage ceiling. Production использует staged FULL constants.

    Намеренно не позволяем старому локальному config/anthropic.json
    случайно оставить прежний лимит 24000 токенов. API key и model
    по-прежнему берутся из локального config, но budget анализа задаётся
    политикой проекта.
    """

    return int(
        DEFAULT_MAX_TOKENS
    )


def get_effort(
    config: dict,
) -> str:
    """
    Безопасный default для legacy single-stage вызова. Production staged FULL
    задаёт effort явно, а Scout использует отдельный low-effort policy.
    """

    return str(
        DEFAULT_EFFORT
    )


def get_api_retry_policy(
    api_stage: str,
    config: dict | None = None,
) -> dict:
    """Returns a bounded, user-overridable business retry policy.

    Optional keys in config/anthropic.json:

        scout_model
        scout_effort
        full_max_attempts
        full_retry_delays_seconds
        scout_max_attempts
        scout_retry_delays_seconds

    The API key is never copied into logs or archives.
    """
    if config is None:
        config = load_anthropic_config()

    stage = str(api_stage).strip().upper()
    if stage.endswith("_REPAIR"):
        attempts_key = "repair_max_attempts"
        delays_key = "repair_retry_delays_seconds"
        default_attempts = DEFAULT_REPAIR_MAX_ATTEMPTS
        default_delays = []
    elif stage == "SCOUT":
        attempts_key = "scout_max_attempts"
        delays_key = "scout_retry_delays_seconds"
        default_attempts = DEFAULT_SCOUT_MAX_ATTEMPTS
        default_delays = DEFAULT_SCOUT_RETRY_DELAYS_SECONDS
    else:
        attempts_key = "full_max_attempts"
        delays_key = "full_retry_delays_seconds"
        default_attempts = DEFAULT_FULL_MAX_ATTEMPTS
        default_delays = DEFAULT_FULL_RETRY_DELAYS_SECONDS

    try:
        max_attempts = int(config.get(attempts_key, default_attempts))
    except (TypeError, ValueError):
        max_attempts = default_attempts
    # V8.5.2 reliability floor: an old copied anthropic.json must not silently
    # restore the former 2/1-attempt behaviour that consumed an H1 after a
    # provider refusal.  Operators may raise up to the hard cap, not lower the
    # release-tested minimum for the selected stage.
    max_attempts = min(
        MAX_CONFIGURED_ATTEMPTS,
        max(default_attempts, 1, max_attempts),
    )

    configured_delays = config.get(delays_key, default_delays)
    if not isinstance(configured_delays, list):
        configured_delays = list(default_delays)

    delays = []
    for value in configured_delays:
        try:
            delay = float(value)
        except (TypeError, ValueError):
            continue
        delays.append(min(MAX_RETRY_DELAY_SECONDS, max(0.0, delay)))

    while len(delays) < max_attempts - 1:
        fallback = delays[-1] * 2 if delays else 10.0
        delays.append(min(MAX_RETRY_DELAY_SECONDS, fallback))

    try:
        outcome_unknown_delay = float(
            config.get(
                "outcome_unknown_min_delay_seconds",
                DEFAULT_OUTCOME_UNKNOWN_MIN_DELAY_SECONDS,
            )
        )
    except (TypeError, ValueError):
        outcome_unknown_delay = DEFAULT_OUTCOME_UNKNOWN_MIN_DELAY_SECONDS
    outcome_unknown_delay = min(
        MAX_RETRY_DELAY_SECONDS,
        max(0.0, outcome_unknown_delay),
    )

    return {
        "api_stage": stage,
        "max_attempts": max_attempts,
        "retry_delays_seconds": delays[: max_attempts - 1],
        "outcome_unknown_min_delay_seconds": outcome_unknown_delay,
    }


# ============================================================
# MARKET MESSAGE / PROMPT CACHE
# ============================================================

def _compact_json(value) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    )


RAW_BAR_COLUMNS = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "tick_volume",
    "spread_points",
    "real_volume",
]


def _bars_to_transport_table(bars) -> dict:
    """Encodes dict bars as columns + rows without dropping any value."""
    if not isinstance(bars, list):
        bars = []

    columns = list(RAW_BAR_COLUMNS)
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        for name in bar:
            if name not in columns:
                columns.append(str(name))

    rows = []
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        rows.append([bar.get(name) for name in columns])

    return {
        "columns": columns,
        "rows": rows,
    }


def _transport_timeframes(timeframes, bars_key: str) -> dict:
    """Builds a transport-only copy; the web/archive payload stays intact."""
    result = {}
    if not isinstance(timeframes, dict):
        return result

    for timeframe_name, source in timeframes.items():
        if not isinstance(source, dict):
            continue
        encoded = dict(source)
        encoded[bars_key] = _bars_to_transport_table(source.get(bars_key))
        result[str(timeframe_name)] = encoded

    return result


def build_transport_payload(payload: dict) -> dict:
    """Returns the exact same market facts in a token-efficient wire shape."""
    cacheable_history = payload.get("cacheable_history")
    if not isinstance(cacheable_history, dict):
        cacheable_history = {}
    transport_history = dict(cacheable_history)
    transport_history["closed_market_history_before_day_start"] = (
        _transport_timeframes(
            cacheable_history.get("closed_market_history_before_day_start"),
            "closed_bars",
        )
    )

    live_market = payload.get("live_market")
    if not isinstance(live_market, dict):
        live_market = {}
    transport_live = dict(live_market)
    transport_live["raw_timeframes_since_day_start"] = _transport_timeframes(
        live_market.get("raw_timeframes_since_day_start"),
        "closed_bars_since_day_start",
    )

    return {
        "instrument": payload.get("instrument"),
        "timestamp": payload.get("timestamp"),
        "timezone": payload.get("timezone"),
        "analysis_policy": payload.get("analysis_policy", {}),
        "cache_partition": payload.get("cache_partition", {}),
        "cacheable_history": transport_history,
        "live_market": transport_live,
        "data_capabilities": payload.get("data_capabilities", {}),
        "raw_bar_transport_format": {
            "kind": "columnar_rows_v1",
            "meaning": (
                "For every bar table, rows use the exact field order in "
                "columns. No bars or numeric values were removed."
            ),
        },
    }


def get_transport_payload_size_bytes(payload: dict) -> int:
    return len(_compact_json(build_transport_payload(payload)).encode("utf-8"))


def get_transport_payload_sha256(payload: dict) -> str:
    encoded = _compact_json(build_transport_payload(payload)).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_market_content(
    payload: dict,
    previous_reference: dict | None = None,
) -> list[dict]:
    """
    Формирует user content из отдельных блоков.

    CACHE QUALITY PRINCIPLE
    -----------------------
    Claude видит ТЕ ЖЕ полные raw market data независимо от cache hit/miss.
    Cache меняет только способ обработки одинакового префикса Anthropic.

    Блок 1 — historical-base и спецификация.
    В STEP 10 cache для редких FULL выключен: при интервале в несколько часов
    1h cache успевает истечь до следующего обязательного FULL.

    Блок 2 — все свежие данные текущих суток + current unclosed bars.
    Он никогда не кешируется как источник истины для следующей H1.

    Блок 3 — предыдущий успешный анализ, если он свежий.
    Он reference-only и расположен ПОСЛЕ текущих raw data.
    """

    transport_payload = build_transport_payload(payload)
    cache_partition = transport_payload.get("cache_partition", {})

    cacheable_block = {
        "instrument": transport_payload.get("instrument"),
        "timezone": transport_payload.get("timezone"),
        "base_cutoff_fp": cache_partition.get("base_cutoff_fp"),
        "raw_bar_transport_format": transport_payload.get(
            "raw_bar_transport_format"
        ),
        "cacheable_history": transport_payload.get("cacheable_history", {}),
    }

    live_block = {
        "timestamp": transport_payload.get("timestamp"),
        "analysis_policy": transport_payload.get("analysis_policy", {}),
        "live_market": transport_payload.get("live_market", {}),
        "data_capabilities": transport_payload.get("data_capabilities", {}),
    }

    cache_control = {
        "type": "ephemeral",
        "ttl": PROMPT_CACHE_TTL,
    }

    historical_block = {
        "type": "text",
        "text": (
            "FULL RAW MARKET CONTEXT — STATIC HISTORICAL BASE.\n"
            "This is raw MT5 history, not a prior interpretation. "
            "Combine it with the fresh live block that follows.\n\n"
            "<raw_historical_base>\n"
            f"{_compact_json(cacheable_block)}\n"
            "</raw_historical_base>"
        ),
    }

    if PROMPT_CACHE_ENABLED:
        historical_block["cache_control"] = cache_control

    content = [
        historical_block,
        {
            "type": "text",
            "text": (
                "FULL RAW MARKET CONTEXT — FRESH CURRENT-DAY DATA.\n"
                "These data are current for this request. Reconstruct the "
                "market independently from the historical base + this block.\n\n"
                "<fresh_market_data>\n"
                f"{_compact_json(live_block)}\n"
                "</fresh_market_data>"
            ),
        },
    ]

    if previous_reference is not None:
        reference_payload = {
            "saved_at_fp": previous_reference.get("saved_at_fp"),
            "market_snapshot_time_fp": previous_reference.get(
                "market_snapshot_time_fp"
            ),
            "h1_closed_bar_time_fp": previous_reference.get(
                "h1_closed_bar_time_fp"
            ),
            "analysis": previous_reference.get("analysis", {}),
        }

        content.append(
            {
                "type": "text",
                "text": (
                    "PREVIOUS ANALYSIS — REFERENCE ONLY.\n"
                    "Do NOT treat this as truth and do NOT continue it "
                    "automatically. First trust your independent reading of "
                    "the complete fresh raw market data above. Use this old "
                    "analysis only to compare what changed or was confirmed.\n\n"
                    "<previous_analysis_reference>\n"
                    f"{_compact_json(reference_payload)}\n"
                    "</previous_analysis_reference>"
                ),
            }
        )

    return content


# ============================================================
# SYSTEM PROMPT
# ============================================================

def build_system_content():
    return SYSTEM_PROMPT


# ============================================================
# TOKEN COUNT
# ============================================================

def count_input_tokens(
    client: Anthropic,
    model: str,
    system_content,
    market_content: list[dict],
    effort: str,
) -> int:

    result = client.messages.count_tokens(

        model=model,

        system=system_content,

        messages=[
            {
                "role": "user",
                "content": market_content,
            }
        ],

        output_config={
            "effort": effort,

            "format": {
                "type": "json_schema",
                "schema": CLAUDE_RESPONSE_SCHEMA,
            }
        },
    )

    return int(
        result.input_tokens
    )


# ============================================================
# RAW RESPONSE DEBUG
# ============================================================

def save_raw_response(
    response,
    request_id=None,
):

    DEBUG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    content_data = []

    for block in response.content:

        block_type = getattr(
            block,
            "type",
            None,
        )

        item = {
            "type": block_type,
        }

        if block_type == "text":

            item["text"] = getattr(
                block,
                "text",
                "",
            )

        elif block_type in (
            "thinking",
            "redacted_thinking",
        ):

            item["thinking_present"] = True

        content_data.append(
            item
        )

    usage = getattr(
        response,
        "usage",
        None,
    )

    raw = {

        "request_id": (
            str(request_id) if request_id not in (None, "") else None
        ),

        "id": getattr(
            response,
            "id",
            None,
        ),

        "model": getattr(
            response,
            "model",
            None,
        ),

        "stop_reason": getattr(
            response,
            "stop_reason",
            None,
        ),

        "stop_sequence": getattr(
            response,
            "stop_sequence",
            None,
        ),

        "content": content_data,

        "usage": {
            "input_tokens": int(
                getattr(
                    usage,
                    "input_tokens",
                    0,
                )
                or 0
            ),

            "output_tokens": int(
                getattr(
                    usage,
                    "output_tokens",
                    0,
                )
                or 0
            ),

            "cache_creation_input_tokens": int(
                getattr(
                    usage,
                    "cache_creation_input_tokens",
                    0,
                )
                or 0
            ),

            "cache_read_input_tokens": int(
                getattr(
                    usage,
                    "cache_read_input_tokens",
                    0,
                )
                or 0
            ),
        },
    }

    with open(
        DEBUG_RAW_RESPONSE_PATH,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            raw,
            file,
            ensure_ascii=False,
            indent=2,
        )

    response_identifier = raw.get("id") or raw.get("request_id")
    if response_identifier:
        safe_identifier = "".join(
            character
            for character in str(response_identifier)
            if character.isalnum() or character in {"-", "_"}
        )
        if safe_identifier:
            DEBUG_ATTEMPTS_DIR.mkdir(parents=True, exist_ok=True)
            attempt_path = DEBUG_ATTEMPTS_DIR / f"{safe_identifier}.json"
            with open(attempt_path, "w", encoding="utf-8") as file:
                json.dump(raw, file, ensure_ascii=False, indent=2)


# ============================================================
# TEXT RESPONSE
# ============================================================

def extract_text_response(
    response,
) -> str:

    blocks = []

    for block in response.content:

        if getattr(
            block,
            "type",
            None,
        ) == "text":

            text = getattr(
                block,
                "text",
                "",
            )

            if text:

                blocks.append(
                    text
                )

    return "".join(
        blocks
    ).strip()


# ============================================================
# ПРОВЕРКА STOP REASON
# ============================================================

def validate_stop_reason(
    response,
):

    stop_reason = getattr(
        response,
        "stop_reason",
        None,
    )

    if stop_reason == "max_tokens":

        raise ClaudeIncompleteResponseError(
            "Claude достиг max_tokens до завершения ответа. "
            "Structured Output был оборван. "
            "Нужен последовательный compact recovery retry."
        )

    if stop_reason == "refusal":

        raise ClaudeRefusalResponseError(
            "Claude отказался выполнять запрос. "
            "Отказ не является торговым анализом; нужен recovery retry."
        )

    if stop_reason not in (
        "end_turn",
        None,
    ):

        raise RuntimeError(
            f"Неожиданный stop_reason Claude: "
            f"{stop_reason}"
        )


# ============================================================
# ПРОВЕРКА TRADE LEVELS
# ============================================================

def validate_trade_levels(
    analysis: dict,
):

    recommendation = analysis[
        "recommendation"
    ]

    action = recommendation[
        "action"
    ]

    entry = recommendation[
        "entry_price"
    ]

    stop = recommendation[
        "stop_loss"
    ]

    take_profit = recommendation[
        "take_profit"
    ]

    if action == "stay_out":

        if (
            entry is not None
            or stop is not None
            or take_profit is not None
        ):

            raise ValueError(
                "Claude вернул stay_out, "
                "но указал Entry / SL / TP."
            )

        return

    if (
        entry is None
        or stop is None
        or take_profit is None
    ):

        raise ValueError(
            "Claude рекомендовал вход, "
            "но Entry / SL / TP заполнены не полностью."
        )

    if action == "enter_long":

        if not (
            stop
            < entry
            < take_profit
        ):

            raise ValueError(
                "Некорректные уровни LONG: "
                "должно выполняться SL < Entry < TP."
            )

    elif action == "enter_short":

        if not (
            take_profit
            < entry
            < stop
        ):

            raise ValueError(
                "Некорректные уровни SHORT: "
                "должно выполняться TP < Entry < SL."
            )

    else:

        raise ValueError(
            f"Неизвестное действие Claude: "
            f"{action}"
        )


# ============================================================
# VALIDATION — PROFESSIONAL TRADER CONTRACT
# ============================================================

def validate_analysis_contract(
    analysis: dict,
):
    """
    Проверяет логическую согласованность расширенного
    Structured Output профессионального трейдера.

    Эта проверка не оценивает качество стратегии и не заменяет
    Risk Manager. Она защищает только контракт данных.
    """

    recommendation = analysis["recommendation"]
    action = recommendation["action"]
    setup_type = recommendation["setup_type"]
    order_type = recommendation["order_type"]
    setup_quality = recommendation["setup_quality"]
    entry_quality = recommendation["entry_quality"]
    invalidation_level = recommendation["invalidation_level"]

    allowed_regimes = {
        "trend",
        "correction",
        "range",
        "breakout",
        "reversal",
        "transition",
        "unclear",
    }
    allowed_directions = {
        "bullish",
        "bearish",
        "neutral",
        "mixed",
        "unclear",
    }
    allowed_setup_types = {
        "trend_pullback",
        "wave3_continuation",
        "wave5_continuation",
        "correction_a_leg",
        "correction_b_leg",
        "correction_c_leg",
        "correction_completion",
        "range_long",
        "range_short",
        "range_breakout",
        "breakout_retest",
        "false_breakout_reversal",
        "trend_reversal",
        "diagonal_reversal",
        "pattern_continuation",
        "pattern_reversal",
        "transition_trade",
        "other",
        "no_trade",
    }
    allowed_trade_horizons = {
        "intraday",
        "swing",
        "multi_day",
        "unclear",
    }
    allowed_phase_status = {
        "developing",
        "mature",
        "completing",
        "completed",
        "transitioning",
        "failed",
        "unclear",
    }
    allowed_setup_quality = {
        "weak",
        "acceptable",
        "good",
        "excellent",
    }
    allowed_entry_quality = {
        "poor",
        "fair",
        "good",
        "excellent",
    }
    allowed_confidence = {
        "low",
        "medium",
        "high",
    }
    allowed_fvg_roles = {
        "confirmation",
        "entry_zone",
        "target",
        "invalidation",
        "conflict",
        "neutral",
        "no_relevant_fvg",
        "not_assessed_legacy",
    }

    if analysis.get("instrument") != SYMBOL:
        raise ValueError(
            f"Claude вернул неожиданный инструмент: {analysis.get('instrument')}."
        )

    regime = analysis.get("market_regime", {})
    if regime.get("primary_regime") not in allowed_regimes:
        raise ValueError(
            "Claude вернул неизвестный market_regime: "
            f"{regime.get('primary_regime')}."
        )

    if regime.get("direction") not in allowed_directions:
        raise ValueError(
            "Claude вернул неизвестное направление market_regime: "
            f"{regime.get('direction')}."
        )

    current_phase = str(regime.get("current_phase", "")).strip()
    if not current_phase:
        raise ValueError(
            "Claude не указал market_regime.current_phase."
        )

    if regime.get("phase_status") not in allowed_phase_status:
        raise ValueError(
            "Claude вернул неизвестный phase_status: "
            f"{regime.get('phase_status')}."
        )

    if setup_type not in allowed_setup_types:
        raise ValueError(
            f"Claude вернул неизвестный setup_type: {setup_type}."
        )

    if recommendation.get("trade_horizon") not in allowed_trade_horizons:
        raise ValueError(
            "Claude вернул неизвестный trade_horizon: "
            f"{recommendation.get('trade_horizon')}."
        )

    if setup_quality not in allowed_setup_quality:
        raise ValueError(
            f"Claude вернул неизвестный setup_quality: {setup_quality}."
        )

    if entry_quality not in allowed_entry_quality:
        raise ValueError(
            f"Claude вернул неизвестный entry_quality: {entry_quality}."
        )

    if recommendation.get("confidence") not in allowed_confidence:
        raise ValueError(
            "Claude вернул неизвестный confidence: "
            f"{recommendation.get('confidence')}."
        )

    fvg_role = recommendation.get("fvg_role") or "not_assessed_legacy"
    recommendation["fvg_role"] = fvg_role
    recommendation.setdefault("fvg_ids", "")
    recommendation.setdefault("fvg_basis", "")
    if fvg_role not in allowed_fvg_roles:
        raise ValueError(f"Claude вернул неизвестную роль FVG: {fvg_role}.")
    if action in {"enter_long", "enter_short"} and fvg_role == "conflict":
        raise ValueError("Claude рекомендовал вход при явно конфликтующем FVG-контексте.")

    if action == "stay_out":
        if setup_type != "no_trade":
            raise ValueError(
                "Claude вернул stay_out, но setup_type != no_trade."
            )

        if order_type != "none":
            raise ValueError(
                "Claude вернул stay_out, но order_type != none."
            )

        if invalidation_level is not None:
            raise ValueError(
                "Claude вернул stay_out, но указал invalidation_level."
            )

        return

    if setup_type == "no_trade":
        raise ValueError(
            "Claude рекомендовал вход, но setup_type = no_trade."
        )

    if setup_quality == "weak":
        raise ValueError(
            "Claude рекомендовал вход с setup_quality = weak. "
            "По контракту слабый setup должен быть stay_out."
        )

    if order_type == "none":
        raise ValueError(
            "Claude рекомендовал вход, но order_type = none."
        )

    if entry_quality == "poor":
        raise ValueError(
            "Claude рекомендовал вход с entry_quality = poor. "
            "По контракту такой setup должен быть stay_out."
        )

    if invalidation_level is None:
        raise ValueError(
            "Claude рекомендовал вход без структурного invalidation_level."
        )

    phase_status = regime.get("phase_status")

    if setup_type in {
        "correction_a_leg",
        "correction_b_leg",
        "correction_c_leg",
    } and phase_status in {"completed", "failed"}:
        raise ValueError(
            "Claude пытается торговать активную коррекционную волну, "
            f"но phase_status = {phase_status}. "
            "Для завершённой/сломавшейся фазы нужен другой setup_type."
        )

    if setup_type == "correction_completion" and phase_status not in {
        "completing",
        "completed",
        "transitioning",
    }:
        raise ValueError(
            "Claude вернул correction_completion, но phase_status не "
            "указывает на завершение/переход: "
            f"{phase_status}."
        )


# ============================================================
# SAVE FINAL RESPONSE
# ============================================================

def save_debug_response(
    analysis: dict,
):

    DEBUG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        DEBUG_RESPONSE_PATH,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            analysis,
            file,
            ensure_ascii=False,
            indent=2,
        )

    print(
        "[OK] Ответ Claude сохранён:"
    )

    print(
        f"     {DEBUG_RESPONSE_PATH}"
    )


# ============================================================
# USAGE
# ============================================================

def get_usage_stats(
    response,
) -> dict:

    usage = response.usage

    output_details = getattr(usage, "output_tokens_details", None)

    return {

        "input_tokens": int(
            getattr(
                usage,
                "input_tokens",
                0,
            )
            or 0
        ),

        "output_tokens": int(
            getattr(
                usage,
                "output_tokens",
                0,
            )
            or 0
        ),

        "cache_creation_input_tokens": int(
            getattr(
                usage,
                "cache_creation_input_tokens",
                0,
            )
            or 0
        ),

        "cache_read_input_tokens": int(
            getattr(
                usage,
                "cache_read_input_tokens",
                0,
            )
            or 0
        ),

        "thinking_tokens": int(
            getattr(
                output_details,
                "thinking_tokens",
                0,
            )
            or 0
        ),
    }


def get_last_usage_stats() -> dict | None:
    if not isinstance(_LAST_USAGE_STATS, dict):
        return None
    return dict(_LAST_USAGE_STATS)


def get_last_attempt_diagnostics() -> dict | None:
    if not isinstance(_LAST_ATTEMPT_DIAGNOSTICS, dict):
        return None
    return dict(_LAST_ATTEMPT_DIAGNOSTICS)


# ============================================================
# PRINT ANALYSIS
# ============================================================

def print_analysis_summary(
    analysis: dict,
):
    """
    Печатает компактный Structured Output, сохраняя полный
    профессиональный смысл анализа.
    """

    regime = analysis["market_regime"]
    tf = analysis["timeframe_analysis"]
    price = analysis["price_structure"]
    patterns = analysis["patterns"]
    wave = analysis["wave_count"]
    higher = analysis["higher_timeframe_context"]
    scenarios = analysis["scenario_map"]
    rec = analysis["recommendation"]
    quality = analysis["data_quality"]

    if os.getenv("ROBOT_CONSOLE_DETAIL", "compact").strip().lower() not in {
        "full", "detailed", "debug", "1", "true", "yes"
    }:
        def ru(value):
            text = str(value or "")
            text = text.split("RU:", 1)[-1] if "RU:" in text else text
            text = " ".join(text.split())
            return text if len(text) <= 360 else text[:357].rstrip() + "..."

        print()
        print("=" * 80)
        print("ИТОГ FULL-АНАЛИЗА")
        print("=" * 80)
        print(f"Время:          {analysis['timestamp']}")
        print(f"Режим/фаза:     {regime['primary_regime']} / {regime['current_phase']}")
        print(f"Текущая волна:  {wave['current_label']} ({wave['current_phase']})")
        print(f"Инвалидация:    {wave['invalidation_level']}")
        print(f"Решение:        {rec['action']}")
        print(f"Сетап/качество: {rec['setup_type']} / {rec['setup_quality']} / {rec['entry_quality']}")
        print(f"Уверенность:    {rec['confidence']}")
        print(f"Entry/SL/TP:    {rec['entry_price']} / {rec['stop_loss']} / {rec['take_profit']}")
        print(f"Почему:         {ru(rec['why_now'])}")
        print(f"Следующий шанс: {ru(scenarios['next_opportunity'])}")
        print(f"Смена сценария: {ru(scenarios['regime_change_trigger'])}")
        print(f"Качество данных:{' OK' if quality['sufficient'] else ' НЕДОСТАТОЧНО'}")
        print("[DETAIL] Полный RU/EN-анализ и графические объекты сохранены в archive/debug.")
        print("=" * 80)
        return

    print()
    print("=" * 80)
    print("АНАЛИЗ CLAUDE — PROFESSIONAL TRADER")
    print("=" * 80)
    print(f"Инструмент:    {analysis['instrument']}")
    print(f"Время:         {analysis['timestamp']}")

    print()
    print("MARKET REGIME")
    print("-" * 80)
    print(f"Primary regime:{' '}{regime['primary_regime']}")
    print(f"Направление:   {regime['direction']}")
    print(f"Current phase: {regime['current_phase']}")
    print(f"Phase status:  {regime['phase_status']}")
    print(f"Зрелость:      {regime['maturity']}")
    print(f"Location:      {regime['location']}")
    print(f"Описание:      {regime['summary']}")

    print()
    print("MULTI-TIMEFRAME ANALYSIS")
    print("-" * 80)
    print(f"D1:            {tf['D1']}")
    print(f"H4:            {tf['H4']}")
    print(f"H1:            {tf['H1']}")
    print(f"Связь TF:      {tf['relationship']}")
    print(f"Итог:          {tf['summary']}")

    print()
    print("PRICE STRUCTURE / PRICE ACTION")
    print("-" * 80)
    print(f"Состояние:     {price['structure_state']}")
    print(f"Swings:        {price['swing_structure']}")
    print(f"Ключевые уровни: {price['key_levels']}")
    print(f"Liquidity:     {price['liquidity_context']}")
    print(f"Описание:      {price['summary']}")

    print()
    print("PATTERNS")
    print("-" * 80)
    print(patterns)

    print()
    print("ВОЛНЫ ЭЛЛИОТА")
    print("-" * 80)
    print(f"Структура:     {wave['structure_type']}")
    print(f"Направление:   {wave['direction']}")
    print(f"Текущая волна: {wave['current_label']}")
    print(f"Фаза:          {wave['current_phase']}")
    print(f"Инвалидация:   {wave['invalidation_level']}")
    print(f"Альтернатива:  {wave['alternate_count']}")
    print(f"Описание:      {wave['summary']}")

    print()
    print("СТАРШИЙ КОНТЕКСТ")
    print("-" * 80)
    print(f"D1:            {higher['d1_trend']}")
    print(f"D1 waves:      {higher['d1_wave_context']}")
    print(f"H4:            {higher['h4_trend']}")
    print(f"H4 waves:      {higher['h4_wave_context']}")
    print(f"Согласование:  {higher['alignment']}")
    print(f"Описание:      {higher['summary']}")

    print()
    print("SCENARIO MAP")
    print("-" * 80)
    print(f"Основной:      {scenarios['primary_scenario']}")
    print(f"Альтернативный: {scenarios['alternate_scenario']}")
    print(f"Ожидаемый путь: {scenarios['expected_path']}")
    print(f"Сейчас:        {scenarios['current_opportunity']}")
    print(f"Следом:        {scenarios['next_opportunity']}")
    print(f"Смена режима:  {scenarios['regime_change_trigger']}")

    print()
    print("ТОРГОВАЯ РЕКОМЕНДАЦИЯ")
    print("-" * 80)
    print(f"Действие:      {rec['action']}")
    print(f"Setup:         {rec['setup_type']}")
    print(f"Горизонт:      {rec['trade_horizon']}")
    print(f"Setup quality: {rec['setup_quality']}")
    print(f"Entry quality: {rec['entry_quality']}")
    print(f"Тип ордера:    {rec['order_type']}")
    print(f"Entry:         {rec['entry_price']}")
    print(f"Stop Loss:     {rec['stop_loss']}")
    print(f"Take Profit:   {rec['take_profit']}")
    print(f"Setup invalid: {rec['invalidation_level']}")
    print(f"Confidence:    {rec['confidence']}")
    print(f"Почему сейчас: {rec['why_now']}")
    print(f"Основа SL:     {rec['structural_stop_basis']}")
    print(f"Основа TP:     {rec['target_basis']}")
    print(f"Обоснование:   {rec['reasoning']}")
    print(f"Инвалидация:   {rec['invalidation_reason']}")

    print()
    print("КАЧЕСТВО ДАННЫХ")
    print("-" * 80)
    print(f"Достаточно:    {quality['sufficient']}")
    print(f"Проблемы:      {quality['issues']}")
    print("=" * 80)


def analyze_market(
    payload: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:

    global _LAST_USAGE_STATS
    global _LAST_ATTEMPT_DIAGNOSTICS

    _LAST_USAGE_STATS = None
    _LAST_ATTEMPT_DIAGNOSTICS = None

    config = load_anthropic_config()

    model = get_model(
        config
    )

    max_tokens = get_max_tokens(
        config
    )

    effort = get_effort(
        config
    )

    client = create_anthropic_client(
        config
    )

    market_content = build_market_content(
        payload=payload,
        previous_reference=previous_reference,
    )

    transport_payload_bytes = get_transport_payload_size_bytes(payload)
    transport_payload_sha256 = get_transport_payload_sha256(payload)

    system_content = build_system_content()

    print()
    print("=" * 80)
    print("ANTHROPIC API")
    print("=" * 80)

    print(
        f"Модель:       {model}"
    )

    print(
        f"Max tokens:   {max_tokens}"
    )

    print(
        f"Effort:       {effort}"
    )

    if PROMPT_CACHE_ENABLED:
        print(
            "Prompt cache: ВКЛ — explicit raw historical base"
        )
        print(
            f"Cache TTL:    {PROMPT_CACHE_TTL}"
        )
    else:
        print(
            "Prompt cache: ВЫКЛ — FULL запускается редко, 1h cache не окупается"
        )

    print(
        "Analysis mode: FULL INDEPENDENT RAW REANALYSIS"
    )

    print(
        "Previous ref:  "
        f"{'YES (reference-only)' if previous_reference else 'NO'}"
    )

    print(
        "Transport:    SSE streaming"
    )

    print(
        f"Timeout:      "
        f"{get_effective_timeout_seconds(config):.0f} sec"
    )

    print(
        "SDK retries:  OFF (business retries are journaled by main.py)"
    )

    original_payload_bytes = len(_compact_json(payload).encode("utf-8"))
    print(
        "Wire payload: "
        f"{transport_payload_bytes / 1024:.1f} KB "
        f"(archive JSON {original_payload_bytes / 1024:.1f} KB; "
        "all market values preserved)"
    )

    print()
    print(
        "[INFO] Считаем входные токены..."
    )

    try:
        token_count = count_input_tokens(
            client=client,
            model=model,
            system_content=system_content,
            market_content=market_content,
            effort=effort,
        )
    except anthropic.AuthenticationError as error:
        raise ClaudePermanentRequestError(
            "Ошибка авторизации Anthropic API при подсчёте токенов.",
            request_id=_anthropic_error_request_id(error),
            status_code=401,
        ) from error
    except anthropic.PermissionDeniedError as error:
        raise ClaudePermanentRequestError(
            "API key не имеет доступа к модели при подсчёте токенов.",
            request_id=_anthropic_error_request_id(error),
            status_code=403,
        ) from error
    except (anthropic.APITimeoutError, anthropic.APIConnectionError) as error:
        raise ClaudeTransientRequestError(
            "Не удалось выполнить бесплатный подсчёт входных токенов; "
            "платный Messages-запрос ещё не отправлялся.",
            request_id=_anthropic_error_request_id(error),
        ) from error
    except anthropic.APIStatusError as error:
        raise _translate_api_status_error(error, "token counting") from error

    _LAST_ATTEMPT_DIAGNOSTICS = {
        "model": model,
        "input_tokens": int(token_count),
        "transport_payload_bytes": int(transport_payload_bytes),
        "payload_sha256": transport_payload_sha256,
        "request_id": None,
        "response_id": None,
        "stop_reason": None,
        "response_received": False,
        "usage": None,
    }

    if callable(on_preflight):
        # A failure here intentionally prevents the paid call: the attempt
        # must be durably journaled with its token count before dispatch.
        on_preflight(dict(_LAST_ATTEMPT_DIAGNOSTICS))

    print(
        f"[INFO] Входных токенов: "
        f"{token_count:,}"
    )

    print()
    print(
        "[INFO] Отправляем рыночные данные Claude..."
    )

    request_id = None

    try:

        # Для длинных MAX-запросов Anthropic рекомендует streaming.
        # stream.get_final_message() собирает тот же финальный Message,
        # но SSE-события поддерживают соединение активным во время
        # длительного thinking/generation.
        with client.messages.stream(

            model=model,

            max_tokens=max_tokens,

            system=system_content,

            messages=[
                {
                    "role": "user",
                    "content": market_content,
                }
            ],

            output_config={
                "effort": effort,

                "format": {
                    "type": "json_schema",
                    "schema": CLAUDE_RESPONSE_SCHEMA,
                }
            },
        ) as stream:

            response = (
                stream.get_final_message()
            )

            request_id = getattr(
                stream,
                "request_id",
                None,
            )

            if request_id in (None, ""):
                request_id = getattr(response, "_request_id", None)

    except anthropic.AuthenticationError as error:

        raise ClaudePermanentRequestError(
            "Ошибка авторизации Anthropic API. Точный повтор не поможет.",
            request_id=_anthropic_error_request_id(error),
            status_code=401,
        ) from error

    except anthropic.PermissionDeniedError as error:

        raise ClaudePermanentRequestError(
            "API key не имеет доступа к модели. Точный повтор не поможет.",
            request_id=_anthropic_error_request_id(error),
            status_code=403,
        ) from error

    except anthropic.RateLimitError as error:

        raise ClaudeTransientRequestError(
            "Превышен rate limit Anthropic API; запрос будет повторён "
            "после controlled backoff.",
            request_id=_anthropic_error_request_id(error),
            status_code=429,
            retry_after_seconds=_anthropic_retry_after_seconds(error),
        ) from error

    except anthropic.APITimeoutError as error:

        raise ClaudeRequestOutcomeUnknownError(
            "Anthropic API timeout после отправки запроса. "
            "Исход генерации неизвестен; controlled retry разрешён "
            "торговой policy проекта. Возможна повторная тарификация.",
            request_id=_anthropic_error_request_id(error),
        ) from error

    except anthropic.APIConnectionError as error:

        raise ClaudeRequestOutcomeUnknownError(
            "Соединение с Anthropic API оборвалось во время запроса. "
            "Исход генерации неизвестен; controlled retry разрешён "
            "торговой policy проекта. Возможна повторная тарификация.",
            request_id=_anthropic_error_request_id(error),
        ) from error

    except anthropic.APIStatusError as error:

        raise _translate_api_status_error(error, "Messages streaming") from error

    except Exception as error:

        raise ClaudeRequestOutcomeUnknownError(
            "Непредвиденная ошибка внутри Messages SSE stream. Исход "
            "генерации неизвестен; controlled retry разрешён.",
            request_id=_anthropic_error_request_id(error),
        ) from error

    # ========================================================
    # СРАЗУ СОХРАНЯЕМ ДИАГНОСТИКУ
    # ========================================================

    stop_reason = getattr(
        response,
        "stop_reason",
        None,
    )

    usage = get_usage_stats(
        response
    )

    _LAST_USAGE_STATS = dict(usage)

    _LAST_ATTEMPT_DIAGNOSTICS.update(
        {
            "request_id": (
                str(request_id) if request_id not in (None, "") else None
            ),
            "response_id": getattr(response, "id", None),
            "stop_reason": stop_reason,
            "response_received": True,
            "usage": dict(usage),
        }
    )

    save_raw_response(
        response,
        request_id=request_id,
    )

    if callable(on_response):
        try:
            on_response(dict(_LAST_ATTEMPT_DIAGNOSTICS))
        except Exception as callback_error:
            # We already have the paid response locally. A telemetry failure
            # must not discard it or trigger another paid generation.
            print(
                "[API JOURNAL WARNING] Не удалось дополнить attempt после "
                "получения ответа: "
                f"{type(callback_error).__name__}: {callback_error}"
            )

    print()
    print(
        "[INFO] Ответ API получен."
    )

    if request_id:
        print(
            f"[INFO] Request ID: {request_id}"
        )

    print(
        f"[INFO] Stop reason: "
        f"{stop_reason}"
    )

    print()
    print("ИСПОЛЬЗОВАНИЕ ТОКЕНОВ")
    print("-" * 80)

    print(
        f"Input:        "
        f"{usage['input_tokens']:,}"
    )

    print(
        f"Output:       "
        f"{usage['output_tokens']:,}"
    )

    if usage.get("thinking_tokens", 0):
        print(
            f"Thinking:     "
            f"{usage['thinking_tokens']:,}"
        )

    print(
        f"Cache write:  "
        f"{usage['cache_creation_input_tokens']:,}"
    )

    print(
        f"Cache read:   "
        f"{usage['cache_read_input_tokens']:,}"
    )

    # ========================================================
    # ПРОВЕРЯЕМ, ЗАВЕРШИЛСЯ ЛИ ОТВЕТ НОРМАЛЬНО
    # ========================================================

    try:
        if stop_reason == "model_context_window_exceeded":
            raise ClaudePermanentRequestError(
                "Claude исчерпал context window. Точный повтор того же "
                "payload/max_tokens не исправит причину.",
                request_id=request_id,
            )

        validate_stop_reason(response)

        # ====================================================
        # ИЗВЛЕКАЕМ JSON
        # ====================================================
        response_text = extract_text_response(response)
        if not response_text:
            raise RuntimeError(
                "Claude завершил запрос без текстового Structured Output."
            )

        analysis = json.loads(response_text)

        # ====================================================
        # ЛОКАЛЬНАЯ ВАЛИДАЦИЯ
        # ====================================================
        validate_trade_levels(analysis)
        validate_analysis_contract(analysis)

    except ClaudeRequestError:
        raise
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "Ответ Claude получен и тарифицирован, но не прошёл локальную "
            "проверку торгового контракта; controlled retry разрешён. "
            f"Причина: {type(error).__name__}: {error}. "
            f"Raw: {DEBUG_RAW_RESPONSE_PATH}",
            request_id=request_id,
        ) from error

    # Только chart metadata: торговые поля recommendation не меняются.
    chart_warnings = sanitize_visualization(
        analysis=analysis,
        payload=payload,
    )

    if chart_warnings:
        print(
            f"[CHART WARNING] Отфильтровано/отмечено объектов: "
            f"{len(chart_warnings)}"
        )

    # ========================================================
    # SAVE
    # ========================================================

    save_debug_response(
        analysis
    )

    # ========================================================
    # PRINT
    # ========================================================

    print()
    print(
        "[OK] Structured Output успешно разобран."
    )

    print_analysis_summary(
        analysis
    )

    return analysis
