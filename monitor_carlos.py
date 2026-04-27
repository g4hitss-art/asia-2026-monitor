"""
Monitor de Vuelos — Ruta Exacta Carlos
BOG → SAL → SFO → NRT  (Avianca + Zipair)  IDA
ICN → SFO → SAL → BOG  (Air Premia + Avianca) VUELTA

Precio base Carlos:
  IDA   : $3,160,141 COP x persona
  VUELTA: $3,163,941 COP x persona
  TOTAL : $6,324,082 COP x persona / $25,296,328 COP x 4

Equipaje: 2 tiquetes con maleta 23kg + 2 sin maleta (por pareja)
Alerta WhatsApp: Diego + Juliana cuando baje del umbral
Guarda histórico en Supabase para el dashboard HTML
"""

import os, time, sqlite3, logging, sys, re
from datetime import datetime, timedelta
import requests
from twilio.rest import Client

# ── Silenciar logs verbosos ─────────────────────────────────
logging.getLogger("twilio.http_client").setLevel(logging.WARNING)
logging.getLogger("twilio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ── Log ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("monitor_carlos.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("MonitorCarlos")

def sep(t=""):
    log.info("=" * 62)
    if t:
        log.info(f"  {t}")
        log.info("=" * 62)

# ── Configuración ────────────────────────────────────────────
CONFIG = {
    # Twilio WhatsApp
    "TWILIO_SID":   os.getenv("TWILIO_SID",   "AC6ba1ba9733df887c4c44cfe5b0696124"),
    "TWILIO_TOKEN": os.getenv("TWILIO_TOKEN",  "b8c60fbeb944e468af52a06f78b66fca"),
    "WA_DESDE":     "whatsapp:+14155238886",
    "WA_NUMEROS": [
        "whatsapp:+573102745611",  # Diego
        "whatsapp:+573144624739",  # Juliana
    ],

    # Supabase
    "SUPABASE_URL": os.getenv("SUPABASE_URL", "https://qalxstxtuuvybudkqtpd.supabase.co"),
    "SUPABASE_KEY": os.getenv("SUPABASE_KEY", "sb_secret_O5MF06-7ueAeDlo8zLg4YQ_LU4VaycC"),

    # Ruta Carlos — IDA
    "IDA_ORIGEN":   "BOG",
    "IDA_DESTINO":  "NRT",
    "IDA_FECHA":    "2026-10-23",
    "IDA_ESCALAS":  "SAL, SFO",
    "IDA_AEROLINEAS": "Avianca + Zipair",

    # Ruta Carlos — VUELTA
    "VUELTA_ORIGEN":    "ICN",
    "VUELTA_DESTINO":   "BOG",
    "VUELTA_FECHA":     "2026-11-15",
    "VUELTA_ESCALAS":   "SFO, SAL",
    "VUELTA_AEROLINEAS": "Air Premia + Avianca",

    # Precios base Carlos (por persona)
    "PRECIO_BASE_IDA_PX":    3_160_141,
    "PRECIO_BASE_VUELTA_PX": 3_163_941,
    "PRECIO_BASE_TOTAL_PX":  6_324_082,
    "PRECIO_BASE_TOTAL_4PX": 25_296_328,

    # Equipaje: 2 con maleta 23kg + 2 sin maleta
    "PASAJEROS": 4,
    "CON_MALETA": 2,   # Classic / paid baggage
    "SIN_MALETA": 2,   # Light / sin maleta bodega

    # Alertas
    "UMBRAL_BAJADA_PCT": 3,   # Alerta si baja más del 3%
    "DB": "monitor_carlos.db",
}

# ── Supabase ─────────────────────────────────────────────────
def supabase_headers():
    return {
        "apikey":        CONFIG["SUPABASE_KEY"],
        "Authorization": f"Bearer {CONFIG['SUPABASE_KEY']}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def supabase_guardar(registro: dict):
    url = CONFIG["SUPABASE_URL"]
    key = CONFIG["SUPABASE_KEY"]
    if not url or not key:
        return
    try:
        r = requests.post(
            f"{url}/rest/v1/monitor_carlos",
            headers=supabase_headers(),
            json=registro,
            timeout=15,
        )
        if r.status_code in (200, 201):
            log.info("  Supabase: guardado OK")
        else:
            log.warning(f"  Supabase: {r.status_code} {r.text[:80]}")
    except Exception as e:
        log.warning(f"  Supabase error: {e}")

def supabase_historico():
    """Retorna los últimos 50 registros del historial."""
    url = CONFIG["SUPABASE_URL"]
    key = CONFIG["SUPABASE_KEY"]
    if not url or not key:
        return []
    try:
        r = requests.get(
            f"{url}/rest/v1/monitor_carlos"
            f"?select=*&order=fecha_consulta.desc&limit=50",
            headers=supabase_headers(),
            timeout=15,
        )
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []

# ── DB local (respaldo) ───────────────────────────────────────
def init_db():
    conn = sqlite3.connect(CONFIG["DB"])
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS consultas (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_consulta   TEXT,
            tramo            TEXT,
            precio_cop_px    REAL,
            precio_cop_4px   REAL,
            tasa_ref         REAL,
            variacion_pct    REAL,
            nivel            TEXT,
            alerta_enviada   INTEGER DEFAULT 0
        )""")
    conn.commit()
    conn.close()

def guardar_local(tramo, px, total, tasa, var, nivel, alerta):
    conn = sqlite3.connect(CONFIG["DB"])
    conn.cursor().execute(
        "INSERT INTO consultas VALUES (NULL,?,?,?,?,?,?,?,?)",
        (datetime.now().isoformat(), tramo, px, total, tasa, var, nivel,
         1 if alerta else 0)
    )
    conn.commit()
    conn.close()

def ultimo_precio_local(tramo):
    conn = sqlite3.connect(CONFIG["DB"])
    c = conn.cursor()
    c.execute(
        "SELECT precio_cop_px FROM consultas WHERE tramo=? "
        "ORDER BY fecha_consulta DESC LIMIT 1", (tramo,)
    )
    r = c.fetchone()
    conn.close()
    return r[0] if r else None

# ── TRM ───────────────────────────────────────────────────────
def tasa_cop():
    try:
        r = requests.get(
            "https://api.exchangerate-api.com/v4/latest/USD", timeout=10
        )
        t = r.json()["rates"]["COP"]
        log.info(f"TRM: ${t:,.2f} COP/USD")
        return t
    except Exception as e:
        log.warning(f"TRM fallback $3,701: {e}")
        return 3_701.0

# ── Links directos ────────────────────────────────────────────
LINKS = {
    "ida_skyscanner": (
        "https://www.skyscanner.com.co/transporte/vuelos/bog/nrt/261023/"
        "?adults=4&cabinclass=economy"
    ),
    "ida_avianca": (
        "https://www.avianca.com/es_co/reservas/"
        "?origin=BOG&destination=SAL&date=2026-10-23&adults=4"
    ),
    "ida_zipair": "https://www.zipair.net/en/flight/search",
    "vuelta_skyscanner": (
        "https://www.skyscanner.com.co/transporte/vuelos/icn/bog/261115/"
        "?adults=4&cabinclass=economy"
    ),
    "vuelta_airpremia": "https://airpremia.com/en/booking/",
}

# ── Búsqueda de precio ────────────────────────────────────────
def buscar_precio_skyscanner(tramo: str, origen: str, destino: str,
                              fecha: str, tasa: float):
    """
    Consulta precio via fast-flights (Google Flights).
    Para la ruta Carlos filtramos por aerolíneas específicas.
    """
    try:
        from fast_flights import FlightData, Passengers, get_flights
    except ImportError:
        log.error("fast-flights no instalado. Ejecutar: pip install fast-flights")
        return None

    log.info(f"  Buscando {tramo}: {origen} → {destino} · {fecha} ...")

    for intento in range(1, 4):
        try:
            if intento > 1:
                log.info(f"  Reintento {intento}/3 ...")
                time.sleep(12)

            result = get_flights(
                flight_data=[
                    FlightData(
                        date=fecha,
                        from_airport=origen,
                        to_airport=destino,
                    )
                ],
                trip="one-way",
                seat="economy",
                passengers=Passengers(adults=CONFIG["PASAJEROS"]),
                fetch_mode="fallback",
            )

            if not result or not result.flights:
                log.warning(f"  Sin resultados para {tramo}")
                continue

            # Buscar vuelo más barato con precio válido
            mejor_cop = None
            mejor_info = {}

            for v in result.flights:
                raw = getattr(v, "price", None)
                if raw is None:
                    continue
                solo = re.sub(r"[^\d.]", "", str(raw))
                if not solo:
                    continue
                try:
                    precio = float(solo)
                except Exception:
                    continue

                # Convertir a COP si viene en USD
                precio_cop = (precio if precio > 50_000
                              else precio * CONFIG["PASAJEROS"] * tasa)

                if mejor_cop is None or precio_cop < mejor_cop:
                    mejor_cop = precio_cop
                    mejor_info = {
                        "aerolinea": str(getattr(v, "name", "") or ""),
                        "hora":      str(getattr(v, "departure", "") or ""),
                        "duracion":  str(getattr(v, "duration", "") or ""),
                        "escalas":   str(getattr(v, "stops", "") or ""),
                    }

            if mejor_cop is None:
                log.warning(f"  Sin precio válido para {tramo}")
                continue

            # Precio por persona (dividimos por 4)
            precio_px = mejor_cop / CONFIG["PASAJEROS"]
            nivel = str(getattr(result, "current_price", "typical")).lower()

            log.info(
                f"  {tramo}: ${precio_px:,.0f} COP/px "
                f"(${mejor_cop:,.0f} COP total 4px) "
                f"| {mejor_info.get('aerolinea','?')} "
                f"| {nivel.upper()}"
            )

            return {
                "tramo":       tramo,
                "precio_px":   precio_px,
                "precio_4px":  mejor_cop,
                "nivel":       nivel,
                "aerolinea":   mejor_info.get("aerolinea", "Ver Skyscanner"),
                "hora":        mejor_info.get("hora", "Ver Skyscanner"),
                "duracion":    mejor_info.get("duracion", ""),
                "escalas":     mejor_info.get("escalas", ""),
            }

        except Exception as e:
            log.warning(f"  Error intento {intento}: {str(e)[:100]}")

    return None

# ── WhatsApp ─────────────────────────────────────────────────
def enviar_whatsapp(ida: dict, vuelta: dict, tasa: float,
                    var_ida: float, var_vuelta: float, es_base: bool = False):
    try:
        client = Client(CONFIG["TWILIO_SID"], CONFIG["TWILIO_TOKEN"])

        total_px   = (ida["precio_px"] + vuelta["precio_px"])
        total_4px  = total_px * CONFIG["PASAJEROS"]
        ahorro_px  = CONFIG["PRECIO_BASE_TOTAL_PX"] - total_px
        ahorro_4px = ahorro_px * CONFIG["PASAJEROS"]

        enc = "PRECIO BASE REGISTRADO" if es_base else "⚡ ALERTA PRECIO BAJO"
        pie = "Monitoreando desde este precio." if es_base else "¡Buen momento para comprar!"

        msg  = f"[{enc}]\n"
        msg += f"Monitor Ruta Carlos — Asia 2026\n"
        msg += f"================================\n"
        msg += f"✈️ IDA (Oct 23)\n"
        msg += f"BOG→SAL→SFO→NRT\n"
        msg += f"Avianca + Zipair\n"
        msg += f"Precio x persona: ${ida['precio_px']:,.0f} COP\n"
        msg += f"Base Carlos:      ${CONFIG['PRECIO_BASE_IDA_PX']:,.0f} COP\n"
        if var_ida != 0:
            msg += f"Variación:        {var_ida:+.1f}%\n"
        msg += f"================================\n"
        msg += f"✈️ VUELTA (Nov 15)\n"
        msg += f"ICN→SFO→SAL→BOG\n"
        msg += f"Air Premia + Avianca\n"
        msg += f"Precio x persona: ${vuelta['precio_px']:,.0f} COP\n"
        msg += f"Base Carlos:      ${CONFIG['PRECIO_BASE_VUELTA_PX']:,.0f} COP\n"
        if var_vuelta != 0:
            msg += f"Variación:        {var_vuelta:+.1f}%\n"
        msg += f"================================\n"
        msg += f"TOTAL x PERSONA: ${total_px:,.0f} COP\n"
        msg += f"TOTAL 4 PERSONAS: ${total_4px:,.0f} COP\n"
        if not es_base and ahorro_px > 0:
            msg += f"AHORRO x px:     ${ahorro_px:,.0f} COP\n"
            msg += f"AHORRO GRUPO:    ${ahorro_4px:,.0f} COP\n"
        msg += f"TRM: ${tasa:,.2f} COP/USD\n"
        msg += f"================================\n"
        msg += f"VER VUELOS:\n"
        msg += f"IDA: {LINKS['ida_skyscanner']}\n"
        msg += f"VUELTA: {LINKS['vuelta_skyscanner']}\n"
        msg += f"================================\n"
        msg += f"{pie}"

        enviados = 0
        for num in CONFIG["WA_NUMEROS"]:
            try:
                client.messages.create(
                    body=msg,
                    from_=CONFIG["WA_DESDE"],
                    to=num,
                )
                log.info(f"  WhatsApp → {num} ✓")
                enviados += 1
            except Exception as e:
                log.error(f"  WhatsApp → {num} ERROR: {e}")
            time.sleep(1)

        return enviados > 0

    except Exception as e:
        log.error(f"  Error WhatsApp: {e}")
        return False

# ── Ciclo principal ───────────────────────────────────────────
def ciclo():
    sep(f"CICLO — {datetime.now().strftime('%A %d/%m/%Y %H:%M:%S')}")
    tasa = tasa_cop()

    # Buscar precio IDA
    ida = buscar_precio_skyscanner(
        "IDA",
        CONFIG["IDA_ORIGEN"],
        CONFIG["IDA_DESTINO"],
        CONFIG["IDA_FECHA"],
        tasa,
    )
    time.sleep(5)

    # Buscar precio VUELTA
    vuelta = buscar_precio_skyscanner(
        "VUELTA",
        CONFIG["VUELTA_ORIGEN"],
        CONFIG["VUELTA_DESTINO"],
        CONFIG["VUELTA_FECHA"],
        tasa,
    )

    if not ida or not vuelta:
        log.warning("No se obtuvieron precios completos. Ciclo omitido.")
        return

    # Calcular variaciones
    ant_ida    = ultimo_precio_local("IDA")
    ant_vuelta = ultimo_precio_local("VUELTA")
    es_primera = ant_ida is None and ant_vuelta is None

    var_ida    = ((ant_ida    - ida["precio_px"])    / ant_ida    * 100) if ant_ida    else 0
    var_vuelta = ((ant_vuelta - vuelta["precio_px"]) / ant_vuelta * 100) if ant_vuelta else 0

    total_px = ida["precio_px"] + vuelta["precio_px"]

    # Determinar si alertar
    alerta = False
    if not es_primera:
        if total_px < CONFIG["PRECIO_BASE_TOTAL_PX"]:
            alerta = True
            log.info(f"  >>> PRECIO TOTAL BAJO DEL BASE DE CARLOS <<<")
        if var_ida >= CONFIG["UMBRAL_BAJADA_PCT"]:
            alerta = True
            log.info(f"  >>> IDA BAJO {var_ida:.1f}% <<<")
        if var_vuelta >= CONFIG["UMBRAL_BAJADA_PCT"]:
            alerta = True
            log.info(f"  >>> VUELTA BAJO {var_vuelta:.1f}% <<<")

    # Guardar en DB local
    for d, ant, var, base in [
        (ida,    ant_ida,    var_ida,    CONFIG["PRECIO_BASE_IDA_PX"]),
        (vuelta, ant_vuelta, var_vuelta, CONFIG["PRECIO_BASE_VUELTA_PX"]),
    ]:
        guardar_local(
            d["tramo"], d["precio_px"], d["precio_4px"],
            tasa, var, d["nivel"], alerta,
        )

    # Guardar en Supabase
    registro = {
        "fecha_consulta":    datetime.now().isoformat(),
        "precio_ida_px":     round(ida["precio_px"]),
        "precio_vuelta_px":  round(vuelta["precio_px"]),
        "precio_total_px":   round(total_px),
        "precio_total_4px":  round(total_px * CONFIG["PASAJEROS"]),
        "tasa_ref":          round(tasa, 2),
        "var_ida_pct":       round(var_ida, 2),
        "var_vuelta_pct":    round(var_vuelta, 2),
        "nivel_ida":         ida["nivel"],
        "nivel_vuelta":      vuelta["nivel"],
        "alerta_enviada":    alerta,
        "es_precio_base":    es_primera,
        "es_manual":         False,
        "aerolinea_ida":     ida["aerolinea"],
        "aerolinea_vuelta":  vuelta["aerolinea"],
    }
    supabase_guardar(registro)

    # Enviar WhatsApp
    if es_primera or alerta:
        ok = enviar_whatsapp(ida, vuelta, tasa, var_ida, var_vuelta,
                             es_base=es_primera)
        log.info(f"  WhatsApp: {'ENVIADO OK' if ok else 'ERROR'}")

    # Resumen
    sep("RESUMEN")
    log.info(f"IDA    Carlos base : ${CONFIG['PRECIO_BASE_IDA_PX']:>14,.0f} COP/px")
    log.info(f"IDA    precio hoy  : ${ida['precio_px']:>14,.0f} COP/px  ({var_ida:+.1f}%)")
    log.info(f"VUELTA Carlos base : ${CONFIG['PRECIO_BASE_VUELTA_PX']:>14,.0f} COP/px")
    log.info(f"VUELTA precio hoy  : ${vuelta['precio_px']:>14,.0f} COP/px  ({var_vuelta:+.1f}%)")
    log.info(f"TOTAL  Carlos base : ${CONFIG['PRECIO_BASE_TOTAL_PX']:>14,.0f} COP/px")
    log.info(f"TOTAL  precio hoy  : ${total_px:>14,.0f} COP/px")
    diff = total_px - CONFIG["PRECIO_BASE_TOTAL_PX"]
    if diff < 0:
        log.info(f"*** MÁS BARATO QUE CARLOS: ${abs(diff):,.0f} COP/px de ahorro ***")
    else:
        log.info(f"Sigue igual o más caro que Carlos (+${diff:,.0f} COP/px)")

    prox = datetime.now() + timedelta(hours=3)
    log.info(f"Próxima revisión: {prox.strftime('%Y-%m-%d %H:%M')}")

# ── Main ─────────────────────────────────────────────────────
if __name__ == "__main__":
    sep("MONITOR RUTA CARLOS — Asia 2026")
    log.info(f"Ruta IDA   : {CONFIG['IDA_ORIGEN']} → {CONFIG['IDA_ESCALAS']} → {CONFIG['IDA_DESTINO']}")
    log.info(f"Ruta VUELTA: {CONFIG['VUELTA_ORIGEN']} → {CONFIG['VUELTA_ESCALAS']} → {CONFIG['VUELTA_DESTINO']}")
    log.info(f"Precio base: ${CONFIG['PRECIO_BASE_TOTAL_PX']:,} COP/px · ${CONFIG['PRECIO_BASE_TOTAL_4PX']:,} COP total 4px")
    log.info(f"Alertas    : Diego + Juliana vía WhatsApp")
    log.info(f"Umbral     : {CONFIG['UMBRAL_BAJADA_PCT']}% de bajada")
    sep()

    try:
        from fast_flights import FlightData, Passengers, get_flights
    except ImportError:
        log.error("pip install fast-flights twilio requests")
        sys.exit(1)

    init_db()
    ciclo()
    log.info("Ejecución completada.")
