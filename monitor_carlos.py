"""
Monitor de Vuelos — Ruta Exacta Carlos
BOG → SAL → SFO → NRT  (Avianca + Zipair)  IDA  · 23 Oct 2026
ICN → SFO → SAL → BOG  (Air Premia + Avianca) VUELTA · 15 Nov 2026
API: fast-flights (Google Flights scraping)
"""

import os, time, sqlite3, logging, sys, re
from datetime import datetime, timedelta
import requests
from twilio.rest import Client

logging.getLogger("twilio").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

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

CONFIG = {
    "TWILIO_SID":   os.getenv("TWILIO_SID",   "AC6ba1ba9733df887c4c44cfe5b0696124"),
    "TWILIO_TOKEN": os.getenv("TWILIO_TOKEN",  "b8c60fbeb944e468af52a06f78b66fca"),
    "WA_DESDE":     "whatsapp:+14155238886",
    "WA_NUMEROS": [
        "whatsapp:+573102745611",
        "whatsapp:+573144624739",
    ],
    "SUPABASE_URL": os.getenv("SUPABASE_URL", "https://qalxstxtuuvybudkqtpd.supabase.co"),
    "SUPABASE_KEY": os.getenv("SUPABASE_KEY", "sb_secret_O5MF06-7ueAeDlo8zLg4YQ_LU4VaycC"),

    "IDA_ORIGEN":      "BOG",
    "IDA_DESTINO":     "NRT",
    "IDA_FECHA":       "2026-10-23",
    "IDA_AEROLINEAS":  "Avianca + Zipair",
    "IDA_FILTRO":      ["avianca", "zipair"],

    "VUELTA_ORIGEN":     "ICN",
    "VUELTA_DESTINO":    "BOG",
    "VUELTA_FECHA":      "2026-11-15",
    "VUELTA_AEROLINEAS": "Air Premia + Avianca",
    "VUELTA_FILTRO":     ["air premia", "airpremia", "avianca"],

    "PRECIO_BASE_IDA_PX":    3_160_141,
    "PRECIO_BASE_VUELTA_PX": 3_163_941,
    "PRECIO_BASE_TOTAL_PX":  6_324_082,
    "PRECIO_BASE_TOTAL_4PX": 25_296_328,

    "PASAJEROS":         4,
    "UMBRAL_BAJADA_PCT": 3,
    "DB":                "monitor_carlos.db",
}

LINKS = {
    "ida":    "https://www.skyscanner.com.co/transporte/vuelos/bog/nrt/261023/?adults=4&cabinclass=economy",
    "vuelta": "https://www.skyscanner.com.co/transporte/vuelos/icn/bog/261115/?adults=4&cabinclass=economy",
}

# ── Supabase ─────────────────────────────────────────────────
def supabase_guardar(registro: dict):
    url = CONFIG["SUPABASE_URL"]
    key = CONFIG["SUPABASE_KEY"]
    if not url or not key:
        return
    try:
        r = requests.post(
            f"{url}/rest/v1/monitor_carlos",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            json=registro, timeout=15,
        )
        if r.status_code in (200, 201):
            log.info("  Supabase: guardado OK")
        else:
            log.warning(f"  Supabase: {r.status_code} {r.text[:80]}")
    except Exception as e:
        log.warning(f"  Supabase error: {e}")

# ── DB local ─────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(CONFIG["DB"])
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS consultas (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha_consulta TEXT,
            tramo          TEXT,
            precio_cop_px  REAL,
            tasa_ref       REAL,
            variacion_pct  REAL,
            es_exacto      INTEGER DEFAULT 1,
            alerta_enviada INTEGER DEFAULT 0
        )""")
    conn.commit()
    conn.close()

def guardar_local(tramo, px, tasa, var, es_exacto, alerta):
    conn = sqlite3.connect(CONFIG["DB"])
    conn.cursor().execute(
        "INSERT INTO consultas VALUES (NULL,?,?,?,?,?,?,?)",
        (datetime.now().isoformat(), tramo, px, tasa, var,
         1 if es_exacto else 0, 1 if alerta else 0)
    )
    conn.commit()
    conn.close()

def ultimo_precio_local(tramo):
    conn = sqlite3.connect(CONFIG["DB"])
    c = conn.cursor()
    c.execute("SELECT precio_cop_px FROM consultas WHERE tramo=? ORDER BY fecha_consulta DESC LIMIT 1", (tramo,))
    r = c.fetchone()
    conn.close()
    return r[0] if r else None

# ── TRM ──────────────────────────────────────────────────────
def tasa_cop():
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10)
        t = r.json()["rates"]["COP"]
        log.info(f"TRM: ${t:,.2f} COP/USD")
        return t
    except Exception as e:
        log.warning(f"TRM fallback $4,200: {e}")
        return 4_200.0

# ── Buscar precio con fast-flights ────────────────────────────
def buscar_precio(tramo: str, origen: str, destino: str,
                  fecha: str, tasa: float):
    try:
        from fast_flights import FlightData, Passengers, get_flights
    except ImportError:
        log.error("fast-flights no instalado.")
        return None

    filtro     = CONFIG["IDA_FILTRO"]     if tramo == "IDA" else CONFIG["VUELTA_FILTRO"]
    aero_label = CONFIG["IDA_AEROLINEAS"] if tramo == "IDA" else CONFIG["VUELTA_AEROLINEAS"]

    log.info(f"  Buscando {tramo}: {origen}→{destino} · {fecha} · {aero_label}")

    for intento in range(1, 4):
        try:
            if intento > 1:
                log.info(f"  Reintento {intento}/3 ...")
                time.sleep(12)

            result = get_flights(
                flight_data=[FlightData(date=fecha, from_airport=origen, to_airport=destino)],
                trip="one-way",
                seat="economy",
                passengers=Passengers(adults=CONFIG["PASAJEROS"]),
                fetch_mode="fallback",
            )

            if not result or not result.flights:
                log.warning(f"  Sin resultados para {tramo}")
                continue

            log.info(f"  {len(result.flights)} vuelos encontrados")

            # Separar objetivo vs fallback
            mejor_obj_px  = None
            mejor_obj_info = {}
            mejor_fb_px   = None
            mejor_fb_info  = {}

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

                # fast-flights devuelve precio total 4px en USD
                precio_cop_total = precio * tasa if precio < 50_000 else precio
                precio_px = precio_cop_total / CONFIG["PASAJEROS"]

                aero = str(getattr(v, "name", "") or "").lower()
                es_objetivo = any(f in aero for f in filtro)

                log.info(f"    {'OK' if es_objetivo else '--'} {aero} → ${precio_px:,.0f} COP/px")

                if es_objetivo:
                    if mejor_obj_px is None or precio_px < mejor_obj_px:
                        mejor_obj_px = precio_px
                        mejor_obj_info = {
                            "aerolinea":  aero_label,
                            "precio_4px": precio_cop_total,
                            "es_exacto":  True,
                        }
                else:
                    if mejor_fb_px is None or precio_px < mejor_fb_px:
                        mejor_fb_px = precio_px
                        mejor_fb_info = {
                            "aerolinea":  str(getattr(v, "name", "") or "Estimado"),
                            "precio_4px": precio_cop_total,
                            "es_exacto":  False,
                        }

            # Usar objetivo si existe, sino fallback con advertencia
            if mejor_obj_px is not None:
                px   = mejor_obj_px
                info = mejor_obj_info
                log.info(f"  RESULTADO {tramo} (EXACTO): ${px:,.0f} COP/px")
            elif mejor_fb_px is not None:
                px   = mejor_fb_px
                info = mejor_fb_info
                log.warning(f"  RESULTADO {tramo} (ESTIMADO — no encontró {aero_label}): ${px:,.0f} COP/px")
            else:
                log.warning(f"  {tramo}: Sin precio válido")
                continue

            nivel = str(getattr(result, "current_price", "typical")).lower()
            return {
                "tramo":      tramo,
                "precio_px":  px,
                "precio_4px": info["precio_4px"],
                "aerolinea":  info["aerolinea"],
                "es_exacto":  info["es_exacto"],
                "nivel":      nivel,
            }

        except Exception as e:
            log.warning(f"  Error intento {intento}: {str(e)[:120]}")

    log.warning(f"  {tramo}: No se pudo obtener precio tras 3 intentos")
    return None

# ── WhatsApp ─────────────────────────────────────────────────
def enviar_whatsapp(ida, vuelta, tasa, var_ida, var_vuelta, es_base=False):
    try:
        client    = Client(CONFIG["TWILIO_SID"], CONFIG["TWILIO_TOKEN"])
        total_px  = ida["precio_px"] + vuelta["precio_px"]
        total_4px = total_px * CONFIG["PASAJEROS"]
        ahorro_px = CONFIG["PRECIO_BASE_TOTAL_PX"] - total_px
        enc = "PRECIO BASE" if es_base else "ALERTA PRECIO BAJO"
        pie = "Monitoreando." if es_base else "Buen momento para comprar!"

        vuelta_nota = "" if vuelta["es_exacto"] else " (*estimado)"

        msg  = f"[{enc}] Monitor Ruta Carlos — Asia 2026\n"
        msg += f"IDA (Oct 23) BOG->NRT Avianca+Zipair\n"
        msg += f"Precio/px: ${ida['precio_px']:,.0f} COP\n"
        msg += f"Base Carlos: ${CONFIG['PRECIO_BASE_IDA_PX']:,.0f} COP\n"
        if var_ida: msg += f"Variacion: {var_ida:+.1f}%\n"
        msg += f"VUELTA (Nov 15) ICN->BOG AirPremi+Avianca{vuelta_nota}\n"
        msg += f"Precio/px: ${vuelta['precio_px']:,.0f} COP\n"
        msg += f"Base Carlos: ${CONFIG['PRECIO_BASE_VUELTA_PX']:,.0f} COP\n"
        if var_vuelta: msg += f"Variacion: {var_vuelta:+.1f}%\n"
        msg += f"TOTAL/px: ${total_px:,.0f} | 4px: ${total_4px:,.0f} COP\n"
        if not es_base and ahorro_px > 0:
            msg += f"AHORRO: ${ahorro_px:,.0f}/px | ${ahorro_px*4:,.0f} grupo\n"
        msg += f"TRM: ${tasa:,.0f}\n"
        msg += f"IDA: {LINKS['ida']}\nVUELTA: {LINKS['vuelta']}\n{pie}"

        enviados = 0
        for num in CONFIG["WA_NUMEROS"]:
            try:
                client.messages.create(body=msg, from_=CONFIG["WA_DESDE"], to=num)
                log.info(f"  WhatsApp -> {num} OK")
                enviados += 1
            except Exception as e:
                log.error(f"  WhatsApp -> {num} ERROR: {e}")
            time.sleep(1)
        return enviados > 0
    except Exception as e:
        log.error(f"  WhatsApp error: {e}")
        return False

# ── Ciclo ─────────────────────────────────────────────────────
def ciclo():
    sep(f"CICLO — {datetime.now().strftime('%A %d/%m/%Y %H:%M:%S')}")
    tasa = tasa_cop()

    ida = buscar_precio(
        "IDA", CONFIG["IDA_ORIGEN"], CONFIG["IDA_DESTINO"], CONFIG["IDA_FECHA"], tasa
    )
    time.sleep(8)
    vuelta = buscar_precio(
        "VUELTA", CONFIG["VUELTA_ORIGEN"], CONFIG["VUELTA_DESTINO"], CONFIG["VUELTA_FECHA"], tasa
    )

    if not ida or not vuelta:
        log.warning("No se obtuvieron precios. Ciclo omitido.")
        return

    ant_ida    = ultimo_precio_local("IDA")
    ant_vuelta = ultimo_precio_local("VUELTA")
    es_primera = ant_ida is None and ant_vuelta is None

    var_ida    = ((ant_ida    - ida["precio_px"])    / ant_ida    * 100) if ant_ida    else 0
    var_vuelta = ((ant_vuelta - vuelta["precio_px"]) / ant_vuelta * 100) if ant_vuelta else 0
    total_px   = ida["precio_px"] + vuelta["precio_px"]

    alerta = False
    if not es_primera:
        if total_px < CONFIG["PRECIO_BASE_TOTAL_PX"]: alerta = True
        if var_ida    >= CONFIG["UMBRAL_BAJADA_PCT"]:  alerta = True
        if var_vuelta >= CONFIG["UMBRAL_BAJADA_PCT"]:  alerta = True

    guardar_local("IDA",    ida["precio_px"],    tasa, var_ida,    ida["es_exacto"],    alerta)
    guardar_local("VUELTA", vuelta["precio_px"], tasa, var_vuelta, vuelta["es_exacto"], alerta)

    supabase_guardar({
        "fecha_consulta":   datetime.now().isoformat(),
        "precio_ida_px":    round(ida["precio_px"]),
        "precio_vuelta_px": round(vuelta["precio_px"]),
        "precio_total_px":  round(total_px),
        "precio_total_4px": round(total_px * CONFIG["PASAJEROS"]),
        "tasa_ref":         round(tasa, 2),
        "var_ida_pct":      round(var_ida, 2),
        "var_vuelta_pct":   round(var_vuelta, 2),
        "nivel_ida":        ida["nivel"],
        "nivel_vuelta":     vuelta["nivel"],
        "alerta_enviada":   alerta,
        "es_precio_base":   es_primera,
        "es_manual":        False,
        "aerolinea_ida":    ida["aerolinea"],
        "aerolinea_vuelta": vuelta["aerolinea"],
    })

    if es_primera or alerta:
        ok = enviar_whatsapp(ida, vuelta, tasa, var_ida, var_vuelta, es_base=es_primera)
        log.info(f"  WhatsApp: {'OK' if ok else 'ERROR'}")

    sep("RESUMEN")
    log.info(f"IDA    base : ${CONFIG['PRECIO_BASE_IDA_PX']:>14,.0f} COP/px")
    log.info(f"IDA    hoy  : ${ida['precio_px']:>14,.0f} COP/px  ({var_ida:+.1f}%) {'EXACTO' if ida['es_exacto'] else 'estimado'}")
    log.info(f"VUELTA base : ${CONFIG['PRECIO_BASE_VUELTA_PX']:>14,.0f} COP/px")
    log.info(f"VUELTA hoy  : ${vuelta['precio_px']:>14,.0f} COP/px  ({var_vuelta:+.1f}%) {'EXACTO' if vuelta['es_exacto'] else 'estimado'}")
    log.info(f"TOTAL  base : ${CONFIG['PRECIO_BASE_TOTAL_PX']:>14,.0f} COP/px")
    log.info(f"TOTAL  hoy  : ${total_px:>14,.0f} COP/px")
    diff = total_px - CONFIG["PRECIO_BASE_TOTAL_PX"]
    if diff < 0:
        log.info(f"*** MAS BARATO QUE CARLOS: ${abs(diff):,.0f}/px ***")
    else:
        log.info(f"Mas caro que Carlos: +${diff:,.0f}/px")
    log.info(f"Proxima: {(datetime.now()+timedelta(hours=3)).strftime('%Y-%m-%d %H:%M')}")

# ── Main ─────────────────────────────────────────────────────
if __name__ == "__main__":
    sep("MONITOR RUTA CARLOS — Asia 2026")
    log.info(f"IDA   : {CONFIG['IDA_ORIGEN']}→{CONFIG['IDA_DESTINO']} · {CONFIG['IDA_FECHA']}")
    log.info(f"VUELTA: {CONFIG['VUELTA_ORIGEN']}→{CONFIG['VUELTA_DESTINO']} · {CONFIG['VUELTA_FECHA']}")
    log.info(f"Base  : ${CONFIG['PRECIO_BASE_TOTAL_PX']:,} COP/px")
    log.info(f"API   : fast-flights (Google Flights)")
    sep()
    init_db()
    ciclo()
    log.info("Ejecucion completada.")
