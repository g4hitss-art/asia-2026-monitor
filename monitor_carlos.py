"""
Monitor de Vuelos — Ruta Exacta Carlos
BOG → SAL → SFO → NRT  (Avianca + Zipair)  IDA  · 23 Oct 2026
ICN → SFO → SAL → BOG  (Air Premia + Avianca) VUELTA · 15 Nov 2026

API: Sky Scrapper (Skyscanner) via RapidAPI
"""

import os, time, sqlite3, logging, sys
from datetime import datetime, timedelta
import requests
from twilio.rest import Client

# ── Silenciar logs verbosos ──────────────────────────────────
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
    # RapidAPI — Sky Scrapper
    "RAPIDAPI_KEY":  os.getenv("RAPIDAPI_KEY", "18c21bf708msh8410cf978470aadp1af9ccjsnc83820ca5e5e"),
    "RAPIDAPI_HOST": "sky-scrapper.p.rapidapi.com",

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
    "IDA_ORIGEN":      "BOG",
    "IDA_DESTINO":     "NRT",
    "IDA_FECHA":       "2026-10-23",
    "IDA_AEROLINEAS":  "Avianca + Zipair",
    "IDA_FILTRO":      ["avianca", "zipair"],

    # Ruta Carlos — VUELTA
    "VUELTA_ORIGEN":    "ICN",
    "VUELTA_DESTINO":   "BOG",
    "VUELTA_FECHA":     "2026-11-15",
    "VUELTA_AEROLINEAS": "Air Premia + Avianca",
    "VUELTA_FILTRO":    ["air premia", "airpremia", "avianca"],

    # Precios base Carlos (por persona)
    "PRECIO_BASE_IDA_PX":    3_160_141,
    "PRECIO_BASE_VUELTA_PX": 3_163_941,
    "PRECIO_BASE_TOTAL_PX":  6_324_082,
    "PRECIO_BASE_TOTAL_4PX": 25_296_328,

    "PASAJEROS":         4,
    "UMBRAL_BAJADA_PCT": 3,
    "DB":                "monitor_carlos.db",
}

HEADERS_RAPID = {
    "x-rapidapi-key":  CONFIG["RAPIDAPI_KEY"],
    "x-rapidapi-host": CONFIG["RAPIDAPI_HOST"],
}

BASE_URL = f"https://{CONFIG['RAPIDAPI_HOST']}"

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
            json=registro,
            timeout=15,
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
            alerta_enviada INTEGER DEFAULT 0
        )""")
    conn.commit()
    conn.close()

def guardar_local(tramo, px, tasa, var, alerta):
    conn = sqlite3.connect(CONFIG["DB"])
    conn.cursor().execute(
        "INSERT INTO consultas VALUES (NULL,?,?,?,?,?,?)",
        (datetime.now().isoformat(), tramo, px, tasa, var, 1 if alerta else 0)
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

# ── Sky Scrapper: entityId de aeropuerto ─────────────────────
_entity_cache = {}

def get_entity_id(sky_id: str) -> tuple:
    if sky_id in _entity_cache:
        return _entity_cache[sky_id]
    try:
        r = requests.get(
            f"{BASE_URL}/api/v1/flights/searchAirport",
            headers=HEADERS_RAPID,
            params={"query": sky_id, "locale": "en-US"},
            timeout=15,
        )
        places = r.json().get("data", [])
        for p in places:
            if p.get("skyId", "").upper() == sky_id.upper():
                eid = p.get("entityId", "")
                _entity_cache[sky_id] = (sky_id, eid)
                log.info(f"  Airport {sky_id}: entityId={eid}")
                return sky_id, eid
        if places:
            p = places[0]
            sky = p.get("skyId", sky_id)
            eid = p.get("entityId", "")
            _entity_cache[sky_id] = (sky, eid)
            log.info(f"  Airport {sky_id} (fallback): entityId={eid}")
            return sky, eid
    except Exception as e:
        log.warning(f"  Error entityId {sky_id}: {e}")
    return sky_id, ""

# ── Sky Scrapper: buscar vuelos ───────────────────────────────
def buscar_precio(tramo: str, origen: str, destino: str,
                  fecha: str, tasa: float) -> dict | None:

    filtro     = CONFIG["IDA_FILTRO"]     if tramo == "IDA" else CONFIG["VUELTA_FILTRO"]
    aero_label = CONFIG["IDA_AEROLINEAS"] if tramo == "IDA" else CONFIG["VUELTA_AEROLINEAS"]

    log.info(f"  Buscando {tramo}: {origen}→{destino} · {fecha} · {aero_label}")

    orig_sky, orig_eid = get_entity_id(origen)
    dest_sky, dest_eid = get_entity_id(destino)

    if not orig_eid or not dest_eid:
        log.warning(f"  No se pudieron obtener entityIds para {origen}/{destino}")
        return None

    for intento in range(1, 4):
        try:
            if intento > 1:
                log.info(f"  Reintento {intento}/3 ...")
                time.sleep(15)

            r = requests.get(
                f"{BASE_URL}/api/v2/flights/searchFlightsComplete",
                headers=HEADERS_RAPID,
                params={
                    "originSkyId":         orig_sky,
                    "destinationSkyId":    dest_sky,
                    "originEntityId":      orig_eid,
                    "destinationEntityId": dest_eid,
                    "date":                fecha,
                    "adults":              str(CONFIG["PASAJEROS"]),
                    "cabinClass":          "economy",
                    "currency":            "USD",
                    "market":              "CO",
                    "countryCode":         "CO",
                    "locale":              "es-CO",
                    "sortBy":              "best",
                },
                timeout=30,
            )

            if r.status_code != 200:
                log.warning(f"  HTTP {r.status_code}: {r.text[:100]}")
                continue

            itineraries = r.json().get("data", {}).get("itineraries", [])

            if not itineraries:
                log.warning(f"  Sin itinerarios para {tramo}")
                continue

            log.info(f"  {len(itineraries)} itinerarios encontrados")

            mejor_px   = None
            mejor_info = {}

            for it in itineraries:
                # Precio en USD
                price_obj = it.get("price", {})
                raw = price_obj.get("raw") or price_obj.get("formatted", "")
                try:
                    precio_usd = float(str(raw).replace("$","").replace(",","").strip())
                except Exception:
                    continue

                # Aerolíneas del itinerario
                legs  = it.get("legs", [])
                aeros = []
                for leg in legs:
                    for c in leg.get("carriers", {}).get("marketing", []):
                        n = c.get("name", "").lower()
                        if n:
                            aeros.append(n)

                aero_str   = " + ".join(aeros)
                es_objetivo = any(any(f in a for f in filtro) for a in aeros)

                log.info(f"    {'✅' if es_objetivo else '❌'} {aero_str} → ${precio_usd:,.0f} USD")

                if not es_objetivo:
                    continue

                precio_cop_total = precio_usd * tasa
                precio_px        = precio_cop_total / CONFIG["PASAJEROS"]

                if mejor_px is None or precio_px < mejor_px:
                    mejor_px   = precio_px
                    leg0       = legs[0] if legs else {}
                    mejor_info = {
                        "precio_4px": precio_cop_total,
                        "hora":       leg0.get("departure", ""),
                        "duracion":   leg0.get("durationInMinutes", ""),
                    }

            if mejor_px is None:
                log.warning(f"  {tramo}: No encontró {aero_label} en resultados")
                continue

            log.info(f"  ✅ {tramo}: ${mejor_px:,.0f} COP/px · ${mejor_info['precio_4px']:,.0f} COP total 4px")
            return {
                "tramo":      tramo,
                "precio_px":  mejor_px,
                "precio_4px": mejor_info["precio_4px"],
                "aerolinea":  aero_label,
                "hora":       str(mejor_info["hora"]),
                "nivel":      "typical",
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

        enc = "PRECIO BASE REGISTRADO" if es_base else "ALERTA PRECIO BAJO"
        pie = "Monitoreando desde este precio." if es_base else "Buen momento para comprar!"

        msg  = f"[{enc}] Monitor Ruta Carlos — Asia 2026\n"
        msg += f"{'='*34}\n"
        msg += f"IDA (Oct 23) BOG→SAL→SFO→NRT · Avianca+Zipair\n"
        msg += f"Precio/px: ${ida['precio_px']:,.0f} COP\n"
        msg += f"Base Carlos: ${CONFIG['PRECIO_BASE_IDA_PX']:,.0f} COP\n"
        if var_ida: msg += f"Variacion: {var_ida:+.1f}%\n"
        msg += f"{'='*34}\n"
        msg += f"VUELTA (Nov 15) ICN→SFO→SAL→BOG · AirPremi+Avianca\n"
        msg += f"Precio/px: ${vuelta['precio_px']:,.0f} COP\n"
        msg += f"Base Carlos: ${CONFIG['PRECIO_BASE_VUELTA_PX']:,.0f} COP\n"
        if var_vuelta: msg += f"Variacion: {var_vuelta:+.1f}%\n"
        msg += f"{'='*34}\n"
        msg += f"TOTAL/px: ${total_px:,.0f} COP\n"
        msg += f"TOTAL 4px: ${total_4px:,.0f} COP\n"
        if not es_base and ahorro_px > 0:
            msg += f"AHORRO/px: ${ahorro_px:,.0f} COP\n"
        msg += f"TRM: ${tasa:,.0f}\n"
        msg += f"IDA: {LINKS['ida']}\nVUELTA: {LINKS['vuelta']}\n{pie}"

        enviados = 0
        for num in CONFIG["WA_NUMEROS"]:
            try:
                client.messages.create(body=msg, from_=CONFIG["WA_DESDE"], to=num)
                log.info(f"  WhatsApp → {num} OK")
                enviados += 1
            except Exception as e:
                log.error(f"  WhatsApp → {num} ERROR: {e}")
            time.sleep(1)
        return enviados > 0
    except Exception as e:
        log.error(f"  WhatsApp error: {e}")
        return False

# ── Ciclo principal ───────────────────────────────────────────
def ciclo():
    sep(f"CICLO — {datetime.now().strftime('%A %d/%m/%Y %H:%M:%S')}")
    tasa = tasa_cop()

    ida    = buscar_precio("IDA",    CONFIG["IDA_ORIGEN"],    CONFIG["IDA_DESTINO"],    CONFIG["IDA_FECHA"],    tasa)
    time.sleep(5)
    vuelta = buscar_precio("VUELTA", CONFIG["VUELTA_ORIGEN"], CONFIG["VUELTA_DESTINO"], CONFIG["VUELTA_FECHA"], tasa)

    if not ida or not vuelta:
        log.warning("No se obtuvieron precios completos. Ciclo omitido.")
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
        if var_ida    >= CONFIG["UMBRAL_BAJADA_PCT"]: alerta = True
        if var_vuelta >= CONFIG["UMBRAL_BAJADA_PCT"]: alerta = True

    guardar_local("IDA",    ida["precio_px"],    tasa, var_ida,    alerta)
    guardar_local("VUELTA", vuelta["precio_px"], tasa, var_vuelta, alerta)

    supabase_guardar({
        "fecha_consulta":   datetime.now().isoformat(),
        "precio_ida_px":    round(ida["precio_px"]),
        "precio_vuelta_px": round(vuelta["precio_px"]),
        "precio_total_px":  round(total_px),
        "precio_total_4px": round(total_px * CONFIG["PASAJEROS"]),
        "tasa_ref":         round(tasa, 2),
        "var_ida_pct":      round(var_ida, 2),
        "var_vuelta_pct":   round(var_vuelta, 2),
        "nivel_ida":        "typical",
        "nivel_vuelta":     "typical",
        "alerta_enviada":   alerta,
        "es_precio_base":   es_primera,
        "es_manual":        False,
        "aerolinea_ida":    "Avianca + Zipair",
        "aerolinea_vuelta": "Air Premia + Avianca",
    })

    if es_primera or alerta:
        ok = enviar_whatsapp(ida, vuelta, tasa, var_ida, var_vuelta, es_base=es_primera)
        log.info(f"  WhatsApp: {'OK' if ok else 'ERROR'}")

    sep("RESUMEN")
    log.info(f"IDA    base : ${CONFIG['PRECIO_BASE_IDA_PX']:>14,.0f} COP/px")
    log.info(f"IDA    hoy  : ${ida['precio_px']:>14,.0f} COP/px  ({var_ida:+.1f}%)")
    log.info(f"VUELTA base : ${CONFIG['PRECIO_BASE_VUELTA_PX']:>14,.0f} COP/px")
    log.info(f"VUELTA hoy  : ${vuelta['precio_px']:>14,.0f} COP/px  ({var_vuelta:+.1f}%)")
    log.info(f"TOTAL  base : ${CONFIG['PRECIO_BASE_TOTAL_PX']:>14,.0f} COP/px")
    log.info(f"TOTAL  hoy  : ${total_px:>14,.0f} COP/px")
    diff = total_px - CONFIG["PRECIO_BASE_TOTAL_PX"]
    if diff < 0:
        log.info(f"*** MAS BARATO QUE CARLOS: ${abs(diff):,.0f}/px ***")
    else:
        log.info(f"Mas caro que Carlos: +${diff:,.0f}/px")
    log.info(f"Proxima revision: {(datetime.now()+timedelta(hours=3)).strftime('%Y-%m-%d %H:%M')}")

# ── Main ─────────────────────────────────────────────────────
if __name__ == "__main__":
    sep("MONITOR RUTA CARLOS — Asia 2026")
    log.info(f"IDA   : {CONFIG['IDA_ORIGEN']}→{CONFIG['IDA_DESTINO']} · {CONFIG['IDA_FECHA']}")
    log.info(f"VUELTA: {CONFIG['VUELTA_ORIGEN']}→{CONFIG['VUELTA_DESTINO']} · {CONFIG['VUELTA_FECHA']}")
    log.info(f"Base  : ${CONFIG['PRECIO_BASE_TOTAL_PX']:,} COP/px")
    log.info(f"API   : Sky Scrapper (Skyscanner) via RapidAPI")
    sep()
    init_db()
    ciclo()
    log.info("Ejecucion completada.")
