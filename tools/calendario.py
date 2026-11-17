#!/usr/bin/env python3
"""Calendarios para los scripts de discos: Radicale (CalDAV), Google o los dos.

CALENDAR_BACKENDS elige dónde se escribe: «radicale» (por defecto), «google» o
«radicale,google». Cada script usa lo que le toca:

  add_release_calendar.py         eventos de día completo con cada lanzamiento
      Radicale: calendario RELEASES_CALENDAR_NAME de RADICALE_URL
      Google:   calendario GOOGLE_CALENDAR_ID (uno tuyo, no hace falta que sea el principal)
  discos_escuchados_calendario.py marca como hechas las tareas «Artista - Disco» ya escuchadas
      Radicale: tareas (VTODO) del calendario CALENDAR_NAME
      Google:   lista de Google Tasks GOOGLE_TASKLIST_ID (Google Calendar no tiene tareas)

Credenciales de Google (GOOGLE_CREDENTIALS_FILE, un JSON de Google Cloud):
  - Cuenta de servicio: compartes tu calendario con su correo («Hacer cambios en
    eventos») y no hay que autorizar nada. Solo sirve para el calendario, no para Tasks.
  - Cliente OAuth «App de escritorio»: autorizas una vez con
        python3 calendario.py autorizar
    y el permiso queda en GOOGLE_TOKEN_FILE. Sirve para el calendario y para Tasks.

Otras órdenes:
    python3 calendario.py calendarios    lista tus calendarios de Google y su id
    python3 calendario.py listas         lista tus listas de Google Tasks y su id
"""
import json
import os
import sys
from datetime import date, datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
SCOPES_CALENDAR = ["https://www.googleapis.com/auth/calendar"]
SCOPES_OAUTH = ["https://www.googleapis.com/auth/calendar", "https://www.googleapis.com/auth/tasks"]


def backends():
    out = [b.strip().lower() for b in os.getenv("CALENDAR_BACKENDS", "radicale").split(",") if b.strip()]
    unknown = [b for b in out if b not in ("radicale", "google")]
    if unknown:
        raise SystemExit(f"CALENDAR_BACKENDS: no conozco {', '.join(unknown)} (usa radicale y/o google)")
    return out


def _name(cal):
    """Nombre de un calendario CalDAV (get_display_name en caldav reciente, name en el antiguo)."""
    try:
        return cal.get_display_name()
    except Exception:
        return getattr(cal, "name", "") or ""


def as_date(value):
    return value.date() if isinstance(value, datetime) else value


# ── Radicale (CalDAV) ────────────────────────────────────────────────────────

class RadicaleCalendar:
    """Eventos de día completo en un calendario CalDAV, buscado por nombre."""
    label = "Radicale"

    def __init__(self, url, username, password, name, create=True):
        from caldav import DAVClient
        principal = DAVClient(url, username=username, password=password).principal()
        self.calendar = next((c for c in principal.calendars() if _name(c) == name), None)
        if self.calendar is None:
            if not create:
                raise RuntimeError(f"Radicale: no existe el calendario «{name}»")
            print(f"Radicale: creo el calendario «{name}»")
            self.calendar = principal.make_calendar(name=name)

    def events(self, start=None, end=None):
        """[(resumen, fecha, evento)] de todo el calendario."""
        from icalendar import Calendar
        out = []
        for ev in self.calendar.events():
            try:
                for comp in Calendar.from_ical(ev.data).walk("VEVENT"):
                    out.append((str(comp.get("SUMMARY")).strip(), as_date(comp.get("DTSTART").dt), ev))
            except Exception as e:
                print(f"Radicale: no pude leer un evento: {e}")
        return out

    def add_all_day(self, summary, day, description=""):
        from icalendar import Event
        ev = Event()
        ev.add("summary", summary)
        ev.add("dtstart", day)
        ev.add("dtend", day)
        ev["dtstart"].params["VALUE"] = "DATE"
        ev["dtend"].params["VALUE"] = "DATE"
        if description:
            ev.add("description", description)
        self.calendar.save_event(ev.to_ical())

    def delete(self, handle):
        handle.delete()


class RadicaleTasks:
    """Tareas (VTODO) pendientes de un calendario CalDAV."""
    label = "Radicale"

    def __init__(self, url, username, password, name):
        from caldav import DAVClient
        calendars = DAVClient(url=url, username=username, password=password).principal().calendars()
        # Primero el nombre visible exacto; si no, el último tramo de la URL (sin subcadenas:
        # «discos» no debe coger «discos_pendientes»)
        slug = lambda c: str(c.url).rstrip("/").rsplit("/", 1)[-1]
        self.calendar = (next((c for c in calendars if _name(c) == name), None)
                         or next((c for c in calendars if slug(c) == name), None))
        if self.calendar is None:
            if not calendars:
                raise RuntimeError("Radicale: no hay calendarios")
            self.calendar = calendars[0]
            print(f"⚠ Calendario '{name}' no encontrado, usando: {_name(self.calendar) or 'el primero'}")

    def pending(self):
        """[(resumen, tarea)] de las tareas sin completar."""
        from icalendar import Calendar
        out = []
        for todo in self.calendar.todos(include_completed=False):
            try:
                for comp in Calendar.from_ical(todo.data).walk("VTODO"):
                    out.append((str(comp.get("summary", "")), todo))
            except Exception as e:
                print(f"⚠ Error procesando tarea: {e}")
        return out

    def complete(self, todo):
        import pytz
        from icalendar import Calendar, vDatetime
        cal = Calendar.from_ical(todo.data)
        for comp in cal.walk("VTODO"):
            comp["status"] = "COMPLETED"
            comp["percent-complete"] = 100
            comp["completed"] = vDatetime(datetime.now(pytz.UTC))
        todo.data = cal.to_ical()
        todo.save()


# ── Google ───────────────────────────────────────────────────────────────────

def _credentials_file():
    path = os.getenv("GOOGLE_CREDENTIALS_FILE", "")
    if not path:
        raise RuntimeError("Google: falta GOOGLE_CREDENTIALS_FILE (JSON de cuenta de servicio o de cliente OAuth)")
    if not os.path.isabs(path):
        path = os.path.join(HERE, path)
    if not os.path.exists(path):
        raise RuntimeError(f"Google: no existe {path}")
    return path


def _token_file():
    path = os.getenv("GOOGLE_TOKEN_FILE", "google_token.json")
    return path if os.path.isabs(path) else os.path.join(HERE, path)


def google_credentials(need_tasks=False):
    path = _credentials_file()
    with open(path) as f:
        kind = json.load(f).get("type")
    if kind == "service_account":
        if need_tasks:
            raise RuntimeError("Google Tasks no admite cuentas de servicio: usa un cliente OAuth y "
                               "«python3 calendario.py autorizar»")
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_file(path, scopes=SCOPES_CALENDAR)
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    token = _token_file()
    creds = Credentials.from_authorized_user_file(token, SCOPES_OAUTH) if os.path.exists(token) else None
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _save_token(creds)
        return creds
    raise RuntimeError("Google: falta autorizar el acceso. Lanza «python3 calendario.py autorizar» "
                       "en un equipo con navegador y copia el fichero de GOOGLE_TOKEN_FILE junto al script")


def _save_token(creds):
    token = _token_file()
    with open(token, "w") as f:
        f.write(creds.to_json())
    os.chmod(token, 0o600)


def _service(name, version, need_tasks=False):
    from googleapiclient.discovery import build
    return build(name, version, credentials=google_credentials(need_tasks), cache_discovery=False)


class GoogleCalendar:
    """Eventos de día completo en un calendario concreto de Google."""
    label = "Google Calendar"

    def __init__(self, calendar_id):
        if not calendar_id:
            raise RuntimeError("Google: falta GOOGLE_CALENDAR_ID (lístalos con «python3 calendario.py calendarios»)")
        self.calendar_id = calendar_id
        self.svc = _service("calendar", "v3")

    def events(self, start, end):
        """[(resumen, fecha, id)] entre start y end (fechas, end incluida)."""
        out, page = [], None
        while True:
            r = self.svc.events().list(
                calendarId=self.calendar_id, singleEvents=True, maxResults=2500, pageToken=page,
                timeMin=f"{start.isoformat()}T00:00:00Z",
                timeMax=f"{(end + timedelta(days=1)).isoformat()}T00:00:00Z").execute()
            for ev in r.get("items", []):
                when = ev.get("start", {})
                day = when.get("date") or (when.get("dateTime") or "")[:10]
                if day:
                    out.append(((ev.get("summary") or "").strip(), date.fromisoformat(day), ev["id"]))
            page = r.get("nextPageToken")
            if not page:
                return out

    def add_all_day(self, summary, day, description=""):
        self.svc.events().insert(calendarId=self.calendar_id, body={
            "summary": summary, "description": description,
            "start": {"date": day.isoformat()}, "end": {"date": (day + timedelta(days=1)).isoformat()},
            "transparency": "transparent",  # no te marca como ocupado
        }).execute()

    def delete(self, event_id):
        self.svc.events().delete(calendarId=self.calendar_id, eventId=event_id).execute()


class GoogleTasks:
    """Tareas pendientes de una lista de Google Tasks."""
    label = "Google Tasks"

    def __init__(self, tasklist_id):
        self.tasklist = tasklist_id or "@default"
        self.svc = _service("tasks", "v1", need_tasks=True)

    def pending(self):
        out, page = [], None
        while True:
            r = self.svc.tasks().list(tasklist=self.tasklist, showCompleted=False, maxResults=100,
                                      pageToken=page).execute()
            out += [(t.get("title", ""), t["id"]) for t in r.get("items", []) if t.get("title")]
            page = r.get("nextPageToken")
            if not page:
                return out

    def complete(self, task_id):
        self.svc.tasks().patch(tasklist=self.tasklist, task=task_id, body={"status": "completed"}).execute()


# ── Qué abrir según CALENDAR_BACKENDS ────────────────────────────────────────

def _need(*names):
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno: {', '.join(missing)}")


def release_calendar(backend):
    """Calendario donde van los lanzamientos, para un destino («radicale» o «google»)."""
    if backend == "radicale":
        _need("RADICALE_URL", "RADICALE_USERNAME", "RADICALE_PW")
        return RadicaleCalendar(os.getenv("RADICALE_URL"), os.getenv("RADICALE_USERNAME"),
                                os.getenv("RADICALE_PW"), os.getenv("RELEASES_CALENDAR_NAME", "discos"))
    return GoogleCalendar(os.getenv("GOOGLE_CALENDAR_ID", ""))


def task_list(backend):
    """Lista de tareas de discos pendientes de escuchar, para un destino."""
    if backend == "radicale":
        _need("RADICALE_URL", "RADICALE_USERNAME", "RADICALE_PW", "CALENDAR_NAME")
        return RadicaleTasks(os.getenv("RADICALE_URL"), os.getenv("RADICALE_USERNAME"),
                             os.getenv("RADICALE_PW"), os.getenv("CALENDAR_NAME"))
    return GoogleTasks(os.getenv("GOOGLE_TASKLIST_ID", ""))


# ── Órdenes de ayuda ─────────────────────────────────────────────────────────

def _load_env():
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(HERE, ".env"))
    except ImportError:
        pass


def main(argv):
    _load_env()
    cmd = argv[1] if len(argv) > 1 else ""
    if cmd == "autorizar":
        from google_auth_oauthlib.flow import InstalledAppFlow
        flow = InstalledAppFlow.from_client_secrets_file(_credentials_file(), SCOPES_OAUTH)
        creds = flow.run_local_server(port=0, open_browser="--sin-navegador" not in argv)
        _save_token(creds)
        print(f"Autorizado. Permiso guardado en {_token_file()}")
    elif cmd == "calendarios":
        for c in _service("calendar", "v3").calendarList().list().execute().get("items", []):
            print(f"{c['id']}\t{c.get('summary', '')}{'  (principal)' if c.get('primary') else ''}")
    elif cmd == "listas":
        for t in _service("tasks", "v1", need_tasks=True).tasklists().list().execute().get("items", []):
            print(f"{t['id']}\t{t.get('title', '')}")
    else:
        print(__doc__)


if __name__ == "__main__":
    try:
        main(sys.argv)
    except RuntimeError as e:
        raise SystemExit(f"✗ {e}")
