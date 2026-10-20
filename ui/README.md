# Viewer: working on it locally

The viewer is static HTML, CSS and JavaScript in this folder, served by nginx
on the server with `/api/` proxied to the FastAPI container and `/media/`
proxied to its thumbnail mount (see `nginx.conf`). Nothing is compiled: edit a
file, reload the page.

On the PC there is no nginx, so `scripts/dev_ui.py` stands in for it. It
serves this folder on `http://localhost:8080/`, forwards `/api/` to an API of
your choosing and `/media/` to the server's thumbnails, and disables caching so
edits show up on reload.

## UI work only

Point the API at the server's live FastAPI, which is read-only for everything
the viewer does:

```
python scripts/dev_ui.py --api http://192.168.1.164:8000
```

## UI and API work

Run the API locally against the server's database. The PC-side `.env` at the
repo root (gitignored) holds the `POSTGRES_*` credentials copied from the
server plus `DB_HOST=192.168.1.164`. One-time setup, from `api/`:

```
py -3.13 -m venv .venv
.venv\Scripts\python -m pip install "fastapi==0.115.8" "uvicorn[standard]==0.30.6" "asyncpg>=0.30,<0.31"
```

The container pins `asyncpg==0.29.0`, which has no wheel for Python 3.13; the
newer asyncpg is used only in this local environment and behaves the same for
the viewer's queries.

Start it (Git Bash shown; PowerShell needs the three `DB_*` variables set the
same way):

```
cd api
set -a; source ../.env; set +a
DB_NAME=$POSTGRES_DB DB_USER=$POSTGRES_USER DB_PASSWORD=$POSTGRES_PASSWORD \
  .venv/Scripts/uvicorn main:app --port 8000 --reload
```

then, from the repo root:

```
python scripts/dev_ui.py
```

`--reload` restarts uvicorn when `api/main.py` changes.

## Deploying

The UI folder is bind-mounted into the nginx container, so on the server a
`git pull --ff-only` in `~/dashcam-db` is the whole deployment for UI changes.
API changes need `docker compose build api && docker compose up -d api`.
Database changes are applied by hand; see the viewer overhaul plan.
