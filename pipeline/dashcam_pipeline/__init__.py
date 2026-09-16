"""Plug in the card and walk away.

This package watches for a camera card, copies what is new off it without ever
writing to it, works out which drives those clips belong to, processes them,
hands them to the server, and holds the raw video for a fortnight in case it is
wanted again.

Standard library only (tomllib, sqlite3, ctypes, urllib, tarfile): it runs
unattended from Task Scheduler, so it must not depend on anything that could be
missing or half-upgraded.
"""

__version__ = "0.1.0"
