"""Plug in the card and walk away.

This package watches for a camera card, works out which drives the clips on it
belong to, processes the new ones straight off the card without copying it or
writing to it, hands the results to the server, and says when the card can be
erased. No video is kept: the database needs only the GPS track, the
accelerometer trace and a still every thirty seconds.

Standard library only (tomllib, sqlite3, ctypes, urllib, tarfile): it runs
unattended from Task Scheduler, so it must not depend on anything that could be
missing or half-upgraded.
"""

__version__ = "0.1.0"
