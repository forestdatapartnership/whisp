"""Exceptions Whisp must never swallow: they are how the caller stops a run."""

# Celery (the Whisp API's task runner) stops a task that runs too long by raising
# SoftTimeLimitExceeded, which subclasses Exception, so a blanket "except Exception" around an
# Earth Engine call would eat it, retry, and the API would never see its own timeout (#235).
# Handlers on those paths re-raise these first. KeyboardInterrupt and SystemExit already pass
# through an "except Exception"; they are listed so the intent is in one place.
try:
    from celery.exceptions import SoftTimeLimitExceeded

    PROPAGATE = (KeyboardInterrupt, SystemExit, SoftTimeLimitExceeded)
except ImportError:  # celery not installed: nothing extra to let through
    PROPAGATE = (KeyboardInterrupt, SystemExit)
