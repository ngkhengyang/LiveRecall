"""Typed wire messages for the LiveRecall protocol.

Every message that crosses the WebSocket becomes a small frozen dataclass that
knows how to validate itself out of JSON and serialize itself back. Nothing
outside this module hand-assembles or hand-parses a protocol dict; the single
exception is ``Server._accept``, which must peek at ``raw["type"]`` to know
which class to hand the payload to (inherent to any JSON tagged union).

``from_dict`` returns ``None`` on anything malformed instead of raising, so a
badly formed frame can never take the server down.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


def _as_int(value: Any) -> int | None:
    """Strict int: rejects bools, which are ``int`` subclasses in Python."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


@dataclass(frozen=True)
class SourceHello:
    """First message from the caption source."""

    session: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SourceHello | None":
        session = raw.get("session")
        if not isinstance(session, str) or not session:
            return None
        return cls(session=session)

    def to_dict(self) -> dict[str, Any]:
        return {"type": "SOURCE_HELLO", "session": self.session}

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass(frozen=True)
class ClientHello:
    """First message from a browser / load-test client.

    Parsing is deliberately lenient here: every field has a sane default, so a
    client that omits an optional field still gets a working connection rather
    than a 1008 close. Only the message type itself is mandatory, and that is
    checked by the caller.
    """

    client_id: int = 0
    session: str = "L1"
    last_segment: int = 0
    last_revision: int = 0
    history_seconds: float = 30.0

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ClientHello | None":
        client_id = _as_int(raw.get("client_id", 0))
        session = raw.get("session", "L1")
        last_segment = _as_int(raw.get("last_segment", 0))
        last_revision = _as_int(raw.get("last_revision", 0))
        history_seconds = _as_number(raw.get("history_seconds", 30))

        if client_id is None:
            client_id = 0
        if not isinstance(session, str) or not session:
            session = "L1"
        if last_segment is None or last_segment < 0:
            last_segment = 0
        if last_revision is None or last_revision < 0:
            last_revision = 0
        if history_seconds is None or history_seconds < 0:
            history_seconds = 30.0

        return cls(
            client_id=client_id,
            session=session,
            last_segment=last_segment,
            last_revision=last_revision,
            history_seconds=history_seconds,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "CLIENT_HELLO",
            "client_id": self.client_id,
            "session": self.session,
            "last_segment": self.last_segment,
            "last_revision": self.last_revision,
            "history_seconds": self.history_seconds,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass(frozen=True)
class CaptionUpdate:
    """One revision of one caption segment.

    This is the message that flows through almost the whole system: the Source
    parses it in, the CaptionStore validates it, the OutboundBuffer holds it,
    and the Client sends it back out. It is plain data with no behavior beyond
    parse/serialize.

    Validation is strict, unlike ``ClientHello``: a malformed caption update is
    rejected outright rather than repaired, because guessing at a segment or
    revision number would corrupt the revision state everything else relies on.
    """

    session: str
    segment: int
    revision: int
    source_ts: float
    final: bool
    text: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "CaptionUpdate | None":
        segment = _as_int(raw.get("segment"))
        revision = _as_int(raw.get("revision"))
        source_ts = _as_number(raw.get("source_ts"))
        final = raw.get("final")
        text = raw.get("text")
        session = raw.get("session", "L1")

        if segment is None or segment <= 0:
            return None
        if revision is None or revision <= 0:
            return None
        if source_ts is None:
            return None
        if not isinstance(final, bool):
            return None
        if not isinstance(text, str):
            return None
        if not isinstance(session, str) or not session:
            session = "L1"

        return cls(
            session=session,
            segment=segment,
            revision=revision,
            source_ts=source_ts,
            final=final,
            text=text,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "CAPTION_UPDATE",
            "session": self.session,
            "segment": self.segment,
            "revision": self.revision,
            "source_ts": self.source_ts,
            "final": self.final,
            "text": self.text,
        }

    def to_json(self) -> str:
        """Serialize once, reuse for every client.

        One update object is fanned out to every connected client, so without
        this the server would run ``json.dumps`` N times on identical data. The
        cache is stored outside the dataclass fields (hence ``object.__setattr__``
        on a frozen instance) so it never affects equality or ``repr``.
        """
        cached = self.__dict__.get("_json_cache")
        if cached is None:
            cached = json.dumps(self.to_dict())
            object.__setattr__(self, "_json_cache", cached)
        return cached


@dataclass(frozen=True)
class History:
    """Recovery context sent in reply to ``CLIENT_HELLO``.

    Server-constructed only; it is never parsed, so it has no ``from_dict``.
    Items are finalized captions ordered oldest to newest.
    """

    items: list[CaptionUpdate] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"type": "HISTORY", "items": [item.to_dict() for item in self.items]}

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass(frozen=True)
class SourceDone:
    """Test-control message marking the end of a deterministic run."""

    session: str = "L1"
    segments: int | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SourceDone | None":
        session = raw.get("session", "L1")
        if not isinstance(session, str) or not session:
            session = "L1"
        return cls(session=session, segments=_as_int(raw.get("segments")))

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": "SOURCE_DONE", "session": self.session}
        if self.segments is not None:
            payload["segments"] = self.segments
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
