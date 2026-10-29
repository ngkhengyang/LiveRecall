/*
 * LiveRecall browser client.
 *
 * Mirrors the server's revision rules locally so the two can never disagree:
 * a final caption is durable, and a revision is only accepted if it is newer
 * than the one already held for that segment. Everything the user sees --
 * revision replacement, the recall window, reconnect resume -- falls out of
 * that one rule.
 *
 * "What did I miss?" is answered entirely from local state. The server is
 * asked for context only through the HISTORY reply to CLIENT_HELLO, which is
 * to say only on a late join or after a genuine disconnection.
 */

(function () {
  "use strict";

  // Keep well over the 60 s the recall window needs, while still bounded.
  var RETENTION_SECONDS = 150;
  var MAX_SEGMENTS = 400;
  var MAX_ROWS = 80;
  var RECONNECT_BASE_MS = 500;
  var RECONNECT_MAX_MS = 5000;
  var DEFAULT_RECALL_SECONDS = 30;

  function serverUrl() {
    var override = new URLSearchParams(location.search).get("ws");
    if (override) return override;
    var host = location.hostname || "127.0.0.1";
    return "ws://" + host + ":8765";
  }

  function nowSeconds() {
    return Date.now() / 1000;
  }

  /* ----------------------------------------------------------------- *
   * CaptionState - the local mirror of the lecture.
   * ----------------------------------------------------------------- */

  function CaptionState() {
    this.segments = new Map(); // segment -> caption
    this.lastFinalSegment = 0;
    this.lastFinalRevision = 0;
    this.suppressed = 0; // duplicate or stale updates rejected
  }

  /*
   * The single gate every incoming caption passes through, live or recovered.
   * Returns true only when the update genuinely advances this segment, which
   * is what guarantees a finalized caption is never displayed twice even if
   * HISTORY and the live stream overlap.
   */
  CaptionState.prototype.accept = function (update) {
    var held = this.segments.get(update.segment);
    if (held && (held.final || update.revision <= held.revision)) {
      this.suppressed += 1;
      return false;
    }

    this.segments.set(update.segment, {
      segment: update.segment,
      revision: update.revision,
      final: !!update.final,
      text: update.text,
      sourceTs: update.source_ts,
    });

    if (update.final) {
      // Resume state tracks the latest FINALIZED caption, never the latest
      // provisional one, so a reconnect asks for exactly what it is missing.
      if (
        update.segment > this.lastFinalSegment ||
        (update.segment === this.lastFinalSegment &&
          update.revision > this.lastFinalRevision)
      ) {
        this.lastFinalSegment = update.segment;
        this.lastFinalRevision = update.revision;
      }
    }
    this.prune();
    return true;
  };

  CaptionState.prototype.prune = function () {
    var cutoff = nowSeconds() - RETENTION_SECONDS;
    var keys = Array.from(this.segments.keys()).sort(function (a, b) {
      return a - b;
    });
    for (var i = 0; i < keys.length; i++) {
      var caption = this.segments.get(keys[i]);
      var tooOld = caption.final && caption.sourceTs < cutoff;
      var tooMany = this.segments.size > MAX_SEGMENTS;
      if (!tooOld && !tooMany) break;
      this.segments.delete(keys[i]);
    }
  };

  CaptionState.prototype.ordered = function () {
    return Array.from(this.segments.values()).sort(function (a, b) {
      return a.segment - b.segment;
    });
  };

  CaptionState.prototype.recall = function (seconds) {
    var cutoff = nowSeconds() - seconds;
    return this.ordered().filter(function (caption) {
      return caption.final && caption.sourceTs >= cutoff;
    });
  };

  CaptionState.prototype.finalCount = function () {
    var n = 0;
    this.segments.forEach(function (caption) {
      if (caption.final) n += 1;
    });
    return n;
  };

  /* ----------------------------------------------------------------- *
   * View
   * ----------------------------------------------------------------- */

  function View(state) {
    this.state = state;
    this.statusEl = document.getElementById("status");
    this.liveEl = document.getElementById("live");
    this.recallEl = document.getElementById("recall");
    this.statsEl = document.getElementById("stats");
    this.rows = new Map(); // segment -> element
    this.recallSeconds = DEFAULT_RECALL_SECONDS;
    this.stats = { attempts: 0, historyItems: 0, recovered: 0 };
  }

  View.prototype.setStatus = function (text, stateName) {
    this.statusEl.textContent = text;
    this.statusEl.dataset.state = stateName;
  };

  View.prototype.buildRow = function (caption) {
    var row = document.createElement("div");
    row.className = "caption";
    row.dataset.segment = String(caption.segment);

    var seg = document.createElement("span");
    seg.className = "seg";
    seg.textContent = "#" + caption.segment;

    var badge = document.createElement("span");
    badge.className = "badge";

    var text = document.createElement("span");
    text.className = "text";

    row.appendChild(seg);
    row.appendChild(badge);
    row.appendChild(text);
    return row;
  };

  /*
   * One row per segment, rewritten in place. A newer revision therefore
   * replaces what is on screen instead of adding a line.
   */
  View.prototype.renderCaption = function (caption) {
    var row = this.rows.get(caption.segment);
    if (!row) {
      row = this.buildRow(caption);
      this.rows.set(caption.segment, row);
      this.insertInOrder(row, caption.segment);
    }
    row.className = "caption " + (caption.final ? "is-final" : "is-live");
    row.querySelector(".badge").textContent = caption.final ? "final" : "live";
    row.querySelector(".text").textContent = caption.text;
  };

  View.prototype.insertInOrder = function (row, segment) {
    var next = null;
    var children = this.liveEl.children;
    for (var i = children.length - 1; i >= 0; i--) {
      if (Number(children[i].dataset.segment) > segment) {
        next = children[i];
      } else {
        break;
      }
    }
    this.liveEl.insertBefore(row, next);
  };

  View.prototype.trimRows = function () {
    var live = this.state.segments;
    var self = this;
    this.rows.forEach(function (row, segment) {
      if (!live.has(segment)) {
        row.remove();
        self.rows.delete(segment);
      }
    });
    while (this.liveEl.children.length > MAX_ROWS) {
      var first = this.liveEl.children[0];
      this.rows.delete(Number(first.dataset.segment));
      first.remove();
    }
  };

  View.prototype.scrollToLatest = function () {
    var el = this.liveEl;
    var nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
    if (nearBottom) el.scrollTop = el.scrollHeight;
  };

  View.prototype.renderRecall = function () {
    var items = this.state.recall(this.recallSeconds);
    this.recallEl.textContent = "";
    var self = this;
    var now = nowSeconds();
    items.forEach(function (caption) {
      var row = self.buildRow(caption);
      row.className = "caption is-final";
      row.querySelector(".badge").textContent = "final";
      row.querySelector(".text").textContent = caption.text;
      var age = document.createElement("span");
      age.className = "recall-time";
      age.textContent = "-" + Math.max(0, Math.round(now - caption.sourceTs)) + "s";
      row.appendChild(age);
      self.recallEl.appendChild(row);
    });
  };

  View.prototype.renderStats = function () {
    this.statsEl.innerHTML = "";
    var state = this.state;
    var pairs = [
      ["segments held", state.segments.size],
      ["finals held", state.finalCount()],
      ["resume point", state.lastFinalSegment + "r" + state.lastFinalRevision],
      ["duplicates suppressed", state.suppressed],
      ["recovered from history", this.stats.recovered],
      ["last HISTORY size", this.stats.historyItems],
      ["connection attempts", this.stats.attempts],
    ];
    var self = this;
    pairs.forEach(function (pair) {
      var span = document.createElement("span");
      span.textContent = pair[0] + ": ";
      var b = document.createElement("b");
      b.textContent = String(pair[1]);
      span.appendChild(b);
      self.statsEl.appendChild(span);
    });
  };

  /* ----------------------------------------------------------------- *
   * Connection
   * ----------------------------------------------------------------- */

  function Connection(state, view) {
    this.state = state;
    this.view = view;
    this.ws = null;
    this.retries = 0;
    this.timer = null;
    this.manualClose = false;
  }

  Connection.prototype.clientId = function () {
    var key = "liverecall-client-id";
    var value = sessionStorage.getItem(key);
    if (!value) {
      value = String(Math.floor(100000 + Math.random() * 900000));
      sessionStorage.setItem(key, value);
    }
    return Number(value);
  };

  Connection.prototype.connect = function () {
    var self = this;
    clearTimeout(this.timer);
    this.view.stats.attempts += 1;
    this.view.setStatus("connecting", "connecting");
    this.view.renderStats();

    var ws;
    try {
      ws = new WebSocket(serverUrl());
    } catch (err) {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.addEventListener("open", function () {
      self.retries = 0;
      self.view.setStatus("connected", "connected");
      // last_segment / last_revision describe the latest FINALIZED state this
      // browser holds, so the server replies with only what was missed. A
      // fresh page load sends 0/0 and is simply treated as a late join.
      ws.send(
        JSON.stringify({
          type: "CLIENT_HELLO",
          client_id: self.clientId(),
          session: "L1",
          last_segment: self.state.lastFinalSegment,
          last_revision: self.state.lastFinalRevision,
          history_seconds: 60,
        })
      );
    });

    ws.addEventListener("message", function (event) {
      var msg;
      try {
        msg = JSON.parse(event.data);
      } catch (err) {
        return;
      }
      if (msg.type === "CAPTION_UPDATE") {
        self.applyUpdates([msg], false);
      } else if (msg.type === "HISTORY") {
        var items = Array.isArray(msg.items) ? msg.items : [];
        self.view.stats.historyItems = items.length;
        self.applyUpdates(items, true);
      }
    });

    ws.addEventListener("close", function () {
      self.view.setStatus("disconnected", "offline");
      self.scheduleReconnect();
    });

    ws.addEventListener("error", function () {
      // 'close' always follows, so reconnection is handled in one place.
      self.view.setStatus("connection error", "offline");
    });
  };

  Connection.prototype.applyUpdates = function (updates, recovered) {
    var changed = false;
    for (var i = 0; i < updates.length; i++) {
      var update = updates[i];
      if (typeof update.segment !== "number") continue;
      if (this.state.accept(update)) {
        this.view.renderCaption(this.state.segments.get(update.segment));
        if (recovered) this.view.stats.recovered += 1;
        changed = true;
      }
    }
    if (changed) {
      this.view.trimRows();
      this.view.scrollToLatest();
      this.view.renderRecall();
    }
    this.view.renderStats();
  };

  Connection.prototype.scheduleReconnect = function () {
    var self = this;
    var delay = Math.min(
      RECONNECT_MAX_MS,
      RECONNECT_BASE_MS * Math.pow(2, this.retries)
    );
    this.retries += 1;
    var seconds = (delay / 1000).toFixed(1);
    this.view.setStatus("reconnecting in " + seconds + "s", "connecting");
    clearTimeout(this.timer);
    this.timer = setTimeout(function () {
      self.connect();
    }, delay);
  };

  Connection.prototype.dropNow = function () {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.view.setStatus("disconnected (simulated)", "offline");
      this.ws.close();
    }
  };

  /* ----------------------------------------------------------------- *
   * Wiring
   * ----------------------------------------------------------------- */

  var state = new CaptionState();
  var view = new View(state);
  var connection = new Connection(state, view);

  var buttons = document.querySelectorAll("button[data-seconds]");
  Array.prototype.forEach.call(buttons, function (button) {
    button.addEventListener("click", function () {
      view.recallSeconds = Number(button.dataset.seconds);
      Array.prototype.forEach.call(buttons, function (other) {
        other.setAttribute(
          "aria-pressed",
          String(other === button)
        );
      });
      view.renderRecall();
    });
  });

  document.getElementById("refresh").addEventListener("click", function () {
    view.renderRecall();
  });
  document.getElementById("drop").addEventListener("click", function () {
    connection.dropNow();
  });

  // Ages shown in the recall list drift as time passes even with no traffic.
  setInterval(function () {
    view.renderRecall();
  }, 1000);

  view.renderRecall();
  view.renderStats();
  connection.connect();
})();
