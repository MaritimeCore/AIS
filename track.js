// CELIA P — AIS tracker (streaming)
//
// Holds one AISStream connection open for hours and writes what arrives:
//   • PositionReport  → yacht_positions   (lat/lon/speed/heading)
//   • ShipStaticData  → vessel_ais_data   (destination, ETA, draught, name)
//
// Why it runs here and not in a Supabase Edge Function: that runtime cannot
// open an outbound WebSocket to stream.aisstream.io — the connection closes
// immediately with code 0. It works from any ordinary machine.
//
// Why one long job instead of frequent short ones: AIS is a stream, so keeping
// the socket open captures every transmission (~30 s under way) instead of
// sampling one per run. GitHub allows 6-hour jobs, and its scheduler fires
// irregularly on the free tier, so the cron is not the sampling rate — it is
// the restart mechanism.
//
// Environment (GitHub repository secrets):
//   AISSTREAM_API_KEY, YACHT_MMSI, SUPABASE_URL, SUPABASE_SERVICE_KEY

const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY;
const YACHT_MMSI = process.env.YACHT_MMSI;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

// Stay under GitHub's 6-hour job ceiling with room for runner overhead.
const RUN_MINUTES = 330;

// Nothing for this long means the socket is probably dead rather than the
// yacht being quiet — a moored Class B still reports every ~3 minutes.
const SILENCE_TIMEOUT_MS = 6 * 60 * 1000;

// Movement thresholds, matching the database-side logic so both agree on what
// counts as a change worth storing.
const MIN_DISTANCE_METERS = 5;
const MIN_SPEED_DELTA_KN = 0.3;
const MIN_HEADING_DELTA_DEG = 5;

// A moored yacht never clears those thresholds, so without this the newest
// stored point would age indefinitely and the portal would show "no signal"
// for a boat that is transmitting perfectly well. Write one point per half
// hour regardless, purely so the timestamp stays current.
const HEARTBEAT_MS = 30 * 60 * 1000;

// Static data barely changes; re-storing it on every transmission would bloat
// the table for nothing.
const STATIC_MIN_INTERVAL_MS = 30 * 60 * 1000;

// Negative accuracy marks the point as AIS-derived rather than phone GPS.
const AIS_ACCURACY_MARKER = -1;

const SESSION_REFRESH_MS = 10 * 60 * 1000;

const NAV_STATUS = {
  0: "Under way using engine",
  1: "At anchor",
  2: "Not under command",
  3: "Restricted manoeuverability",
  4: "Constrained by draught",
  5: "Moored",
  6: "Aground",
  7: "Engaged in fishing",
  8: "Under way sailing",
  11: "Power-driven vessel towing astern",
  12: "Power-driven vessel pushing ahead",
  14: "AIS-SART",
  15: "Undefined",
};

for (const [name, value] of Object.entries({
  AISSTREAM_API_KEY, YACHT_MMSI, SUPABASE_URL, SUPABASE_SERVICE_KEY,
})) {
  if (!value) {
    console.error(`✖ Λείπει η μεταβλητή ${name}`);
    process.exit(1);
  }
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const startedAt = Date.now();
const deadline = startedAt + RUN_MINUTES * 60 * 1000;

let sessions = [];
let sessionsFetchedAt = 0;
const lastPos = new Map();      // session_id → last stored position
const lastPosAt = new Map();    // session_id → when it was stored
let lastStaticAt = 0;
let lastStaticKey = "";
let latestNavStatus = null;     // carried from PositionReport into static rows

let written = 0, skipped = 0, heartbeats = 0, statics = 0, received = 0;

function haversineMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function headingDelta(a, b) {
  if (a == null || b == null) return 999;
  const diff = Math.abs(a - b) % 360;
  return diff > 180 ? 360 - diff : diff;
}

const clock = () => new Date().toISOString().slice(11, 19);

async function refreshSessions() {
  if (sessions.length && Date.now() - sessionsFetchedAt < SESSION_REFRESH_MS) {
    return sessions;
  }
  const { data, error } = await supabase
    .from("tracking_sessions").select("id").eq("is_active", true);
  if (error) {
    console.error(`✖ Ανάγνωση sessions: ${error.message}`);
    return sessions;
  }
  sessions = data || [];
  sessionsFetchedAt = Date.now();
  return sessions;
}

// Seed from the database so a restart doesn't immediately re-store a point
// identical to the one already there.
async function seedState() {
  for (const s of sessions) {
    const { data } = await supabase
      .from("yacht_positions")
      .select("latitude, longitude, speed, heading, recorded_at")
      .eq("session_id", s.id)
      .order("recorded_at", { ascending: false })
      .limit(1).maybeSingle();
    if (data) {
      lastPos.set(s.id, data);
      lastPosAt.set(s.id, new Date(data.recorded_at).getTime());
    }
  }
}

function movedEnough(last, pos) {
  if (!last) return true;
  return (
    haversineMeters(last.latitude, last.longitude, pos.latitude, pos.longitude) >= MIN_DISTANCE_METERS ||
    Math.abs((last.speed ?? 0) - (pos.speed ?? 0)) >= MIN_SPEED_DELTA_KN ||
    headingDelta(last.heading, pos.heading) >= MIN_HEADING_DELTA_DEG
  );
}

async function savePosition(pos) {
  const active = await refreshSessions();
  if (!active.length) return;

  const now = Date.now();
  const rows = [];
  let viaHeartbeat = false;

  for (const s of active) {
    const moved = movedEnough(lastPos.get(s.id), pos);
    const stale = now - (lastPosAt.get(s.id) ?? 0) >= HEARTBEAT_MS;
    if (!moved && !stale) { skipped++; continue; }
    if (!moved) viaHeartbeat = true;
    rows.push({
      session_id: s.id,
      latitude: pos.latitude,
      longitude: pos.longitude,
      speed: pos.speed,
      heading: pos.heading,
      accuracy: AIS_ACCURACY_MARKER,
    });
  }
  if (!rows.length) return;

  const { error } = await supabase.from("yacht_positions").insert(rows);
  if (error) { console.error(`✖ Εγγραφή θέσης: ${error.message}`); return; }

  for (const r of rows) { lastPos.set(r.session_id, pos); lastPosAt.set(r.session_id, now); }
  written += rows.length;
  if (viaHeartbeat) heartbeats++;

  console.log(
    `${clock()} ← ${pos.latitude.toFixed(5)}, ${pos.longitude.toFixed(5)} · ` +
    `${pos.speed ?? "—"} kn · ${pos.heading ?? "—"}° · ${rows.length} εγγραφές` +
    (viaHeartbeat ? " (heartbeat)" : ""),
  );
}

// AIS encodes ETA as month/day/hour/minute with no year. Assume the coming
// occurrence: if the date has already passed this year, it means next year.
function parseEta(eta) {
  if (!eta || !eta.Month || !eta.Day) return null;
  const now = new Date();
  let year = now.getUTCFullYear();
  let d = new Date(Date.UTC(year, eta.Month - 1, eta.Day, eta.Hour ?? 0, eta.Minute ?? 0));
  if (d.getTime() < now.getTime() - 7 * 24 * 3600 * 1000) {
    d = new Date(Date.UTC(year + 1, eta.Month - 1, eta.Day, eta.Hour ?? 0, eta.Minute ?? 0));
  }
  return isNaN(d.getTime()) ? null : d.toISOString();
}

async function saveStatic(msg) {
  const s = msg.Message?.ShipStaticData || {};
  const meta = msg.MetaData || {};

  const destination = (s.Destination || "").trim() || null;
  const vesselName = (s.Name || meta.ShipName || "").trim() || null;
  const callsign = (s.CallSign || "").trim() || null;
  const imo = s.ImoNumber ? String(s.ImoNumber) : null;
  const draught = s.MaximumStaticDraught ?? null;
  const eta = parseEta(s.Eta);

  // Only store when something actually differs, or when enough time has passed
  // that a fresh timestamp is worth having.
  const key = [destination, eta, draught, latestNavStatus].join("|");
  const now = Date.now();
  if (key === lastStaticKey && now - lastStaticAt < STATIC_MIN_INTERVAL_MS) return;

  const active = await refreshSessions();
  if (!active.length) return;

  const rows = active.map((sess) => ({
    session_id: sess.id,
    destination,
    eta,
    navigation_status: latestNavStatus != null ? (NAV_STATUS[latestNavStatus] ?? null) : null,
    navigation_status_code: latestNavStatus,
    draught,
    vessel_name: vesselName,
    callsign,
    imo,
    mmsi: String(YACHT_MMSI),
    raw_payload: msg,
  }));

  const { error } = await supabase.from("vessel_ais_data").insert(rows);
  if (error) { console.error(`✖ Εγγραφή AIS: ${error.message}`); return; }

  lastStaticKey = key;
  lastStaticAt = now;
  statics += rows.length;
  console.log(
    `${clock()} ℹ ${vesselName ?? "—"} · προορισμός ${destination ?? "—"} · ` +
    `ETA ${eta ? eta.slice(0, 16).replace("T", " ") : "—"} · βύθισμα ${draught ?? "—"} m`,
  );
}

function summarise(reason) {
  const mins = Math.round((Date.now() - startedAt) / 60000);
  console.log(
    `\n■ ${reason} μετά από ${mins} λεπτά — ${received} εκπομπές, ` +
    `${written} θέσεις (${heartbeats} heartbeat), ${statics} AIS, ${skipped} αμετάβλητες.`,
  );
}

function stop(reason) { summarise(reason); process.exit(0); }

function connect() {
  if (Date.now() >= deadline) return stop("Τέλος χρόνου");

  const ws = new WebSocket("wss://stream.aisstream.io/v0/stream");
  let silenceTimer;

  const resetSilence = () => {
    clearTimeout(silenceTimer);
    silenceTimer = setTimeout(() => {
      console.log(`${clock()} ⚠ Σιωπή ${SILENCE_TIMEOUT_MS / 60000} λεπτών — επανασύνδεση`);
      try { ws.terminate(); } catch { /* already gone */ }
    }, SILENCE_TIMEOUT_MS);
  };

  ws.on("open", () => {
    console.log(`${clock()} → Συνδέθηκε, παρακολούθηση ${YACHT_MMSI}`);
    ws.send(JSON.stringify({
      APIKey: AISSTREAM_API_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: [String(YACHT_MMSI)],
      FilterMessageTypes: ["PositionReport", "ShipStaticData"],
    }));
    resetSilence();
  });

  ws.on("message", async (raw) => {
    const text = raw.toString();

    // An invalid key comes back as plain text, not JSON. Retrying would fail
    // the same way, so stop rather than loop for five hours.
    if (!text.trim().startsWith("{")) {
      console.error(`✖ AISStream: ${text.slice(0, 200)}`);
      clearTimeout(silenceTimer);
      return stop("Διακοπή");
    }

    let msg;
    try { msg = JSON.parse(text); } catch { return; }
    if (msg.error) {
      console.error(`✖ AISStream: ${msg.error}`);
      clearTimeout(silenceTimer);
      return stop("Διακοπή");
    }

    received++;
    resetSilence();

    if (msg.MessageType === "PositionReport") {
      const meta = msg.MetaData || {};
      const r = msg.Message?.PositionReport || {};
      if (typeof meta.latitude !== "number" || typeof meta.longitude !== "number") return;

      if (r.NavigationalStatus != null) latestNavStatus = r.NavigationalStatus;

      // 511 means the transponder has no heading source; course over ground is
      // the sensible fallback.
      const heading = r.TrueHeading != null && r.TrueHeading !== 511
        ? r.TrueHeading
        : r.Cog ?? null;

      await savePosition({
        latitude: meta.latitude,
        longitude: meta.longitude,
        speed: r.Sog ?? null,
        heading,
      });
      return;
    }

    if (msg.MessageType === "ShipStaticData") {
      await saveStatic(msg);
    }
  });

  ws.on("error", (err) => console.error(`✖ ${err.message}`));

  ws.on("close", (code) => {
    clearTimeout(silenceTimer);
    if (Date.now() >= deadline) return stop("Τέλος χρόνου");
    console.log(`${clock()} ⚠ Η σύνδεση έκλεισε (code=${code}) — επανασύνδεση σε 15 δευτ.`);
    setTimeout(connect, 15000);
  });
}

// Stopping at the deadline ourselves, rather than being killed by GitHub,
// means the summary still gets printed.
setTimeout(() => stop("Τέλος χρόνου"), RUN_MINUTES * 60 * 1000);

(async () => {
  await refreshSessions();
  await seedState();
  console.log(
    `▶ Έναρξη · ${sessions.length} ενεργές sessions · διάρκεια ${RUN_MINUTES} λεπτά · ` +
    `heartbeat ${HEARTBEAT_MS / 60000} λεπτά`,
  );
  connect();
})();
