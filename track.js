// CELIA P — AIS position tracker (streaming)
//
// Holds a single AISStream connection open for hours and writes every position
// report as it arrives, instead of connecting, grabbing one point and hanging
// up. Under way that means a point roughly every 30 seconds rather than one per
// scheduled run.
//
// Why this shape: GitHub allows a job to run up to 6 hours, and scheduled
// triggers on the free tier fire irregularly — observed at 1-3 hour intervals
// even with a 5-minute cron. So the schedule is not the sampling rate; it is
// the recovery mechanism. Each trigger starts a fresh long-lived job and
// cancels the previous one, which also keeps us to one AISStream connection.
//
// Environment (GitHub repository secrets):
//   AISSTREAM_API_KEY, YACHT_MMSI, SUPABASE_URL, SUPABASE_SERVICE_KEY

const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY;
const YACHT_MMSI = process.env.YACHT_MMSI;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

// Stay under the 6-hour job ceiling with room for the runner's own overhead.
const RUN_MINUTES = 330;

// If nothing arrives for this long the connection is probably dead rather than
// the yacht simply being quiet — a moored Class B still reports every ~3 min.
const SILENCE_TIMEOUT_MS = 10 * 60 * 1000;

// Same thresholds as the database-side logic, so both agree on what counts as
// movement worth recording.
const MIN_DISTANCE_METERS = 5;
const MIN_SPEED_DELTA_KN = 0.3;
const MIN_HEADING_DELTA_DEG = 5;

// Negative accuracy marks the point as AIS-derived rather than phone GPS.
const AIS_ACCURACY_MARKER = -1;

// Active sessions rarely change mid-run, so re-read them occasionally instead
// of on every message.
const SESSION_REFRESH_MS = 10 * 60 * 1000;

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
const lastBySession = new Map();
let written = 0;
let skipped = 0;
let received = 0;

function haversineMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function headingDelta(a, b) {
  if (a == null || b == null) return 999;
  const diff = Math.abs(a - b) % 360;
  return diff > 180 ? 360 - diff : diff;
}

function clock() {
  return new Date().toISOString().slice(11, 19);
}

async function refreshSessions() {
  if (Date.now() - sessionsFetchedAt < SESSION_REFRESH_MS && sessions.length) {
    return sessions;
  }
  const { data, error } = await supabase
    .from("tracking_sessions")
    .select("id")
    .eq("is_active", true);

  if (error) {
    console.error(`✖ Ανάγνωση sessions: ${error.message}`);
    return sessions;
  }
  sessions = data || [];
  sessionsFetchedAt = Date.now();
  return sessions;
}

// Seed the dedup cache from the database so a restart doesn't immediately
// re-insert a point identical to the last one already stored.
async function seedLastPositions() {
  for (const session of sessions) {
    const { data } = await supabase
      .from("yacht_positions")
      .select("latitude, longitude, speed, heading")
      .eq("session_id", session.id)
      .order("recorded_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (data) lastBySession.set(session.id, data);
  }
}

function isUnchanged(last, pos) {
  if (!last) return false;
  return (
    haversineMeters(last.latitude, last.longitude, pos.latitude, pos.longitude) < MIN_DISTANCE_METERS &&
    Math.abs((last.speed ?? 0) - (pos.speed ?? 0)) < MIN_SPEED_DELTA_KN &&
    headingDelta(last.heading, pos.heading) < MIN_HEADING_DELTA_DEG
  );
}

async function savePosition(pos) {
  const active = await refreshSessions();
  if (!active.length) return;

  const rows = [];
  for (const session of active) {
    if (isUnchanged(lastBySession.get(session.id), pos)) {
      skipped++;
      continue;
    }
    rows.push({
      session_id: session.id,
      latitude: pos.latitude,
      longitude: pos.longitude,
      speed: pos.speed,
      heading: pos.heading,
      accuracy: AIS_ACCURACY_MARKER,
    });
  }
  if (!rows.length) return;

  const { error } = await supabase.from("yacht_positions").insert(rows);
  if (error) {
    console.error(`✖ Εγγραφή: ${error.message}`);
    return;
  }

  for (const row of rows) lastBySession.set(row.session_id, pos);
  written += rows.length;
  console.log(
    `${clock()} ← ${pos.latitude.toFixed(5)}, ${pos.longitude.toFixed(5)} · ` +
    `${pos.speed ?? "—"} kn · ${pos.heading ?? "—"}° · ${rows.length} εγγραφές`,
  );
}

function summarise(reason) {
  const mins = Math.round((Date.now() - startedAt) / 60000);
  console.log(
    `\n■ ${reason} μετά από ${mins} λεπτά — ` +
    `${received} εκπομπές, ${written} εγγραφές, ${skipped} αμετάβλητες.`,
  );
}

function connect() {
  if (Date.now() >= deadline) {
    summarise("Τέλος χρόνου");
    process.exit(0);
  }

  const ws = new WebSocket("wss://stream.aisstream.io/v0/stream");
  let silenceTimer;

  const resetSilenceTimer = () => {
    clearTimeout(silenceTimer);
    silenceTimer = setTimeout(() => {
      console.log(`${clock()} ⚠ Καμία εκπομπή για 10 λεπτά — επανασύνδεση`);
      try { ws.terminate(); } catch { /* already gone */ }
    }, SILENCE_TIMEOUT_MS);
  };

  ws.on("open", () => {
    console.log(`${clock()} → Συνδέθηκε, παρακολούθηση ${YACHT_MMSI}`);
    ws.send(JSON.stringify({
      APIKey: AISSTREAM_API_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: [String(YACHT_MMSI)],
      FilterMessageTypes: ["PositionReport"],
    }));
    resetSilenceTimer();
  });

  ws.on("message", async (raw) => {
    const text = raw.toString();

    // An invalid key comes back as plain text, not JSON. Retrying would just
    // fail the same way, so stop rather than loop for five hours.
    if (!text.trim().startsWith("{")) {
      console.error(`✖ AISStream: ${text.slice(0, 200)}`);
      clearTimeout(silenceTimer);
      summarise("Διακοπή");
      process.exit(0);
    }

    let msg;
    try { msg = JSON.parse(text); } catch { return; }
    if (msg.error) {
      console.error(`✖ AISStream: ${msg.error}`);
      clearTimeout(silenceTimer);
      summarise("Διακοπή");
      process.exit(0);
    }
    if (msg.MessageType !== "PositionReport") return;

    const meta = msg.MetaData || {};
    const report = msg.Message?.PositionReport || {};
    if (typeof meta.latitude !== "number" || typeof meta.longitude !== "number") return;

    received++;
    resetSilenceTimer();

    // 511 means "no heading source"; course over ground is the fallback.
    const heading =
      report.TrueHeading != null && report.TrueHeading !== 511
        ? report.TrueHeading
        : report.Cog ?? null;

    await savePosition({
      latitude: meta.latitude,
      longitude: meta.longitude,
      speed: report.Sog ?? null,
      heading,
    });
  });

  ws.on("error", (err) => console.error(`✖ ${err.message}`));

  ws.on("close", (code) => {
    clearTimeout(silenceTimer);
    if (Date.now() >= deadline) {
      summarise("Τέλος χρόνου");
      process.exit(0);
    }
    console.log(`${clock()} ⚠ Η σύνδεση έκλεισε (code=${code}) — επανασύνδεση σε 15 δευτ.`);
    setTimeout(connect, 15000);
  });
}

// Stopping at the deadline ourselves, rather than letting GitHub kill the job,
// means the summary still gets printed.
setTimeout(() => {
  summarise("Τέλος χρόνου");
  process.exit(0);
}, RUN_MINUTES * 60 * 1000);

(async () => {
  await refreshSessions();
  await seedLastPositions();
  console.log(`▶ Έναρξη · ${sessions.length} ενεργές sessions · διάρκεια ${RUN_MINUTES} λεπτά`);
  connect();
})();
