// CELIA P — AIS position tracker
//
// Connects to AISStream over WebSocket, waits for a position report for the
// yacht, and writes it into Supabase (yacht_positions) for every active
// tracking session.
//
// Why this runs on GitHub Actions instead of a Supabase Edge Function:
// Supabase's function runtime cannot open an outbound WebSocket to
// stream.aisstream.io — the connection closes immediately with code 0. The same
// connection succeeds from an ordinary machine, so the work happens here.
//
// Configuration comes from environment variables (GitHub repository secrets):
//   AISSTREAM_API_KEY, YACHT_MMSI, SUPABASE_URL, SUPABASE_SERVICE_KEY

const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY;
const YACHT_MMSI = process.env.YACHT_MMSI;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

// A Class B transponder reports every ~30 s under way, but only every ~3 min
// when moored. Four minutes covers the slow case with room to spare while
// leaving the job well inside its runner budget.
const WAIT_MS = 3 * 60 * 1000;

// Skip writing a point that is essentially the previous one. Mirrors the
// thresholds used by the in-database cron logic so both sources agree.
const MIN_DISTANCE_METERS = 5;
const MIN_SPEED_DELTA_KN = 0.3;
const MIN_HEADING_DELTA_DEG = 5;

// Negative accuracy marks a point as originating from AIS rather than a phone
// GPS. The portal uses this to tell the two apart.
const AIS_ACCURACY_MARKER = -1;

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

async function savePosition(pos) {
  const { data: sessions, error: sessionsError } = await supabase
    .from("tracking_sessions")
    .select("id")
    .eq("is_active", true);

  if (sessionsError) throw new Error(`Ανάγνωση sessions: ${sessionsError.message}`);
  if (!sessions?.length) {
    console.log("ℹ Καμία ενεργή tracking session — δεν γράφτηκε τίποτα.");
    return;
  }

  let written = 0;
  let skipped = 0;

  for (const session of sessions) {
    const { data: last } = await supabase
      .from("yacht_positions")
      .select("latitude, longitude, speed, heading")
      .eq("session_id", session.id)
      .order("recorded_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (last) {
      const distance = haversineMeters(
        last.latitude, last.longitude, pos.latitude, pos.longitude,
      );
      const speedDelta = Math.abs((last.speed ?? 0) - (pos.speed ?? 0));
      const headDelta = headingDelta(last.heading, pos.heading);

      if (
        distance < MIN_DISTANCE_METERS &&
        speedDelta < MIN_SPEED_DELTA_KN &&
        headDelta < MIN_HEADING_DELTA_DEG
      ) {
        skipped++;
        continue;
      }
    }

    const { error: insertError } = await supabase
      .from("yacht_positions")
      .insert({
        session_id: session.id,
        latitude: pos.latitude,
        longitude: pos.longitude,
        speed: pos.speed,
        heading: pos.heading,
        accuracy: AIS_ACCURACY_MARKER,
      });

    if (insertError) {
      console.error(`✖ Session ${session.id}: ${insertError.message}`);
    } else {
      written++;
    }
  }

  console.log(`✔ Γράφτηκαν ${written} στίγματα, παραλείφθηκαν ${skipped} (αμετάβλητα).`);
}

function fetchPosition() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket("wss://stream.aisstream.io/v0/stream");
    let settled = false;

    const finish = (fn, arg) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { ws.close(); } catch { /* already closing */ }
      fn(arg);
    };

    const timer = setTimeout(
      () => finish(reject, new Error(`Καμία εκπομπή για MMSI ${YACHT_MMSI} μέσα σε ${WAIT_MS / 60000} λεπτά`)),
      WAIT_MS,
    );

    ws.on("open", () => {
      console.log("→ Σύνδεση στο AISStream, αναμονή για", YACHT_MMSI);
      ws.send(JSON.stringify({
        APIKey: AISSTREAM_API_KEY,
        BoundingBoxes: [[[-90, -180], [90, 180]]],
        FiltersShipMMSI: [String(YACHT_MMSI)],
        FilterMessageTypes: ["PositionReport"],
      }));
    });

    ws.on("message", (raw) => {
      const text = raw.toString();

      // An invalid key comes back as plain text rather than JSON.
      if (!text.trim().startsWith("{")) {
        return finish(reject, new Error(`AISStream: ${text.slice(0, 200)}`));
      }

      let msg;
      try {
        msg = JSON.parse(text);
      } catch {
        return; // ignore anything unparseable and keep waiting
      }

      if (msg.error) return finish(reject, new Error(`AISStream: ${msg.error}`));
      if (msg.MessageType !== "PositionReport") return;

      const meta = msg.MetaData || {};
      const report = msg.Message?.PositionReport || {};

      if (typeof meta.latitude !== "number" || typeof meta.longitude !== "number") {
        return; // malformed report, wait for the next one
      }

      // TrueHeading is 511 when the transponder has no heading source; course
      // over ground is the sensible fallback.
      const heading =
        report.TrueHeading != null && report.TrueHeading !== 511
          ? report.TrueHeading
          : report.Cog ?? null;

      finish(resolve, {
        latitude: meta.latitude,
        longitude: meta.longitude,
        speed: report.Sog ?? null,
        heading,
        name: (meta.ShipName || "").trim(),
        time: meta.time_utc,
      });
    });

    ws.on("error", (err) => finish(reject, err));

    ws.on("close", (code, reason) => {
      finish(reject, new Error(`Η σύνδεση έκλεισε: code=${code} reason=${reason || "n/a"}`));
    });
  });
}

(async () => {
  try {
    const pos = await fetchPosition();
    console.log(`← ${pos.name || "σκάφος"} @ ${pos.latitude}, ${pos.longitude}`);
    console.log(`  ταχύτητα ${pos.speed ?? "—"} kn, πορεία ${pos.heading ?? "—"}°, ${pos.time}`);
    await savePosition(pos);
    process.exit(0);
  } catch (err) {
    // A missed window is normal — the yacht may be out of range of any station.
    // Exiting non-zero would email a failure notice every time, so we don't.
    console.log(`ℹ ${err.message}`);
    process.exit(0);
  }
})();
