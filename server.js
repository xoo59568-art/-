const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  delay,
} = require("@whiskeysockets/baileys");
const { Boom } = require("@hapi/boom");
const pino = require("pino");
const path = require("path");
const fs = require("fs");
const archiver = require("archiver");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const activeSessions = new Map();
const pendingResults = new Map(); // ← Race condition fix: result এখানে hold হবে
const TEMP_DIR = path.join(__dirname, "temp");

if (!fs.existsSync(TEMP_DIR)) {
  fs.mkdirSync(TEMP_DIR, { recursive: true });
}

// ── ZIP → Base64 SESSION_ID ────────────────────────────────
async function generateSessionID(sessionPath, phoneNumber) {
  const zipPath = path.join(TEMP_DIR, `${phoneNumber}_session.zip`);

  await new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver("zip", { zlib: { level: 9 } });
    output.on("close", resolve);
    archive.on("error", reject);
    archive.pipe(output);
    archive.directory(sessionPath, false);
    archive.finalize();
  });

  const zipBuffer = fs.readFileSync(zipPath);
  const base64Data = zipBuffer.toString("base64");

  if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath);

  return `RABBITXMD~${base64Data}`;
}

// ── Cleanup temp files ─────────────────────────────────────
function cleanupSession(sessionPath, phoneNumber) {
  try {
    if (fs.existsSync(sessionPath)) {
      fs.rmSync(sessionPath, { recursive: true, force: true });
    }
    activeSessions.delete(phoneNumber);
    console.log(`✅ Cleaned up session for ${phoneNumber}`);
  } catch (err) {
    console.error("Cleanup error:", err.message);
  }
}

// ── API: Generate Pair Code ────────────────────────────────
app.post("/api/generate-pair", async (req, res) => {
  let { number } = req.body;

  if (!number) {
    return res.status(400).json({ error: "Phone number is required" });
  }

  number = number.replace(/[^0-9]/g, "");

  if (number.length < 10 || number.length > 15) {
    return res.status(400).json({ error: "Invalid phone number (10-15 digits required)" });
  }

  // আগের session পরিষ্কার করো
  if (activeSessions.has(number)) {
    try {
      const old = activeSessions.get(number);
      old.sock?.ev?.removeAllListeners();
      old.sock?.ws?.close();
    } catch (_) {}
    activeSessions.delete(number);
  }

  pendingResults.delete(number); // পুরনো pending result মুছো

  const sessionPath = path.join(TEMP_DIR, number);
  if (fs.existsSync(sessionPath)) {
    fs.rmSync(sessionPath, { recursive: true, force: true });
  }
  fs.mkdirSync(sessionPath, { recursive: true });

  try {
    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);

    const sock = makeWASocket({
      auth: state,
      printQRInTerminal: false,
      logger: pino({ level: "silent" }),
      browser: ["RabbitXMD", "Chrome", "120.0.0"],
      markOnlineOnConnect: false,
    });

    activeSessions.set(number, { sock, path: sessionPath });

    sock.ev.on("creds.update", saveCreds);

    // Pair code generate (3 sec delay দিয়ে socket ready হতে দাও)
    let pairCodeSent = false;
    setTimeout(async () => {
      try {
        if (!pairCodeSent) {
          pairCodeSent = true;
          const code = await sock.requestPairingCode(number);
          const formatted = code?.match(/.{1,4}/g)?.join("-").toUpperCase() || code;
          if (!res.headersSent) {
            res.json({ success: true, pairCode: formatted, number });
          }
        }
      } catch (err) {
        if (!res.headersSent) {
          res.status(500).json({ error: "Pair Code generate হয়নি: " + err.message });
        }
      }
    }, 3000);

    // Connection update listener
    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect } = update;

      if (connection === "open") {
        console.log(`📱 WhatsApp connected: ${number}`);

        await delay(3000); // creds.json পুরোপুরি লেখা হতে দাও

        try {
          const sessionID = await generateSessionID(sessionPath, number);
          const userNumber =
            sock.user?.id?.split(":")[0] ||
            sock.user?.id?.split("@")[0] ||
            number;

          const result = { sessionID, connectedNumber: userNumber };

          // ── KEY FIX: আগে pending-এ রাখো, তারপর emit করো ──
          pendingResults.set(number, result);

          // Socket room-এ emit করো
          io.to(number).emit("session_ready", result);
          console.log(`📤 session_ready emitted for ${number}`);

          // Socket বন্ধ করো
          try {
            sock.ev.removeAllListeners();
            sock.ws.close();
          } catch (_) {}

          // 30 sec পর cleanup (frontend-এর জন্য যথেষ্ট সময়)
          setTimeout(() => {
            cleanupSession(sessionPath, number);
            // pending result 2 মিনিট পর মুছো
            setTimeout(() => pendingResults.delete(number), 120000);
          }, 30000);

        } catch (err) {
          console.error("SESSION_ID error:", err.message);
          io.to(number).emit("error", {
            message: "SESSION_ID generate করতে সমস্যা হয়েছে: " + err.message,
          });
          cleanupSession(sessionPath, number);
        }
      }

      if (connection === "close") {
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
        console.log(`Connection closed for ${number}, reason: ${statusCode}`);

        if (
          statusCode !== DisconnectReason.connectionClosed &&
          statusCode !== DisconnectReason.loggedOut &&
          !pendingResults.has(number) // SESSION_ID ready হলে error পাঠাবো না
        ) {
          io.to(number).emit("error", { message: "Connection বন্ধ হয়ে গেছে। আবার চেষ্টা করুন।" });
          cleanupSession(sessionPath, number);
        }
      }
    });

  } catch (err) {
    if (!res.headersSent) {
      res.status(500).json({ error: err.message });
    }
    cleanupSession(sessionPath, number);
  }
});

// ── Socket.io ──────────────────────────────────────────────
io.on("connection", (socket) => {
  socket.on("subscribe", (number) => {
    socket.join(number);
    console.log(`Socket joined room: ${number}`);

    // ── KEY FIX: Join করার সাথে সাথে pending result চেক করো ──
    if (pendingResults.has(number)) {
      const result = pendingResults.get(number);
      console.log(`📤 Sending pending session_ready to late subscriber: ${number}`);
      socket.emit("session_ready", result);
    }
  });
});

// ── Root ───────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🐇 RabbitXMD Session Generator → http://localhost:${PORT}\n`);
});
