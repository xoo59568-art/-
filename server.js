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
const io = new Server(server, {
  cors: { origin: "*" },
});

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const activeSessions = new Map();
const TEMP_DIR = path.join(__dirname, "temp");

// Temp directory তৈরি করো
if (!fs.existsSync(TEMP_DIR)) {
  fs.mkdirSync(TEMP_DIR, { recursive: true });
}

// ─── Helper: ZIP করে Base64 SESSION_ID বানাও ──────────────
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

  // ZIP file delete করো
  if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath);

  return `RABBITXMD~${base64Data}`;
}

// ─── Helper: সব temporary ফাইল delete করো ────────────────
function cleanupSession(sessionPath, phoneNumber) {
  try {
    if (fs.existsSync(sessionPath)) {
      fs.rmSync(sessionPath, { recursive: true, force: true });
    }
    if (activeSessions.has(phoneNumber)) {
      activeSessions.delete(phoneNumber);
    }
    console.log(`✅ Cleaned up session for ${phoneNumber}`);
  } catch (err) {
    console.error("Cleanup error:", err.message);
  }
}

// ─── API: Pair Code Generate ──────────────────────────────
app.post("/api/generate-pair", async (req, res) => {
  let { number } = req.body;

  // Validate
  if (!number) {
    return res.status(400).json({ error: "Phone number is required" });
  }

  // শুধু digit রাখো
  number = number.replace(/[^0-9]/g, "");

  if (number.length < 10 || number.length > 15) {
    return res
      .status(400)
      .json({ error: "Invalid phone number (10-15 digits required)" });
  }

  // আগের session থাকলে বন্ধ করো
  if (activeSessions.has(number)) {
    try {
      const old = activeSessions.get(number);
      old.sock?.ev?.removeAllListeners();
      old.sock?.ws?.close();
    } catch (_) {}
    activeSessions.delete(number);
  }

  const sessionPath = path.join(TEMP_DIR, number);

  // পুরনো temp folder delete করো
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

    // Credentials save করো
    sock.ev.on("creds.update", saveCreds);

    // Pair code request করো (3 second delay দিয়ে)
    let pairCodeSent = false;

    const pairCodeTimeout = setTimeout(async () => {
      try {
        if (!pairCodeSent) {
          pairCodeSent = true;
          const code = await sock.requestPairingCode(number);
          const formatted =
            code?.match(/.{1,4}/g)?.join("-").toUpperCase() || code;

          res.json({ success: true, pairCode: formatted, number });
        }
      } catch (err) {
        if (!res.headersSent) {
          res.status(500).json({ error: "Failed to generate pair code: " + err.message });
        }
      }
    }, 3000);

    // Connection update এ listen করো
    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (connection === "open") {
        clearTimeout(pairCodeTimeout);
        console.log(`📱 WhatsApp connected for ${number}`);

        // 2 second পর SESSION_ID generate করো
        await delay(2000);

        try {
          const sessionID = await generateSessionID(sessionPath, number);
          const userNumber =
            sock.user?.id?.split(":")[0] ||
            sock.user?.id?.split("@")[0] ||
            number;

          // Socket দিয়ে frontend-এ পাঠাও
          io.to(number).emit("session_ready", {
            sessionID,
            connectedNumber: userNumber,
          });

          // Session বন্ধ করো
          try {
            sock.ev.removeAllListeners();
            sock.ws.close();
          } catch (_) {}

          // Cleanup করো (5 second পর)
          setTimeout(() => cleanupSession(sessionPath, number), 5000);
        } catch (err) {
          io.to(number).emit("error", {
            message: "SESSION_ID generate করতে সমস্যা হয়েছে: " + err.message,
          });
          cleanupSession(sessionPath, number);
        }
      }

      if (connection === "close") {
        clearTimeout(pairCodeTimeout);
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;

        if (statusCode === DisconnectReason.loggedOut) {
          io.to(number).emit("error", { message: "WhatsApp logged out" });
          cleanupSession(sessionPath, number);
        } else if (statusCode !== DisconnectReason.connectionClosed) {
          io.to(number).emit("error", {
            message: "Connection closed. Please try again.",
          });
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

// ─── Socket.io ───────────────────────────────────────────
io.on("connection", (socket) => {
  socket.on("subscribe", (number) => {
    socket.join(number);
    console.log(`Socket joined room: ${number}`);
  });

  socket.on("disconnect", () => {});
});

// ─── Root Route ──────────────────────────────────────────
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ─── Server Start ─────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🐇 RabbitXMD Session Generator running on port ${PORT}`);
  console.log(`🌐 Open: http://localhost:${PORT}\n`);
});
