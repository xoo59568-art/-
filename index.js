const express = require("express");
const fs = require("fs");
const path = require("path");
const archiver = require("archiver");
const pino = require("pino");

const {
  default: makeWASocket,
  useMultiFileAuthState
} = require("@whiskeysockets/baileys");

const app = express();
const PORT = process.env.PORT || 3000;

const sessions = new Map();

async function zipToBase64(folder) {
  return new Promise((resolve, reject) => {
    const zipPath = `${folder}.zip`;

    const output = fs.createWriteStream(zipPath);
    const archive = archiver("zip", {
      zlib: { level: 9 }
    });

    output.on("close", () => {
      const base64 = fs
        .readFileSync(zipPath)
        .toString("base64");

      fs.unlinkSync(zipPath);

      resolve(`RABBITXMD~${base64}`);
    });

    archive.on("error", reject);

    archive.pipe(output);
    archive.directory(folder, false);
    archive.finalize();
  });
}

app.get("/api/pair", async (req, res) => {
  try {
    let number = req.query.number;

    if (!number) {
      return res.json({
        success: false,
        message: "Number required"
      });
    }

    number = number.replace(/\D/g, "");

    const folder = path.join(
      __dirname,
      "temp",
      number
    );

    fs.mkdirSync(folder, {
      recursive: true
    });

    const { state, saveCreds } =
      await useMultiFileAuthState(folder);

    const sock = makeWASocket({
      auth: state,
      logger: pino({
        level: "silent"
      })
    });

    sock.ev.on(
      "creds.update",
      saveCreds
    );

    sock.ev.on(
      "connection.update",
      async ({ connection }) => {
        if (connection === "open") {
          const sessionId =
            await zipToBase64(folder);

          sessions.set(
            number,
            sessionId
          );

          console.log(
            `${number} paired`
          );
        }
      }
    );

    const code =
      await sock.requestPairingCode(
        number
      );

    res.json({
      success: true,
      code
    });
  } catch (e) {
    res.json({
      success: false,
      message: e.message
    });
  }
});

app.get("/api/session", (req, res) => {
  const number = (
    req.query.number || ""
  ).replace(/\D/g, "");

  const sessionId =
    sessions.get(number);

  res.json({
    success: true,
    sessionId:
      sessionId || null
  });
});

app.listen(PORT, () => {
  console.log(
    `Server running on ${PORT}`
  );
});
