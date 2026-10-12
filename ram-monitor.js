
'use strict';

require('dotenv').config();

const os = require('os');
const fs = require('fs');
const path = require('path');
const https = require('https');

const CONFIG = {
  // Check RAM every minute
  CHECK_INTERVAL_MS: 60 * 1000,

  // Whole-server available RAM thresholds
  WARNING_AVAILABLE_GIB: 1.5,
  CRITICAL_AVAILABLE_GIB: 1.0,

  // Required duration before sending an alert
  WARNING_DURATION_MS: 5 * 60 * 1000,
  CRITICAL_DURATION_MS: 2 * 60 * 1000,

  // Separate server.js process memory alert
  SERVER_RSS_LIMIT_GIB: 600 / 1024,
  SERVER_RSS_DURATION_MS: 5 * 60 * 1000,

  // Repeat the same alert only after 1 hour
  ALERT_COOLDOWN_MS: 60 * 60 * 1000,

  SERVER_ID: process.env.RAM_MONITOR_SERVER_ID || os.hostname(),
  SERVER_SCRIPT_PATH: path.resolve(
    __dirname,
    process.env.RAM_MONITOR_SERVER_PATH || 'server.js'
  ),

  LOG_FILE: path.join(__dirname, 'logs', 'ram-monitor.log'),
};

const GIB = 1024 ** 3;
const MIB = 1024 ** 2;

const state = {
  warningSince: null,
  criticalSince: null,
  serverRssSince: null,
  lastAlert: {},
};

function log(message) {
  const line = `[${new Date().toISOString()}] ${message}`;
  console.log(line);

  try {
    fs.mkdirSync(path.dirname(CONFIG.LOG_FILE), {
      recursive: true,
    });
    fs.appendFileSync(CONFIG.LOG_FILE, line + '\n');
  } catch (error) {
    console.error('Could not write monitor log:', error.message);
  }
}

function readServerRss() {
  try {
    const procEntries = fs.readdirSync('/proc', {
      withFileTypes: true,
    });

    let highestRss = 0;
    let matchedPid = null;

    for (const entry of procEntries) {
      if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) {
        continue;
      }

      const pid = Number(entry.name);

      try {
        const rawCmdline = fs.readFileSync(
          `/proc/${pid}/cmdline`,
          'utf8'
        );

        const cmdline = rawCmdline.split('\0').filter(Boolean);

        const isServerProcess = cmdline.some(arg => {
          const normalizedArg = arg.trim();

          return (
            normalizedArg === CONFIG.SERVER_SCRIPT_PATH ||
            normalizedArg === '/root/webthaali-api/server.js' ||
            normalizedArg.endsWith('/server.js')
          );
        });

        const isMonitorProcess = cmdline.some(arg =>
          arg.includes('ram-monitor.js')
        );

        if (!isServerProcess || isMonitorProcess) {
          continue;
        }

        const status = fs.readFileSync(
          `/proc/${pid}/status`,
          'utf8'
        );

        const match = status.match(/^VmRSS:\s+(\d+)\s+kB/m);

        if (match) {
          const rssBytes = Number(match[1]) * 1024;

          if (rssBytes > highestRss) {
            highestRss = rssBytes;
            matchedPid = pid;
          }
        }
      } catch {
        // The process may exit while being inspected.
      }
    }

    return {
      pid: matchedPid,
      rssBytes: highestRss,
    };
  } catch (error) {
    log(`Process inspection error: ${error.message}`);
    return {
      pid: null,
      rssBytes: 0,
    };
  }
}

function sendEmail(subject, message) {
  const apiKey = process.env.BREVO_API_KEY;
  const from = process.env.RAM_ALERT_FROM;
  const to = process.env.RAM_ALERT_TO;

  if (!apiKey || !from || !to) {
    log(
      `EMAIL NOT CONFIGURED: ${subject}. ` +
      'Check BREVO_API_KEY, RAM_ALERT_FROM and RAM_ALERT_TO in .env.'
    );
    return Promise.resolve(false);
  }

  const recipients = to
    .split(',')
    .map(email => email.trim())
    .filter(Boolean)
    .map(email => ({ email }));

  if (recipients.length === 0) {
    log(`EMAIL NOT SENT: No valid recipient configured for ${subject}`);
    return Promise.resolve(false);
  }

  const body = JSON.stringify({
    sender: {
      name: 'Billey',
      email: from,
    },
    to: recipients,
    subject,
    textContent: message,
  });

  return new Promise(resolve => {
    let settled = false;

    const finish = result => {
      if (settled) return;
      settled = true;
      resolve(result);
    };

    const request = https.request(
      {
        hostname: 'api.brevo.com',
        path: '/v3/smtp/email',
        method: 'POST',
        headers: {
          'api-key': apiKey,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
        timeout: 15000,
      },
      response => {
        let responseBody = '';

        response.on('data', chunk => {
          responseBody += chunk;
        });

        response.on('end', () => {
          if (
            response.statusCode >= 200 &&
            response.statusCode < 300
          ) {
            log(`EMAIL SENT VIA BREVO: ${subject}`);
            finish(true);
          } else {
            log(
              `BREVO EMAIL FAILED (${response.statusCode}): ` +
              responseBody.slice(0, 500)
            );
            finish(false);
          }
        });
      }
    );

    request.on('timeout', () => {
      request.destroy(new Error('Brevo email request timed out'));
    });

    request.on('error', error => {
      log(`BREVO EMAIL ERROR: ${error.message}`);
      finish(false);
    });

    request.write(body);
    request.end();
  });
}

async function alertOnce(key, subject, message) {
  const now = Date.now();
  const lastSent = state.lastAlert[key] || 0;

  if (now - lastSent < CONFIG.ALERT_COOLDOWN_MS) {
    return;
  }

  const sent = await sendEmail(subject, message);

  if (sent) {
    state.lastAlert[key] = Date.now();
  }
}

async function checkMemory() {
  const now = Date.now();
  const totalBytes = os.totalmem();

  let availableBytes = os.freemem();

  try {
    const meminfo = fs.readFileSync('/proc/meminfo', 'utf8');
    const match = meminfo.match(/^MemAvailable:\s+(\d+)\s+kB/m);

    if (match) {
      availableBytes = Number(match[1]) * 1024;
    }
  } catch {
    // Use os.freemem() as fallback.
  }

  const usedBytes = totalBytes - availableBytes;
  const availableGiB = availableBytes / GIB;
  const usedGiB = usedBytes / GIB;

  const server = readServerRss();

  log(
    `RAM: used=${usedGiB.toFixed(2)} GiB, ` +
    `available=${availableGiB.toFixed(2)} GiB, ` +
    `server.js PID=${server.pid ?? 'not found'}, ` +
    `RSS=${(server.rssBytes / MIB).toFixed(0)} MiB`
  );

  const details =
    `Server ID: ${CONFIG.SERVER_ID}\n` +
    `Hostname: ${os.hostname()}\n` +
    `Time: ${new Date().toISOString()}\n` +
    `Total RAM: ${(totalBytes / GIB).toFixed(2)} GiB\n` +
    `Used RAM: ${usedGiB.toFixed(2)} GiB\n` +
    `Available RAM: ${availableGiB.toFixed(2)} GiB\n` +
    `server.js PID: ${server.pid ?? 'not found'}\n` +
    `server.js RSS: ${(server.rssBytes / MIB).toFixed(0)} MiB\n`;

  // Whole-server RAM warning
  if (availableGiB < CONFIG.WARNING_AVAILABLE_GIB) {
    state.warningSince ??= now;

    if (now - state.warningSince >= CONFIG.WARNING_DURATION_MS) {
      await alertOnce(
        'ram-warning',
        'Billey Server RAM Warning',
        details
      );
    }
  } else {
    state.warningSince = null;
  }

  // Whole-server critical RAM alert
  if (availableGiB < CONFIG.CRITICAL_AVAILABLE_GIB) {
    state.criticalSince ??= now;

    if (now - state.criticalSince >= CONFIG.CRITICAL_DURATION_MS) {
      await alertOnce(
        'ram-critical',
        'CRITICAL: Billey Server RAM',
        details
      );
    }
  } else {
    state.criticalSince = null;
  }

  // Separate server.js process RSS alert
  if (
    server.pid &&
    server.rssBytes > CONFIG.SERVER_RSS_LIMIT_GIB * GIB
  ) {
    state.serverRssSince ??= now;

    if (now - state.serverRssSince >= CONFIG.SERVER_RSS_DURATION_MS) {
      await alertOnce(
        'server-rss',
        'Billey server.js Memory Warning',
        details
      );
    }
  } else {
    state.serverRssSince = null;
  }
}

let isChecking = false;

async function runCheck() {
  if (isChecking) return;

  isChecking = true;

  try {
    await checkMemory();
  } catch (error) {
    log(`MONITOR ERROR: ${error.stack || error.message}`);
  } finally {
    isChecking = false;
  }
}

log('RAM monitor started.');
runCheck();

const timer = setInterval(runCheck, CONFIG.CHECK_INTERVAL_MS);

process.on('SIGTERM', () => {
  clearInterval(timer);
  log('RAM monitor stopping.');
  process.exit(0);
});

process.on('SIGINT', () => {
  clearInterval(timer);
  log('RAM monitor stopping.');
  process.exit(0);
});
