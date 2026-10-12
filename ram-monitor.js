
'use strict';
require('dotenv').config();

const os = require('os');
const fs = require('fs');
const path = require('path');
const https = require('https');

const CONFIG = {
  CHECK_INTERVAL_MS: 60 * 1000,

  // Available RAM thresholds (GiB)
  WARNING_AVAILABLE_GIB: 1.5,
  CRITICAL_AVAILABLE_GIB: 1.0,

  // Minimum duration before alert
  WARNING_DURATION_MS: 5 * 60 * 1000,
  CRITICAL_DURATION_MS: 2 * 60 * 1000,

  // Alert if server.js RSS exceeds this limit
  SERVER_RSS_LIMIT_GIB: 1.2,
  SERVER_RSS_DURATION_MS: 5 * 60 * 1000,

  // Same alert cooldown
  ALERT_COOLDOWN_MS: 60 * 60 * 1000,

  // Identify the main API process by its script name
  SERVER_ID: process.env.RAM_MONITOR_SERVER_ID || os.hostname(),
  SERVER_SCRIPT_NAME: 'server.js',
  SERVER_SCRIPT_PATH: process.env.RAM_MONITOR_SERVER_PATH || '',

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
  // Linux process inspection through /proc
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
        const cmdline = fs.readFileSync(
          `/proc/${pid}/cmdline`,
          'utf8'
        ).split('\0').filter(Boolean);

        // Match the actual server.js script argument,
        // not a path mentioned in another command's arguments.
               const isServerProcess = cmdline.some(arg => {
          if (path.basename(arg) !== CONFIG.SERVER_SCRIPT_NAME) {
            return false;
          }

          if (!CONFIG.SERVER_SCRIPT_PATH) {
            return true;
          }

          return path.resolve(arg) ===
            path.resolve(CONFIG.SERVER_SCRIPT_PATH);
        });

        const isMonitorProcess = cmdline.some(
          arg => path.basename(arg) === 'ram-monitor.js'
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
        // Process may exit while being inspected.
      }
    }

    return { pid: matchedPid, rssBytes: highestRss };
  } catch (error) {
    log(`Process inspection error: ${error.message}`);
    return { pid: null, rssBytes: 0 };
  }
}

function sendEmail(subject, message) {
  const apiKey = process.env.RESEND_API_KEY;
  const from = process.env.RAM_ALERT_FROM;
  const to = process.env.RAM_ALERT_TO;

  if (!apiKey || !from || !to) {
    log(
      `EMAIL NOT CONFIGURED: ${subject}. ` +
      'Set RESEND_API_KEY, RAM_ALERT_FROM and RAM_ALERT_TO.'
    );
    return Promise.resolve(false);
  }

  const body = JSON.stringify({
    from,
    to: to.split(',').map(email => email.trim()).filter(Boolean),
    subject,
    text: message,
  });

  return new Promise(resolve => {
    const request = https.request(
      {
        hostname: 'api.resend.com',
        path: '/emails',
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
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
            log(`EMAIL SENT: ${subject}`);
            resolve(true);
          } else {
            log(
              `EMAIL FAILED (${response.statusCode}): ` +
              responseBody.slice(0, 500)
            );
            resolve(false);
          }
        });
      }
    );

    request.on('timeout', () => {
      request.destroy(new Error('Email request timed out'));
    });

    request.on('error', error => {
      log(`EMAIL ERROR: ${error.message}`);
      resolve(false);
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
    `Used RAM (estimated): ${usedGiB.toFixed(2)} GiB\n` +
    `Available RAM: ${availableGiB.toFixed(2)} GiB\n` +
    `server.js PID: ${server.pid ?? 'not found'}\n` +
    `server.js RSS: ${(server.rssBytes / MIB).toFixed(0)} MiB\n`;

  // Warning: available RAM below 1.5 GiB for 5 minutes.
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

  // Critical: available RAM below 1 GiB for 2 minutes.
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

  // Main API process RSS threshold.
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
