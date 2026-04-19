const metricsGrid = document.querySelector("#metricsGrid");
const runsTable = document.querySelector("#runsTable");
const schedulesTable = document.querySelector("#schedulesTable");
const runDetail = document.querySelector("#runDetail");
const refreshButton = document.querySelector("#refreshButton");
const submitRunButton = document.querySelector("#submitRunButton");
const scheduleForm = document.querySelector("#scheduleForm");
const authPanel = document.querySelector("#authPanel");
const authForm = document.querySelector("#authForm");
const apiKeyInput = document.querySelector("#apiKeyInput");

const API_KEY_STORAGE = "orbiter.console.apiKey";
let authEnabled = false;

async function fetchJson(url, options = {}) {
  const apiKey = window.localStorage.getItem(API_KEY_STORAGE);
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  if (apiKey) {
    headers["X-Orbiter-Key"] = apiKey;
  }
  const response = await fetch(url, {
    headers,
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed: ${response.status}`);
  }
  return response.json();
}

async function fetchMetrics() {
  const apiKey = window.localStorage.getItem(API_KEY_STORAGE);
  const response = await fetch("/metrics", {
    headers: apiKey ? { "X-Orbiter-Key": apiKey } : {},
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(text || `Request failed: ${response.status}`);
  }
  return parseMetrics(text);
}

async function loadAuthConfig() {
  const response = await fetch("/auth/config");
  const data = await response.json();
  authEnabled = Boolean(data.enabled);
  authPanel.hidden = !authEnabled;
  return data;
}

function parseMetrics(text) {
  const metrics = {};
  for (const line of text.split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const [key, raw] = line.split(" ");
    metrics[key] = Number(raw);
  }
  return metrics;
}

function metricValue(metrics, prefix, state) {
  return metrics[`${prefix}{state="${state}"}`] ?? 0;
}

function renderMetrics(metrics) {
  const cards = [
    ["Running runs", metricValue(metrics, "orbiter_dag_runs_total", "running")],
    ["Succeeded runs", metricValue(metrics, "orbiter_dag_runs_total", "succeeded")],
    ["Failed runs", metricValue(metrics, "orbiter_dag_runs_total", "failed")],
    ["Running tasks", metricValue(metrics, "orbiter_task_runs_total", "running")],
    ["Retrying tasks", metricValue(metrics, "orbiter_task_runs_total", "retrying")],
    ["Dead letters", metricValue(metrics, "orbiter_task_runs_total", "dead_letter")],
  ];
  metricsGrid.innerHTML = cards
    .map(
      ([label, value]) => `
        <article class="metric-card">
          <span class="label">${label}</span>
          <strong class="value">${value}</strong>
        </article>
      `
    )
    .join("");
}

function formatTime(value) {
  if (!value) return "—";
  return new Date(value * 1000).toLocaleString("en-GB");
}

function badge(value) {
  return `<span class="badge ${value}">${value}</span>`;
}

function renderRuns(runs) {
  runsTable.innerHTML = runs
    .map(
      (run) => `
        <tr>
          <td><button class="secondary" data-run-detail="${run.id}">${run.id.slice(0, 8)}</button></td>
          <td>${badge(run.state)}</td>
          <td>${run.trigger || "manual"}</td>
          <td>${formatTime(run.started_at)}</td>
          <td>${formatTime(run.finished_at)}</td>
          <td>
            <div class="actions">
              <button class="secondary" data-run-detail="${run.id}">Inspect</button>
              ${["pending", "running"].includes(run.state) ? `<button class="danger" data-run-cancel="${run.id}">Cancel</button>` : ""}
            </div>
          </td>
        </tr>
      `
    )
    .join("");
}

function renderSchedules(schedules) {
  schedulesTable.innerHTML = schedules
    .map(
      (schedule) => `
        <tr>
          <td>${schedule.name}</td>
          <td>${badge(schedule.state)}</td>
          <td>${schedule.overlap_policy}</td>
          <td>${schedule.interval_seconds}s</td>
          <td>${formatTime(schedule.next_run_at)}</td>
          <td>${schedule.last_run_id ? schedule.last_run_id.slice(0, 8) : "—"}</td>
          <td>
            <div class="actions">
              <button class="secondary" data-schedule-run="${schedule.id}">Run now</button>
              ${schedule.state === "active"
                ? `<button class="warning" data-schedule-pause="${schedule.id}">Pause</button>`
                : `<button class="primary" data-schedule-resume="${schedule.id}">Resume</button>`}
            </div>
          </td>
        </tr>
      `
    )
    .join("");
}

async function loadRunDetail(runId) {
  const data = await fetchJson(`/runs/${runId}`);
  runDetail.textContent = JSON.stringify(data, null, 2);
}

async function refresh() {
  await loadAuthConfig();
  const [metrics, runsData, schedulesData] = await Promise.all([
    fetchMetrics(),
    fetchJson("/runs?limit=50"),
    fetchJson("/schedules?limit=50"),
  ]);
  renderMetrics(metrics);
  renderRuns(runsData.runs);
  renderSchedules(schedulesData.schedules);
}

document.body.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.dataset.runDetail) {
    await loadRunDetail(target.dataset.runDetail);
    return;
  }

  if (target.dataset.runCancel) {
    await fetchJson(`/runs/${target.dataset.runCancel}/cancel`, { method: "POST" });
    await refresh();
    return;
  }

  if (target.dataset.schedulePause) {
    await fetchJson(`/schedules/${target.dataset.schedulePause}/pause`, { method: "POST" });
    await refresh();
    return;
  }

  if (target.dataset.scheduleResume) {
    await fetchJson(`/schedules/${target.dataset.scheduleResume}/resume`, { method: "POST" });
    await refresh();
    return;
  }

  if (target.dataset.scheduleRun) {
    await fetchJson(`/schedules/${target.dataset.scheduleRun}/run`, { method: "POST" });
    await refresh();
  }
});

refreshButton.addEventListener("click", refresh);

authForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const key = apiKeyInput.value.trim();
  if (!key) {
    return;
  }
  window.localStorage.setItem(API_KEY_STORAGE, key);
  await refresh();
});

submitRunButton.addEventListener("click", async () => {
  await fetchJson("/runs", {
    method: "POST",
    body: JSON.stringify({ params: {} }),
  });
  await refresh();
});

scheduleForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = new FormData(scheduleForm);
  const params = JSON.parse(String(form.get("scheduleParams") || "{}"));
  await fetchJson("/schedules", {
    method: "POST",
    body: JSON.stringify({
      name: String(form.get("scheduleName") || ""),
      interval_seconds: Number(form.get("scheduleInterval")),
      overlap_policy: String(form.get("scheduleOverlap") || "allow"),
      params,
    }),
  });
  scheduleForm.reset();
  document.querySelector("#scheduleOverlap").value = "forbid";
  document.querySelector("#scheduleInterval").value = "3600";
  document.querySelector("#scheduleParams").value = "{}";
  await refresh();
});

refresh().catch((error) => {
  runDetail.textContent = `Console failed to load.\n\n${error.message}`;
});
