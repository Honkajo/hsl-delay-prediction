let dataCache = [];
let refreshTimer = null;
const VEHICLE_URL = "/vehicle_positions.json";

async function loadData() {
  try {
    const res = await fetch(VEHICLE_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    dataCache = Array.isArray(data) ? data : [];
    populateDropdown();
    updateTimestamp();
  } catch (err) {
    console.error("Failed to load vehicle data:", err);
    document.getElementById("output").innerHTML =
      `<div class="empty">Could not load data. Try again later.</div>`;
  }
}

function populateDropdown() {
  const select = document.getElementById("lineSelect");
  select.length = 1;

  const lines = [...new Set(dataCache.map(d => d.line_number).filter(Boolean))]
    .sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true }));

  lines.forEach(line => {
    const opt = document.createElement("option");
    opt.value = line;
    opt.textContent = line;
    select.appendChild(opt);
  });

  if (lines.length > 0) {
    if (!select.value || !lines.includes(select.value)) {
      select.value = lines[0];
    }
    renderLine(select.value);
  } else {
    document.getElementById("output").innerHTML = `<div class="empty">No vehicles found.</div>`;
    document.getElementById("count").textContent = "";
  }
}

function renderLine(lineNumber) {
  const rows = dataCache.filter(d => d.line_number === lineNumber);
  if (rows.length === 0) {
    document.getElementById("output").innerHTML =
      `<div class="empty">No active vehicles on line ${lineNumber}.</div>`;
    document.getElementById("count").textContent = "";
    return;
  }

  let html = `
    <table>
      <thead>
        <tr>
          <th>Vehicle ID</th>
          <th>Latitude</th>
          <th>Longitude</th>
          <th>Predicted Delay (min)</th>
          <th>Predicted Delay (sec)</th>
        </tr>
      </thead>
      <tbody>
  `;

  rows.forEach(r => {
    html += `
      <tr>
        <td>${r.vehicle_id ?? ""}</td>
        <td>${r.latitude?.toFixed(5) ?? ""}</td>
        <td>${r.longitude?.toFixed(5) ?? ""}</td>
        <td>${r.pred_delay_seconds ?? ""}</td>
        <td>${r.pred_delay_minutes ?? ""}</td>
      </tr>`;
  });

  html += `</tbody></table>`;
  document.getElementById("output").innerHTML = html;
  document.getElementById("count").textContent = `${rows.length} vehicle(s) on line ${lineNumber}`;
}

function showRaw() {
  const slice = dataCache.slice(0, 200);
  document.getElementById("output").innerHTML =
    `<pre>${JSON.stringify(slice, null, 2)}${dataCache.length > 200 ? `\n... (${dataCache.length - 200} more)` : ""}</pre>`;
  document.getElementById("count").textContent = `${dataCache.length} total row(s)`;
}

function updateTimestamp() {
  const now = new Date();
  document.getElementById("updated").textContent = `Last updated: ${now.toLocaleTimeString()}`;
}

function startAutoRefresh(intervalMs = 30000) {
  clearInterval(refreshTimer);
  refreshTimer = setInterval(async () => {
    console.log("Refreshing vehicle data…");
    const currentLine = document.getElementById("lineSelect").value;
    try {
      const res = await fetch(VEHICLE_URL);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const newData = await res.json();
      dataCache = Array.isArray(newData) ? newData : [];
      if (currentLine) renderLine(currentLine);
      updateTimestamp();
    } catch (err) {
      console.error("Failed to refresh vehicle data:", err);
    }
  }, intervalMs);
}

document.getElementById("rawBtn").addEventListener("click", showRaw);
document.getElementById("lineSelect").addEventListener("change", (e) => {
  const val = e.target.value;
  if (val) renderLine(val);
});

loadData().then(() => startAutoRefresh(15000));

