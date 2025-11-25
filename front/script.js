// ===== helpers =====
function renderTable(containerId, data) {
    const container = document.getElementById(containerId);

    if (!data || data.length === 0) {
        container.innerHTML = "<p>No data</p>";
        return;
    }

    let html = "<table><tr>";
    const keys = Object.keys(data[0]);
    keys.forEach(k => html += `<th>${k}</th>`);
    html += "</tr>";

    data.forEach(row => {
        html += "<tr>";
        keys.forEach(k => html += `<td>${row[k]}</td>`);
        html += "</tr>";
    });
    html += "</table>";

    container.innerHTML = html;
}

function showOutput(data) {
    document.getElementById("output").textContent =
        JSON.stringify(data, null, 2);
}

// ================= TRANSACTIONAL =================
const TR_URL = "http://localhost:5000";

async function loadTRTables() {
    const res = await fetch(`${TR_URL}/tr/tables`);
    const tables = await res.json();

    const sel = document.getElementById("trSelect");
    const selInsert = document.getElementById("trInsertTable");

    tables.forEach(t => {
        sel.innerHTML += `<option value="${t}">${t}</option>`;
        selInsert.innerHTML += `<option value="${t}">${t}</option>`;
    });
}

document.getElementById("trSelect").onchange = async function () {
    const table = this.value;
    if (!table) return;
    // console.log(`${TR_URL}/tr/${table}`);
    const res = await fetch(`${TR_URL}/tr/rows/${table}`);
    const data = await res.json();
    renderTable("trTableContainer", data);
};

document.getElementById("trInsertBtn").onclick = async () => {
    const table = document.getElementById("trInsertTable").value;
    const txt = document.getElementById("trInsertData").value.trim();

    if (!table) return alert("Select table");

    let record;
    try {
        record = JSON.parse(txt);
    } catch {
        return alert("Invalid JSON");
    }

    const res = await fetch(`${TR_URL}/tr/rows/${table}`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(record)
    });

    showOutput(await res.json());
};

document.getElementById("resetTR").onclick = async () => {
    if (!confirm("Reset TR database?")) return;
    const res = await fetch(`${TR_URL}/tr/reset`);
    showOutput(await res.json());
};

// ================= WAREHOUSE =================
const WH_URL = "http://localhost:5001";

async function loadWHTables() {
    const res = await fetch(`${WH_URL}/wh/tables`);
    const tables = await res.json();

    const sel = document.getElementById("whSelect");
    tables.forEach(t => sel.innerHTML += `<option value="${t}">${t}</option>`);
}

document.getElementById("whSelect").onchange = async function () {
    const table = this.value;
    if (!table) return;

    const res = await fetch(`${WH_URL}/wh/rows/${table}`);
    const rows = await res.json();
    renderTable("whTableContainer", rows);
};

document.getElementById("loadWarehouse").onclick = async () => {
    const res = await fetch(`${WH_URL}/etl/load`);
    showOutput(await res.json());

    const table = document.getElementById("whSelect").value;
    if (table) {
        const r = await fetch(`${WH_URL}/wh/rows/${table}`);
        renderTable("whTableContainer", await r.json());
    }
};

document.getElementById("resetWH").onclick = async () => {
    if (!confirm("Reset Warehouse DB?")) return;
    const res = await fetch(`${WH_URL}/wh/reset`);
    showOutput(await res.json());
};

// Init
loadTRTables();
loadWHTables();
