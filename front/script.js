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

// ================= WH SQL RUNNER =================

async function runWHQuery() {
    const sql = document.getElementById("wh-sql").value.trim();
    if (!sql) return alert("Запит порожній!");

    const res = await fetch(`${WH_URL}/wh/sql`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({query: sql})
    });

    const data = await res.json();

    if (data.error) {
        document.getElementById("wh-result").innerHTML =
            `<p style="color:red">Error: ${data.error}</p>`;
        return;
    }

    renderTable("wh-result", data);
}


// ====== PRESET QUERIES (вставляют SQL в textarea и сразу запускают) ======

function presetAvgOrder() {
    document.getElementById("wh-sql").value =
        `SELECT AVG(total_price) AS avg_order_price FROM fact_sales;`;
    runWHQuery();
}

function presetTotalQty() {
    document.getElementById("wh-sql").value =
        `SELECT SUM(quantity) AS total_quantity_sold FROM fact_sales;`;
    runWHQuery();
}

function presetTop5() {
    document.getElementById("wh-sql").value =
        `SELECT dp.product_name, SUM(fs.quantity) AS total_quantity
         FROM fact_sales fs
         JOIN dim_product dp ON fs.product_new_id = dp.product_new_id
         GROUP BY dp.product_name
         ORDER BY total_quantity DESC
         LIMIT 5;`;
    runWHQuery();
}

function presetCancelled() {
    document.getElementById("wh-sql").value =
        `SELECT COUNT(*) AS cancelled_orders
         FROM fact_sales
         WHERE total_price = 0;`;
    runWHQuery();
}

function presetMaxOrder() {
    document.getElementById("wh-sql").value =
        `SELECT sale_id, total_price
         FROM fact_sales
         ORDER BY total_price DESC
         LIMIT 1;`;
    runWHQuery();
}

function presetExpensive() {
    document.getElementById("wh-sql").value =
        `SELECT COUNT(*) AS expensive_products
         FROM dim_product
         WHERE product_name IS NOT NULL
         AND product_new_id IN (
             SELECT product_new_id FROM fact_sales
             WHERE total_price > 100
         );`;
    runWHQuery();
}


// Init
loadTRTables();
loadWHTables();
