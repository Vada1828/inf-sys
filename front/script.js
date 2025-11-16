const tablesSelect = document.getElementById("tables");
const tableDataDiv = document.getElementById("table-data");

const tables = {
  orders: {},
  customers: {},
  "order-details": {},
  managers: {},
  products: {},
};

async function fetchAllTablesData() {
  for (const tableName of Object.keys(tables)) {
    const res = await fetch(`http://localhost:5000/${tableName}`);
    if (res.ok) {
      tables[tableName] = await res.json();
      console.log(`Fetched ${tableName}:`, tables[tableName]);
    } else {
      console.error(`Error fetching ${tableName}:`, res.statusText);
    }
  }
}

fetchAllTablesData().finally(() => {
  Object.keys(tables).forEach((tableName) => {
    const option = document.createElement("option");
    option.value = tableName;
    option.textContent = tableName;
    tablesSelect.appendChild(option);
  });
  tablesSelect.addEventListener("change", (e) => {
    const selectedTable = e.target.value;

    switch (selectedTable) {
      case "orders":
        displayTableData(tables["orders"]);
        break;
      case "customers":
        displayTableData(tables["customers"]);
        break;
      case "order-details":
        displayTableData(tables["order-details"]);
        break;
      case "managers":
        displayTableData(tables["managers"]);
        break;
      case "products":
        displayTableData(tables["products"]);
        break;
      default:
        tableDataDiv.innerHTML = "<p>Select a table to display data.</p>";
    }
  });

  function displayTableData(data) {
    if (!data || data.length === 0) {
      tableDataDiv.innerHTML = "<p>No data available.</p>";
      return;
    }
    const keys = Object.keys(data[0]);
    let html =
      "<table><thead><tr>" +
      keys.map((k) => `<th>${k}</th>`).join("") +
      "</tr></thead><tbody>";
    data.forEach((row) => {
      html += "<tr>" + keys.map((k) => `<td>${row[k]}</td>`).join("") + "</tr>";
    });
    html += "</tbody></table>";
    tableDataDiv.innerHTML = html;
  }
});
