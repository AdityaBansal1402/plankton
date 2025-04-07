import React, { useState, useEffect } from "react";
import { useLocation } from "react-router-dom";
import * as XLSX from "xlsx";
import Papa from "papaparse";

function TableView() {
  const location = useLocation();
  const file = location.state?.file;
  const [columns, setColumns] = useState([]);
  const [rows, setRows] = useState([]);

  useEffect(() => {
    if (!file) return;

    const reader = new FileReader();

    reader.onload = (e) => {
      const fileType = file.name.split(".").pop();
      if (fileType === "csv") {
        const result = Papa.parse(e.target.result, { header: true });
        if (result.data.length > 0) {
          setColumns(Object.keys(result.data[0]));
          setRows(result.data);
        }
      } else if (fileType === "xlsx") {
        const workbook = XLSX.read(e.target.result, { type: "binary" });
        const sheetName = workbook.SheetNames[0];
        const sheet = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);
        if (sheet.length > 0) {
          setColumns(Object.keys(sheet[0]));
          setRows(sheet);
        }
      }
    };

    reader.readAsBinaryString(file);
  }, [file]);

  return (
    <div className="h-screen flex bg-gray-100">
      {/* Left Side: Dropdown */}
      <div className="w-1/4 bg-white shadow-md p-4">
        <h3 className="text-lg font-semibold mb-2">Columns</h3>
        <select className="w-full p-2 border rounded-md">
          {columns.map((col, index) => (
            <option key={index} value={col}>
              {col}
            </option>
          ))}
        </select>
      </div>

      {/* Right Side: Table */}
      <div className="w-3/4 p-4 overflow-auto">
        <div className="overflow-auto max-h-[80vh] bg-white shadow-md rounded-lg">
          <table className="w-full border-collapse">
            <thead className="bg-gray-300 text-gray-700">
              <tr>
                {columns.map((col, index) => (
                  <th key={index} className="p-2 border text-left">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, rowIndex) => (
                <tr key={rowIndex} className="even:bg-gray-100">
                  {columns.map((col, colIndex) => (
                    <td key={colIndex} className="p-2 border">
                      {row[col] || "-"}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export default TableView;
